// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing pemissions and
// limitations under the License.

package applier

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/redo"
	"github.com/pingcap/ticdc/cdc/redo/reader"
	"github.com/pingcap/ticdc/cdc/sink"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/filter"
	"golang.org/x/sync/errgroup"
)

const (
	applierChangefeed = "redo-applier"
	// use the same value as defaultMaxTxnRow in MySQL sink
	emitBatch = 256
	// use defaultWorkerCount(16) * defaultMaxTxnRow(256) in MySQL sink
	readBatch = 16 * 256
)

var errApplyFinished = errors.New("apply finished, can exit safely")

// RedoApplierConfig is the configuration used by a redo log applier
type RedoApplierConfig struct {
	SinkURI string
	Storage string
}

// RedoApplier implements a redo log applier
type RedoApplier struct {
	cfg *RedoApplierConfig

	rd    reader.Reader
	errCh chan error
}

// NewRedoApplier creates a new RedoApplier instance
func NewRedoApplier(cfg *RedoApplierConfig) *RedoApplier {
	return &RedoApplier{
		cfg: cfg,
	}
}

func (ra *RedoApplier) catchError(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-ra.errCh:
			return err
		}
	}
}

func (ra *RedoApplier) consumeLogs(ctx context.Context) error {
	resolvedTs, checkpointTs, err := ra.rd.ReadMeta(ctx)
	if err != nil {
		return err
	}
	err = ra.rd.ResetReader(ctx, checkpointTs, resolvedTs)
	if err != nil {
		return err
	}

	// MySQL sink will use the following replication config
	// - EnableOldValue: default true
	// - ForceReplicate: default false
	// - filter: default []string{"*.*"}
	replicaConfig := config.GetDefaultReplicaConfig()
	ft, err := filter.NewFilter(replicaConfig)
	if err != nil {
		return err
	}
	opts := map[string]string{}
	s, err := sink.NewSink(ctx, applierChangefeed, ra.cfg.SinkURI, ft, replicaConfig, opts, ra.errCh)
	if err != nil {
		return err
	}
	defer func() {
		s.Close(ctx) //nolint:errcheck
	}()

	// TODO: split events for large transaction
	// We use lastSafeResolvedTs and lastResolvedTs to ensure the events in one
	// transaction are flushed in a single batch.
	// lastSafeResolvedTs records the max resolved ts of a closed transaction.
	// Closed transaction means all events of this transaction have been received.
	lastSafeResolvedTs := checkpointTs - 1
	// lastResolvedTs records the max resolved ts we have seen from redo logs.
	lastResolvedTs := checkpointTs
	cachedRows := make([]*model.RowChangedEvent, 0, emitBatch)
	for {
		redoLogs, err := ra.rd.ReadNextLog(ctx, readBatch)
		if err != nil {
			return err
		}
		if len(redoLogs) == 0 {
			break
		}

		for _, redoLog := range redoLogs {
			if len(cachedRows) >= emitBatch {
				err := s.EmitRowChangedEvents(ctx, cachedRows...)
				if err != nil {
					return err
				}
				cachedRows = make([]*model.RowChangedEvent, 0, emitBatch)
			}
			cachedRows = append(cachedRows, redoLog.Row)
			if redoLog.Row.CommitTs > lastResolvedTs {
				lastSafeResolvedTs, lastResolvedTs = lastResolvedTs, redoLog.Row.CommitTs
			}
		}
		err = s.EmitRowChangedEvents(ctx, cachedRows...)
		if err != nil {
			return err
		}
		_, err = s.FlushRowChangedEvents(ctx, lastSafeResolvedTs)
		if err != nil {
			return err
		}
	}
	_, err = s.FlushRowChangedEvents(ctx, resolvedTs)
	if err != nil {
		return err
	}
	err = s.Barrier(ctx)
	if err != nil {
		return err
	}
	return errApplyFinished
}

var createRedoReader = createRedoReaderImpl

func createRedoReaderImpl(cfg *RedoApplierConfig) (reader.Reader, error) {
	readerConfig := &redo.ReaderConfig{
		Storage: cfg.Storage,
	}
	return redo.NewRedoReader(readerConfig)
}

// Apply applies redo log to given target
func (ra *RedoApplier) Apply(ctx context.Context) error {
	rd, err := createRedoReader(ra.cfg)
	if err != nil {
		return err
	}
	ra.rd = rd
	ra.errCh = make(chan error, 1024)

	wg, ctx := errgroup.WithContext(ctx)
	wg.Go(func() error {
		return ra.consumeLogs(ctx)
	})
	wg.Go(func() error {
		return ra.catchError(ctx)
	})

	err = wg.Wait()
	if errors.Cause(err) != errApplyFinished {
		return err
	}
	return nil
}
