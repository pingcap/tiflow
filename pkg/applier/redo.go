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
// See the License for the specific language governing permissions and
// limitations under the License.

package applier

import (
	"context"
	"net/url"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/redo/common"
	"github.com/pingcap/tiflow/cdc/redo/reader"
	"github.com/pingcap/tiflow/cdc/sink"
	"github.com/pingcap/tiflow/cdc/sink/mysql"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/filter"
	"github.com/pingcap/tiflow/pkg/redo"
	"github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	applierChangefeed = "redo-applier"
	emitBatch         = mysql.DefaultMaxTxnRow
	readBatch         = mysql.DefaultWorkerCount * emitBatch
)

var errApplyFinished = errors.New("apply finished, can exit safely")

// RedoApplierConfig is the configuration used by a redo log applier
type RedoApplierConfig struct {
	SinkURI string
	Storage string
	Dir     string
}

// RedoApplier implements a redo log applier
type RedoApplier struct {
	cfg *RedoApplierConfig

	rd    reader.RedoLogReader
	errCh chan error
}

// NewRedoApplier creates a new RedoApplier instance
func NewRedoApplier(cfg *RedoApplierConfig) *RedoApplier {
	return &RedoApplier{
		cfg: cfg,
	}
}

// toLogReaderConfig is an adapter to translate from applier config to redo reader config
// returns storageType, *reader.toLogReaderConfig and error
func (rac *RedoApplierConfig) toLogReaderConfig() (string, *reader.LogReaderConfig, error) {
	uri, err := url.Parse(rac.Storage)
	if err != nil {
		return "", nil, cerror.WrapError(cerror.ErrConsistentStorage, err)
	}
	cfg := &reader.LogReaderConfig{
		Dir:                uri.Path,
		UseExternalStorage: redo.IsExternalStorage(uri.Scheme),
	}
	if cfg.UseExternalStorage {
		cfg.URI = *uri
		// If use external storage as backend, applier will download redo logs to local dir.
		cfg.Dir = rac.Dir
	}
	return uri.Scheme, cfg, nil
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
	checkpointTs, resolvedTs, err := ra.rd.ReadMeta(ctx)
	if err != nil {
		return err
	}
	if checkpointTs == resolvedTs {
		log.Info("apply redo log suncceed: checkpointTs == resolvedTs",
			zap.Uint64("checkpointTs", checkpointTs),
			zap.Uint64("resolvedTs", resolvedTs))
		return errApplyFinished
	}
	err = ra.rd.ResetReader(ctx, checkpointTs, resolvedTs)
	if err != nil {
		return err
	}
	log.Info("apply redo log starts", zap.Uint64("checkpointTs", checkpointTs), zap.Uint64("resolvedTs", resolvedTs))

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
	ctx = contextutil.PutRoleInCtx(ctx, util.RoleRedoLogApplier)
	s, err := sink.New(ctx,
		model.DefaultChangeFeedID(applierChangefeed),
		ra.cfg.SinkURI, ft, replicaConfig, opts, ra.errCh)
	if err != nil {
		return err
	}
	defer func() {
		ra.rd.Close() //nolint:errcheck
		s.Close(ctx)  //nolint:errcheck
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
	tableResolvedTsMap := make(map[model.TableID]model.Ts)
	for {
		redoLogs, err := ra.rd.ReadNextLog(ctx, readBatch)
		if err != nil {
			return err
		}
		if len(redoLogs) == 0 {
			break
		}

		for _, redoLog := range redoLogs {
			tableID := redoLog.Row.Table.TableID
			if _, ok := tableResolvedTsMap[redoLog.Row.Table.TableID]; !ok {
				tableResolvedTsMap[tableID] = lastSafeResolvedTs
			}
			if len(cachedRows) >= emitBatch {
				err := s.EmitRowChangedEvents(ctx, cachedRows...)
				if err != nil {
					return err
				}
				cachedRows = make([]*model.RowChangedEvent, 0, emitBatch)
			}
			cachedRows = append(cachedRows, common.LogToRow(redoLog))

			if redoLog.Row.CommitTs > tableResolvedTsMap[tableID] {
				tableResolvedTsMap[tableID], lastResolvedTs = lastResolvedTs, redoLog.Row.CommitTs
			}
		}

		for tableID, tableLastResolvedTs := range tableResolvedTsMap {
			_, err = s.FlushRowChangedEvents(ctx, tableID, model.NewResolvedTs(tableLastResolvedTs))
			if err != nil {
				return err
			}
		}
	}
	err = s.EmitRowChangedEvents(ctx, cachedRows...)
	if err != nil {
		return err
	}

	for tableID := range tableResolvedTsMap {
		_, err = s.FlushRowChangedEvents(ctx, tableID, model.NewResolvedTs(resolvedTs))
		if err != nil {
			return err
		}
		err = s.RemoveTable(ctx, tableID)
		if err != nil {
			return err
		}
	}
	return errApplyFinished
}

var createRedoReader = createRedoReaderImpl

func createRedoReaderImpl(ctx context.Context, cfg *RedoApplierConfig) (reader.RedoLogReader, error) {
	storageType, readerCfg, err := cfg.toLogReaderConfig()
	if err != nil {
		return nil, err
	}
	return reader.NewRedoLogReader(ctx, storageType, readerCfg)
}

// ReadMeta creates a new redo applier and read meta from reader
func (ra *RedoApplier) ReadMeta(ctx context.Context) (checkpointTs uint64, resolvedTs uint64, err error) {
	rd, err := createRedoReader(ctx, ra.cfg)
	if err != nil {
		return 0, 0, err
	}
	return rd.ReadMeta(ctx)
}

// Apply applies redo log to given target
func (ra *RedoApplier) Apply(ctx context.Context) error {
	rd, err := createRedoReader(ctx, ra.cfg)
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
