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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/redo/common"
	"github.com/pingcap/tiflow/cdc/redo/reader"
	"github.com/pingcap/tiflow/cdc/sink/mysql"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink/factory"
	"github.com/pingcap/tiflow/cdc/sinkv2/tablesink"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/redo"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
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

	rd reader.RedoLogReader
	// sinkFactory is used to create table sinks.
	sinkFactory *factory.SinkFactory
	// tableSinks is a map from tableID to table sink.
	// We create it when we need it, and close it after we finish applying the redo logs.
	tableSinks map[model.TableID]tablesink.TableSink
	errCh      chan error
}

// NewRedoApplier creates a new RedoApplier instance
func NewRedoApplier(cfg *RedoApplierConfig) *RedoApplier {
	return &RedoApplier{
		cfg:        cfg,
		tableSinks: make(map[model.TableID]tablesink.TableSink),
		errCh:      make(chan error, 1024),
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

	cachedRows := make([]*model.RowChangedEvent, 0, emitBatch)
	tableResolvedTsMap := make(map[model.TableID]model.Ts)
	appliedLogCount := 0
	for {
		redoLogs, err := ra.rd.ReadNextLog(ctx, readBatch)
		if err != nil {
			return err
		}
		if len(redoLogs) == 0 {
			break
		}
		appliedLogCount += len(redoLogs)

		for _, redoLog := range redoLogs {
			tableID := redoLog.Row.Table.TableID
			if _, ok := ra.tableSinks[tableID]; !ok {
				tableSink := ra.sinkFactory.CreateTableSink(
					model.DefaultChangeFeedID(applierChangefeed),
					tableID,
					prometheus.NewCounter(prometheus.CounterOpts{}),
				)
				ra.tableSinks[tableID] = tableSink
			}
			if _, ok := tableResolvedTsMap[redoLog.Row.Table.TableID]; !ok {
				// Safe to use checkpointTs - 1 as the resolvedTs of a table.
				tableResolvedTsMap[tableID] = checkpointTs - 1
			}
			if len(cachedRows) >= emitBatch {
				for _, row := range cachedRows {
					tableID := row.Table.TableID
					ra.tableSinks[tableID].AppendRowChangedEvents(row)
				}
				cachedRows = make([]*model.RowChangedEvent, 0, emitBatch)
			}
			cachedRows = append(cachedRows, common.LogToRow(redoLog))
			if redoLog.Row.CommitTs > tableResolvedTsMap[tableID] {
				tableResolvedTsMap[tableID] = redoLog.Row.CommitTs
			}
		}

		for tableID, tableLastResolvedTs := range tableResolvedTsMap {
			if err := ra.tableSinks[tableID].UpdateResolvedTs(
				model.NewResolvedTs(tableLastResolvedTs)); err != nil {
				return err
			}
		}
	}
	for _, row := range cachedRows {
		tableID := row.Table.TableID
		ra.tableSinks[tableID].AppendRowChangedEvents(row)
	}
	const warnDuration = 3 * time.Minute
	const flushWaitDuration = 200 * time.Millisecond
	ticker := time.NewTicker(warnDuration)
	defer ticker.Stop()
	for tableID := range tableResolvedTsMap {
		resolvedTs := model.NewResolvedTs(resolvedTs)
		if err := ra.tableSinks[tableID].UpdateResolvedTs(
			resolvedTs); err != nil {
			return err
		}
		// Make sure all events are flushed to downstream.
		for !ra.tableSinks[tableID].GetCheckpointTs().EqualOrGreater(resolvedTs) {
			select {
			case <-ctx.Done():
				return errors.Trace(ctx.Err())
			case <-ticker.C:
				log.Warn(
					"Table sink is not catching up with resolved ts for a long time",
					zap.Int64("tableID", tableID),
					zap.Any("resolvedTs", resolvedTs),
					zap.Any("checkpointTs", ra.tableSinks[tableID].GetCheckpointTs()),
				)

			default:
				time.Sleep(flushWaitDuration)
			}
		}
		ra.tableSinks[tableID].Close(ctx)
	}

	log.Info("apply redo log finishes", zap.Int("appliedLogCount", appliedLogCount))
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
	defer func() {
		if err = ra.rd.Close(); err != nil {
			log.Warn("Close redo reader failed", zap.Error(err))
		}
	}()
	// MySQL sink will use the following replication config
	// - EnableOldValue: default true
	// - ForceReplicate: default false
	// - filter: default []string{"*.*"}
	replicaConfig := config.GetDefaultReplicaConfig()
	ctx = contextutil.PutRoleInCtx(ctx, util.RoleRedoLogApplier)
	sinkFactory, err := factory.New(ctx,
		ra.cfg.SinkURI, replicaConfig, ra.errCh)
	if err != nil {
		return err
	}
	ra.sinkFactory = sinkFactory
	defer sinkFactory.Close()

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
