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
<<<<<<< HEAD
	"github.com/pingcap/tiflow/cdc/redo/common"
=======
	"github.com/pingcap/tiflow/cdc/processor/memquota"
>>>>>>> 7848c0caad (redo(ticdc): limit memory usage in applier (#8494))
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

<<<<<<< HEAD
	rd reader.RedoLogReader
=======
	memQuota     *memquota.MemQuota
	pendingQuota uint64

>>>>>>> 7848c0caad (redo(ticdc): limit memory usage in applier (#8494))
	// sinkFactory is used to create table sinks.
	sinkFactory *factory.SinkFactory
	// tableSinks is a map from tableID to table sink.
	// We create it when we need it, and close it after we finish applying the redo logs.
<<<<<<< HEAD
	tableSinks map[model.TableID]tablesink.TableSink
	errCh      chan error
=======
	tableSinks         map[model.TableID]tablesink.TableSink
	tableResolvedTsMap map[model.TableID]*memquota.MemConsumeRecord
	appliedLogCount    uint64

	errCh chan error
>>>>>>> 7848c0caad (redo(ticdc): limit memory usage in applier (#8494))
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

<<<<<<< HEAD
=======
func (ra *RedoApplier) initSink(ctx context.Context) (err error) {
	replicaConfig := config.GetDefaultReplicaConfig()
	ra.sinkFactory, err = dmlfactory.New(ctx, ra.cfg.SinkURI, replicaConfig, ra.errCh)
	if err != nil {
		return err
	}
	ra.ddlSink, err = ddlfactory.New(ctx, ra.cfg.SinkURI, replicaConfig)
	if err != nil {
		return err
	}

	ra.tableSinks = make(map[model.TableID]tablesink.TableSink)
	ra.tableResolvedTsMap = make(map[model.TableID]*memquota.MemConsumeRecord)
	return nil
}

func (ra *RedoApplier) bgReleaseQuota(ctx context.Context) error {
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-ticker.C:
			for tableID, tableSink := range ra.tableSinks {
				checkpointTs := tableSink.GetCheckpointTs()
				ra.memQuota.Release(spanz.TableIDToComparableSpan(tableID), checkpointTs)
			}
		}
	}
}

>>>>>>> 7848c0caad (redo(ticdc): limit memory usage in applier (#8494))
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

	tableResolvedTsMap := make(map[model.TableID]model.ResolvedTs)
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

<<<<<<< HEAD
		for _, redoLog := range redoLogs {
			row := common.LogToRow(redoLog)
			tableID := row.Table.TableID
			if _, ok := ra.tableSinks[tableID]; !ok {
				tableSink := ra.sinkFactory.CreateTableSink(
					model.DefaultChangeFeedID(applierChangefeed),
					tableID,
					prometheus.NewCounter(prometheus.CounterOpts{}),
				)
				ra.tableSinks[tableID] = tableSink
			}
			if _, ok := tableResolvedTsMap[tableID]; !ok {
				tableResolvedTsMap[tableID] = model.NewResolvedTs(checkpointTs)
			}
			ra.tableSinks[tableID].AppendRowChangedEvents(row)
			if redoLog.Row.CommitTs > tableResolvedTsMap[tableID].Ts {
				// Use batch resolvedTs to flush data as quickly as possible.
				tableResolvedTsMap[tableID] = model.ResolvedTs{
					Mode:    model.BatchResolvedMode,
					Ts:      row.CommitTs,
					BatchID: 1,
				}
=======
	log.Info("apply redo log finishes",
		zap.Uint64("appliedLogCount", ra.appliedLogCount),
		zap.Uint64("appliedDDLCount", ra.appliedDDLCount),
		zap.Uint64("currentCheckpoint", resolvedTs))
	return errApplyFinished
}

func (ra *RedoApplier) resetQuota(rowSize uint64) error {
	if rowSize >= config.DefaultChangefeedMemoryQuota || rowSize < ra.pendingQuota {
		log.Panic("row size exceeds memory quota",
			zap.Uint64("rowSize", rowSize),
			zap.Uint64("memoryQuota", config.DefaultChangefeedMemoryQuota))
	}

	// flush all tables before acquire new quota
	for tableID, tableRecord := range ra.tableResolvedTsMap {
		if !tableRecord.ResolvedTs.IsBatchMode() {
			log.Panic("table resolved ts should always be in batch mode when apply redo log")
		}

		if err := ra.tableSinks[tableID].UpdateResolvedTs(tableRecord.ResolvedTs); err != nil {
			return err
		}
		ra.memQuota.Record(spanz.TableIDToComparableSpan(tableID),
			tableRecord.ResolvedTs, tableRecord.Size)

		// reset new record
		ra.tableResolvedTsMap[tableID] = &memquota.MemConsumeRecord{
			ResolvedTs: tableRecord.ResolvedTs.AdvanceBatch(),
			Size:       0,
		}
	}

	oldQuota := ra.pendingQuota
	ra.pendingQuota = rowSize * mysql.DefaultMaxTxnRow
	if ra.pendingQuota > config.DefaultChangefeedMemoryQuota {
		ra.pendingQuota = config.DefaultChangefeedMemoryQuota
	} else if ra.pendingQuota < 64*1024 {
		ra.pendingQuota = 64 * 1024
	}
	return ra.memQuota.BlockAcquire(ra.pendingQuota - oldQuota)
}

func (ra *RedoApplier) applyDDL(
	ctx context.Context, ddl *model.DDLEvent, checkpointTs, resolvedTs uint64,
) error {
	shouldSkip := func() bool {
		if ddl.CommitTs == checkpointTs {
			if _, ok := unsupportedDDL[ddl.Type]; ok {
				log.Error("ignore unsupported DDL", zap.Any("ddl", ddl))
				return true
>>>>>>> 7848c0caad (redo(ticdc): limit memory usage in applier (#8494))
			}
		}

<<<<<<< HEAD
		for tableID, tableResolvedTs := range tableResolvedTsMap {
			if err := ra.tableSinks[tableID].UpdateResolvedTs(tableResolvedTs); err != nil {
				return err
			}
			if tableResolvedTs.IsBatchMode() {
				tableResolvedTsMap[tableID] = tableResolvedTs.AdvanceBatch()
			}
		}
	}
=======
func (ra *RedoApplier) applyRow(
	row *model.RowChangedEvent, checkpointTs model.Ts,
) error {
	rowSize := uint64(row.ApproximateBytes())
	if rowSize > ra.pendingQuota {
		if err := ra.resetQuota(uint64(row.ApproximateBytes())); err != nil {
			return err
		}
	}
	ra.pendingQuota -= rowSize

	tableID := row.Table.TableID
	if _, ok := ra.tableSinks[tableID]; !ok {
		tableSink := ra.sinkFactory.CreateTableSink(
			model.DefaultChangeFeedID(applierChangefeed),
			spanz.TableIDToComparableSpan(tableID),
			prometheus.NewCounter(prometheus.CounterOpts{}),
		)
		ra.tableSinks[tableID] = tableSink
	}
	if _, ok := ra.tableResolvedTsMap[tableID]; !ok {
		ra.tableResolvedTsMap[tableID] = &memquota.MemConsumeRecord{
			ResolvedTs: model.ResolvedTs{
				Mode:    model.BatchResolvedMode,
				Ts:      checkpointTs,
				BatchID: 1,
			},
			Size: 0,
		}
	}

	ra.tableSinks[tableID].AppendRowChangedEvents(row)
	record := ra.tableResolvedTsMap[tableID]
	record.Size += rowSize
	if row.CommitTs > record.ResolvedTs.Ts {
		// Use batch resolvedTs to flush data as quickly as possible.
		ra.tableResolvedTsMap[tableID] = &memquota.MemConsumeRecord{
			ResolvedTs: model.ResolvedTs{
				Mode:    model.BatchResolvedMode,
				Ts:      row.CommitTs,
				BatchID: 1,
			},
			Size: record.Size,
		}
	} else if row.CommitTs < ra.tableResolvedTsMap[tableID].ResolvedTs.Ts {
		log.Panic("commit ts of redo log regressed",
			zap.Int64("tableID", tableID),
			zap.Uint64("commitTs", row.CommitTs),
			zap.Any("resolvedTs", ra.tableResolvedTsMap[tableID]))
	}

	ra.appliedLogCount++
	return nil
}
>>>>>>> 7848c0caad (redo(ticdc): limit memory usage in applier (#8494))

	const warnDuration = 3 * time.Minute
	const flushWaitDuration = 200 * time.Millisecond
	ticker := time.NewTicker(warnDuration)
	defer ticker.Stop()
<<<<<<< HEAD
	for tableID := range tableResolvedTsMap {
		resolvedTs := model.NewResolvedTs(resolvedTs)
		if err := ra.tableSinks[tableID].UpdateResolvedTs(resolvedTs); err != nil {
			return err
=======

	ra.tableResolvedTsMap[tableID] = &memquota.MemConsumeRecord{
		ResolvedTs: model.ResolvedTs{
			Mode:    model.BatchResolvedMode,
			Ts:      rts,
			BatchID: 1,
		},
		Size: ra.tableResolvedTsMap[tableID].Size,
	}
	tableRecord := ra.tableResolvedTsMap[tableID]
	if err := ra.tableSinks[tableID].UpdateResolvedTs(tableRecord.ResolvedTs); err != nil {
		return err
	}
	ra.memQuota.Record(spanz.TableIDToComparableSpan(tableID),
		tableRecord.ResolvedTs, tableRecord.Size)

	// Make sure all events are flushed to downstream.
	for !ra.tableSinks[tableID].GetCheckpointTs().EqualOrGreater(tableRecord.ResolvedTs) {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-ticker.C:
			log.Warn(
				"Table sink is not catching up with resolved ts for a long time",
				zap.Int64("tableID", tableID),
				zap.Any("resolvedTs", tableRecord.ResolvedTs),
				zap.Any("checkpointTs", ra.tableSinks[tableID].GetCheckpointTs()),
			)
		default:
			time.Sleep(flushWaitDuration)
>>>>>>> 7848c0caad (redo(ticdc): limit memory usage in applier (#8494))
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
		ra.tableSinks[tableID].Close()
	}

<<<<<<< HEAD
	log.Info("apply redo log finishes", zap.Int("appliedLogCount", appliedLogCount))
	return errApplyFinished
=======
	// reset new record
	ra.tableResolvedTsMap[tableID] = &memquota.MemConsumeRecord{
		ResolvedTs: tableRecord.ResolvedTs.AdvanceBatch(),
		Size:       0,
	}
	return nil
>>>>>>> 7848c0caad (redo(ticdc): limit memory usage in applier (#8494))
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
<<<<<<< HEAD
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
=======
func (ra *RedoApplier) Apply(egCtx context.Context) (err error) {
	eg, egCtx := errgroup.WithContext(egCtx)
	egCtx = contextutil.PutRoleInCtx(egCtx, util.RoleRedoLogApplier)

	if ra.rd, err = createRedoReader(egCtx, ra.cfg); err != nil {
		return err
	}
	eg.Go(func() error {
		return ra.rd.Run(egCtx)
	})

	ra.memQuota = memquota.NewMemQuota(model.DefaultChangeFeedID(applierChangefeed),
		config.DefaultChangefeedMemoryQuota, "sink")
	defer ra.memQuota.Close()
	eg.Go(func() error {
		return ra.bgReleaseQuota(egCtx)
	})

	eg.Go(func() error {
		return ra.consumeLogs(egCtx)
	})
	eg.Go(func() error {
		return ra.catchError(egCtx)
>>>>>>> 7848c0caad (redo(ticdc): limit memory usage in applier (#8494))
	})

	err = wg.Wait()
	if errors.Cause(err) != errApplyFinished {
		return err
	}
	return nil
}
