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
	"fmt"
	"net/url"
	"os"
	"time"

	"github.com/pingcap/log"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/model/codec"
	"github.com/pingcap/tiflow/cdc/processor/memquota"
	"github.com/pingcap/tiflow/cdc/redo/reader"
	"github.com/pingcap/tiflow/cdc/sink/ddlsink"
	ddlfactory "github.com/pingcap/tiflow/cdc/sink/ddlsink/factory"
	dmlfactory "github.com/pingcap/tiflow/cdc/sink/dmlsink/factory"
	"github.com/pingcap/tiflow/cdc/sink/tablesink"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/pingcap/tiflow/pkg/redo"
	"github.com/pingcap/tiflow/pkg/sink/mysql"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	applierChangefeed = "redo-applier"
	warnDuration      = 3 * time.Minute
	flushWaitDuration = 200 * time.Millisecond
)

var (
	// In the boundary case, non-idempotent DDLs will not be executed.
	// TODO(CharlesCheung96): fix this
	unsupportedDDL = map[timodel.ActionType]struct{}{
		timodel.ActionExchangeTablePartition: {},
	}
	errApplyFinished = errors.New("apply finished, can exit safely")
)

// RedoApplierConfig is the configuration used by a redo log applier
type RedoApplierConfig struct {
	SinkURI string
	Storage string
	Dir     string
}

// RedoApplier implements a redo log applier
type RedoApplier struct {
	cfg            *RedoApplierConfig
	rd             reader.RedoLogReader
	updateSplitter *updateEventSplitter

	ddlSink         ddlsink.Sink
	appliedDDLCount uint64

	memQuota     *memquota.MemQuota
	pendingQuota uint64

	// sinkFactory is used to create table sinks.
	sinkFactory *dmlfactory.SinkFactory
	// tableSinks is a map from tableID to table sink.
	// We create it when we need it, and close it after we finish applying the redo logs.
	tableSinks         map[model.TableID]tablesink.TableSink
	tableResolvedTsMap map[model.TableID]*memquota.MemConsumeRecord
	appliedLogCount    uint64

	errCh chan error

	// changefeedID is used to identify the changefeed that this applier belongs to.
	// not used for now.
	changefeedID model.ChangeFeedID
}

// NewRedoApplier creates a new RedoApplier instance
func NewRedoApplier(cfg *RedoApplierConfig) *RedoApplier {
	return &RedoApplier{
		cfg:   cfg,
		errCh: make(chan error, 1024),
	}
}

// toLogReaderConfig is an adapter to translate from applier config to redo reader config
// returns storageType, *reader.toLogReaderConfig and error
func (rac *RedoApplierConfig) toLogReaderConfig() (string, *reader.LogReaderConfig, error) {
	uri, err := url.Parse(rac.Storage)
	if err != nil {
		return "", nil, errors.WrapError(errors.ErrConsistentStorage, err)
	}
	if redo.IsLocalStorage(uri.Scheme) {
		uri.Scheme = "file"
	}
	cfg := &reader.LogReaderConfig{
		URI:                *uri,
		Dir:                rac.Dir,
		UseExternalStorage: redo.IsExternalStorage(uri.Scheme),
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

func (ra *RedoApplier) initSink(ctx context.Context) (err error) {
	replicaConfig := config.GetDefaultReplicaConfig()
	ra.sinkFactory, err = dmlfactory.New(ctx, ra.changefeedID, ra.cfg.SinkURI, replicaConfig, ra.errCh, nil)
	if err != nil {
		return err
	}
	ra.ddlSink, err = ddlfactory.New(ctx, ra.changefeedID, ra.cfg.SinkURI, replicaConfig)
	if err != nil {
		return err
	}

	ra.tableSinks = make(map[model.TableID]tablesink.TableSink)
	ra.tableResolvedTsMap = make(map[model.TableID]*memquota.MemConsumeRecord)
	return nil
}

func (ra *RedoApplier) bgReleaseQuota(ctx context.Context) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
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

func (ra *RedoApplier) consumeLogs(ctx context.Context) error {
	checkpointTs, resolvedTs, err := ra.rd.ReadMeta(ctx)
	if err != nil {
		return err
	}
	log.Info("apply redo log starts",
		zap.Uint64("checkpointTs", checkpointTs),
		zap.Uint64("resolvedTs", resolvedTs))
	if err := ra.initSink(ctx); err != nil {
		return err
	}
	defer ra.sinkFactory.Close()

	shouldApplyDDL := func(row *model.RowChangedEvent, ddl *model.DDLEvent) bool {
		if ddl == nil {
			return false
		} else if row == nil {
			// no more rows to apply
			return true
		}
		// If all rows before the DDL (which means row.CommitTs <= ddl.CommitTs)
		// are applied, we should apply this DDL.
		return row.CommitTs > ddl.CommitTs
	}

	row, err := ra.updateSplitter.readNextRow(ctx)
	if err != nil {
		return err
	}
	ddl, err := ra.rd.ReadNextDDL(ctx)
	if err != nil {
		return err
	}
	for {
		if row == nil && ddl == nil {
			break
		}
		if shouldApplyDDL(row, ddl) {
			if err := ra.applyDDL(ctx, ddl, checkpointTs); err != nil {
				return err
			}
			if ddl, err = ra.rd.ReadNextDDL(ctx); err != nil {
				return err
			}
		} else {
			if err := ra.applyRow(row, checkpointTs); err != nil {
				return err
			}
			if row, err = ra.updateSplitter.readNextRow(ctx); err != nil {
				return err
			}
		}
	}
	// wait all tables to flush data
	for tableID := range ra.tableResolvedTsMap {
		if err := ra.waitTableFlush(ctx, tableID, resolvedTs); err != nil {
			return err
		}
		ra.tableSinks[tableID].Close()
	}

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
	ctx context.Context, ddl *model.DDLEvent, checkpointTs uint64,
) error {
	shouldSkip := func() bool {
		if ddl.CommitTs == checkpointTs {
			if _, ok := unsupportedDDL[ddl.Type]; ok {
				log.Error("ignore unsupported DDL", zap.Any("ddl", ddl))
				return true
			}
		}
		if ddl.TableInfo == nil {
			// Note this could omly happen when using old version of cdc, and the commit ts
			// of the DDL should be equal to checkpoint ts or resolved ts.
			log.Warn("ignore DDL without table info", zap.Any("ddl", ddl))
			return true
		}
		return false
	}
	if shouldSkip() {
		return nil
	}
	log.Warn("apply DDL", zap.Any("ddl", ddl))
	// Wait all tables to flush data before applying DDL.
	// TODO: only block tables that are affected by this DDL.
	for tableID := range ra.tableSinks {
		if err := ra.waitTableFlush(ctx, tableID, ddl.CommitTs); err != nil {
			return err
		}
	}
	if err := ra.ddlSink.WriteDDLEvent(ctx, ddl); err != nil {
		return err
	}
	ra.appliedDDLCount++
	return nil
}

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

	tableID := row.GetTableID()
	if _, ok := ra.tableSinks[tableID]; !ok {
		tableSink := ra.sinkFactory.CreateTableSink(
			model.DefaultChangeFeedID(applierChangefeed),
			spanz.TableIDToComparableSpan(tableID),
			checkpointTs,
			pdutil.NewClock4Test(),
			prometheus.NewCounter(prometheus.CounterOpts{}),
			prometheus.NewHistogram(prometheus.HistogramOpts{}),
		)
		ra.tableSinks[tableID] = tableSink
	}
	if _, ok := ra.tableResolvedTsMap[tableID]; !ok {
		// Initialize table record using checkpointTs.
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

func (ra *RedoApplier) waitTableFlush(
	ctx context.Context, tableID model.TableID, rts model.Ts,
) error {
	ticker := time.NewTicker(warnDuration)
	defer ticker.Stop()

	oldTableRecord := ra.tableResolvedTsMap[tableID]
	if oldTableRecord.ResolvedTs.Ts < rts {
		// Use new batch resolvedTs to flush data.
		ra.tableResolvedTsMap[tableID] = &memquota.MemConsumeRecord{
			ResolvedTs: model.ResolvedTs{
				Mode:    model.BatchResolvedMode,
				Ts:      rts,
				BatchID: 1,
			},
			Size: ra.tableResolvedTsMap[tableID].Size,
		}
	} else if oldTableRecord.ResolvedTs.Ts > rts {
		log.Panic("resolved ts of redo log regressed",
			zap.Any("oldResolvedTs", oldTableRecord),
			zap.Any("newResolvedTs", rts))
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
		}
	}

	// reset new record
	ra.tableResolvedTsMap[tableID] = &memquota.MemConsumeRecord{
		ResolvedTs: tableRecord.ResolvedTs.AdvanceBatch(),
		Size:       0,
	}
	return nil
}

var createRedoReader = createRedoReaderImpl

func createRedoReaderImpl(ctx context.Context, cfg *RedoApplierConfig) (reader.RedoLogReader, error) {
	storageType, readerCfg, err := cfg.toLogReaderConfig()
	if err != nil {
		return nil, err
	}
	return reader.NewRedoLogReader(ctx, storageType, readerCfg)
}

// tempTxnInsertEventStorage is used to store insert events in the same transaction
// once you begin to read events from storage, you should read all events before you write new events
type tempTxnInsertEventStorage struct {
	events []*model.RowChangedEvent
	// when events num exceed flushThreshold, write all events to file
	flushThreshold int
	dir            string
	txnCommitTs    model.Ts

	useFileStorage bool
	// eventSizes is used to store the size of each event in file storage
	eventSizes  []int
	writingFile *os.File
	readingFile *os.File
	// reading is used to indicate whether we are reading events from storage
	// this is to ensure that we read all events before write new events
	reading bool
}

const (
	tempStorageFileName   = "_insert_storage.tmp"
	defaultFlushThreshold = 50
)

func newTempTxnInsertEventStorage(flushThreshold int, dir string) *tempTxnInsertEventStorage {
	return &tempTxnInsertEventStorage{
		events:         make([]*model.RowChangedEvent, 0),
		flushThreshold: flushThreshold,
		dir:            dir,
		txnCommitTs:    0,

		useFileStorage: false,
		eventSizes:     make([]int, 0),

		reading: false,
	}
}

func (t *tempTxnInsertEventStorage) initializeAddEvent(ts model.Ts) {
	t.reading = false
	t.useFileStorage = false
	t.txnCommitTs = ts
	t.writingFile = nil
	t.readingFile = nil
}

func (t *tempTxnInsertEventStorage) addEvent(event *model.RowChangedEvent) error {
	// do some pre check
	if !event.IsInsert() {
		log.Panic("event is not insert event", zap.Any("event", event))
	}
	if t.reading && t.hasEvent() {
		log.Panic("should read all events before write new event")
	}
	if !t.hasEvent() {
		t.initializeAddEvent(event.CommitTs)
	} else {
		if t.txnCommitTs != event.CommitTs {
			log.Panic("commit ts of events should be the same",
				zap.Uint64("commitTs", event.CommitTs),
				zap.Uint64("txnCommitTs", t.txnCommitTs))
		}
	}

	if t.useFileStorage {
		return t.writeEventsToFile(event)
	}

	t.events = append(t.events, event)
	if len(t.events) >= t.flushThreshold {
		err := t.writeEventsToFile(t.events...)
		if err != nil {
			return err
		}
		t.events = t.events[:0]
	}
	return nil
}

func (t *tempTxnInsertEventStorage) writeEventsToFile(events ...*model.RowChangedEvent) error {
	if !t.useFileStorage {
		t.useFileStorage = true
		var err error
		t.writingFile, err = os.Create(fmt.Sprintf("%s/%s", t.dir, tempStorageFileName))
		if err != nil {
			return err
		}
	}
	for _, event := range events {
		redoLog := event.ToRedoLog()
		data, err := codec.MarshalRedoLog(redoLog, nil)
		if err != nil {
			return errors.WrapError(errors.ErrMarshalFailed, err)
		}
		t.eventSizes = append(t.eventSizes, len(data))
		_, err = t.writingFile.Write(data)
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *tempTxnInsertEventStorage) hasEvent() bool {
	return len(t.events) > 0 || len(t.eventSizes) > 0
}

func (t *tempTxnInsertEventStorage) readFromFile() (*model.RowChangedEvent, error) {
	if len(t.eventSizes) == 0 {
		return nil, nil
	}
	if t.readingFile == nil {
		var err error
		t.readingFile, err = os.Open(fmt.Sprintf("%s/%s", t.dir, tempStorageFileName))
		if err != nil {
			return nil, err
		}
	}
	size := t.eventSizes[0]
	data := make([]byte, size)
	n, err := t.readingFile.Read(data)
	if err != nil {
		return nil, err
	}
	if n != size {
		return nil, errors.New("read size not equal to expected size")
	}
	t.eventSizes = t.eventSizes[1:]
	redoLog, _, err := codec.UnmarshalRedoLog(data)
	if err != nil {
		return nil, errors.WrapError(errors.ErrUnmarshalFailed, err)
	}
	return redoLog.RedoRow.Row.ToRowChangedEvent(), nil
}

func (t *tempTxnInsertEventStorage) readNextEvent() (*model.RowChangedEvent, error) {
	if !t.hasEvent() {
		return nil, nil
	}
	t.reading = true
	if t.useFileStorage {
		return t.readFromFile()
	}

	event := t.events[0]
	t.events = t.events[1:]
	return event, nil
}

// updateEventSplitter splits an update event to a delete event and a deferred insert event
// when the update event is an update to the handle key or the non empty unique key.
// deferred insert event means all delete events and update events in the same transaction are emitted before this insert event
type updateEventSplitter struct {
	rd             reader.RedoLogReader
	rdFinished     bool
	tempStorage    *tempTxnInsertEventStorage
	prevTxnStartTs model.Ts
	// pendingEvent is the event that trigger the process to emit events from tempStorage, it can be
	// 1) an insert event in the same transaction(because there will be no more update and delete events in the same transaction)
	// 2) a new event in the next transaction
	pendingEvent *model.RowChangedEvent
	// meetInsertInCurTxn is used to indicate whether we meet an insert event in the current transaction
	// this is to add some check to ensure that insert events are emitted after other kinds of events in the same transaction
	meetInsertInCurTxn bool
}

func newUpdateEventSplitter(rd reader.RedoLogReader, dir string) *updateEventSplitter {
	return &updateEventSplitter{
		rd:             rd,
		rdFinished:     false,
		tempStorage:    newTempTxnInsertEventStorage(defaultFlushThreshold, dir),
		prevTxnStartTs: 0,
	}
}

// processEvent return (event to emit, pending event)
func processEvent(
	event *model.RowChangedEvent,
	prevTxnStartTs model.Ts,
	tempStorage *tempTxnInsertEventStorage,
) (*model.RowChangedEvent, *model.RowChangedEvent, error) {
	if event == nil {
		log.Panic("event should not be nil")
	}

	// meet a new transaction
	if prevTxnStartTs != 0 && prevTxnStartTs != event.StartTs {
		if tempStorage.hasEvent() {
			// emit the insert events in the previous transaction
			return nil, event, nil
		}
	}
	// nolint
	if event.IsDelete() {
		return event, nil, nil
	} else if event.IsInsert() {
		if tempStorage.hasEvent() {
			// pend current event and emit the insert events in temp storage first to release memory
			return nil, event, nil
		}
		return event, nil, nil
	} else if !model.ShouldSplitUpdateEvent(event) {
		return event, nil, nil
	} else {
		deleteEvent, insertEvent, err := model.SplitUpdateEvent(event)
		if err != nil {
			return nil, nil, err
		}
		err = tempStorage.addEvent(insertEvent)
		if err != nil {
			return nil, nil, err
		}
		return deleteEvent, nil, nil
	}
}

func (u *updateEventSplitter) checkEventOrder(event *model.RowChangedEvent) {
	if event == nil {
		return
	}
	// meeet a new transaction
	if event.StartTs != u.prevTxnStartTs {
		u.meetInsertInCurTxn = false
		return
	}
	if event.IsInsert() {
		u.meetInsertInCurTxn = true
	} else {
		// delete or update events
		if u.meetInsertInCurTxn {
			log.Panic("insert events should be emitted after other kinds of events in the same transaction")
		}
	}
}

func (u *updateEventSplitter) readNextRow(ctx context.Context) (*model.RowChangedEvent, error) {
	for {
		// case 1: pendingEvent is not nil, emit all events from tempStorage and then process pendingEvent
		if u.pendingEvent != nil {
			if u.tempStorage.hasEvent() {
				return u.tempStorage.readNextEvent()
			}
			var event *model.RowChangedEvent
			var err error
			event, u.pendingEvent, err = processEvent(u.pendingEvent, u.prevTxnStartTs, u.tempStorage)
			if err != nil {
				return nil, err
			}
			if event == nil || u.pendingEvent != nil {
				log.Panic("processEvent return wrong result for pending event",
					zap.Any("event", event),
					zap.Any("pendingEvent", u.pendingEvent))
			}
			return event, nil
		}
		// case 2: no more events from RedoLogReader, emit all events from tempStorage and return nil
		if u.rdFinished {
			if u.tempStorage.hasEvent() {
				return u.tempStorage.readNextEvent()
			}
			return nil, nil
		}
		// case 3: read and process events from RedoLogReader
		event, err := u.rd.ReadNextRow(ctx)
		if err != nil {
			return nil, err
		}
		if event == nil {
			u.rdFinished = true
		} else {
			u.checkEventOrder(event)
			prevTxnStartTs := u.prevTxnStartTs
			u.prevTxnStartTs = event.StartTs
			var err error
			event, u.pendingEvent, err = processEvent(event, prevTxnStartTs, u.tempStorage)
			if err != nil {
				return nil, err
			}
			if event != nil {
				return event, nil
			}
			if u.pendingEvent == nil {
				log.Panic("event to emit and pending event cannot all be nil")
			}
		}
	}
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
func (ra *RedoApplier) Apply(egCtx context.Context) (err error) {
	eg, egCtx := errgroup.WithContext(egCtx)

	if ra.rd, err = createRedoReader(egCtx, ra.cfg); err != nil {
		return err
	}
	eg.Go(func() error {
		return ra.rd.Run(egCtx)
	})
	ra.updateSplitter = newUpdateEventSplitter(ra.rd, ra.cfg.Dir)

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
	})

	err = eg.Wait()
	if errors.Cause(err) != errApplyFinished {
		return err
	}
	return nil
}
