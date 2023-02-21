// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.
package cloudstorage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sinkv2/metrics"
	mcloudstorage "github.com/pingcap/tiflow/cdc/sinkv2/metrics/cloudstorage"
	"github.com/pingcap/tiflow/engine/pkg/clock"
	"github.com/pingcap/tiflow/pkg/chann"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// dmlWorker denotes a worker responsible for writing messages to cloud storage.
type dmlWorker struct {
	// worker id
	id           int
	changeFeedID model.ChangeFeedID
	storage      storage.ExternalStorage
	config       *cloudstorage.Config
	// flushNotifyCh is used to notify that several tables can be flushed.
	flushNotifyCh chan flushTask
	// tableEvents maintains a mapping of <table, []eventFragment>.
	tableEvents *tableEventsMap
	// fileIndex maintains a mapping of <table, indexWithDate>.
	fileIndex map[versionedTable]*indexWithDate
	// fileSize maintains a mapping of <table, file size>.
	fileSize         map[versionedTable]uint64
	isClosed         uint64
	extension        string
	statistics       *metrics.Statistics
	clock            clock.Clock
	bufferPool       sync.Pool
	metricWriteBytes prometheus.Gauge
	metricFileCount  prometheus.Gauge
}

type tableEventsMap struct {
	mu        sync.Mutex
	fragments map[versionedTable][]eventFragment
}

func newTableEventsMap() *tableEventsMap {
	return &tableEventsMap{
		fragments: make(map[versionedTable][]eventFragment),
	}
}

type wrappedTable struct {
	tableName model.TableName
	tableInfo *model.TableInfo
}

// flushTask defines a task containing the tables to be flushed.
type flushTask struct {
	targetTables []wrappedTable
}

type indexWithDate struct {
	index              uint64
	currDate, prevDate string
}

func newDMLWorker(
	id int,
	changefeedID model.ChangeFeedID,
	storage storage.ExternalStorage,
	config *cloudstorage.Config,
	extension string,
	statistics *metrics.Statistics,
) *dmlWorker {
	d := &dmlWorker{
		id:            id,
		changeFeedID:  changefeedID,
		storage:       storage,
		config:        config,
		tableEvents:   newTableEventsMap(),
		flushNotifyCh: make(chan flushTask, 1),
		fileIndex:     make(map[versionedTable]*indexWithDate),
		fileSize:      make(map[versionedTable]uint64),
		extension:     extension,
		statistics:    statistics,
		clock:         clock.New(),
		bufferPool: sync.Pool{
			New: func() interface{} {
				return new(bytes.Buffer)
			},
		},
		metricWriteBytes: mcloudstorage.CloudStorageWriteBytesGauge.WithLabelValues(changefeedID.Namespace, changefeedID.ID),
		metricFileCount:  mcloudstorage.CloudStorageFileCountGauge.WithLabelValues(changefeedID.Namespace, changefeedID.ID),
	}

	return d
}

// run creates a set of background goroutines.
func (d *dmlWorker) run(ctx context.Context, ch *chann.DrainableChann[eventFragment]) error {
	log.Debug("dml worker started", zap.Int("workerID", d.id),
		zap.String("namespace", d.changeFeedID.Namespace),
		zap.String("changefeed", d.changeFeedID.ID))

	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return d.flushMessages(ctx)
	})

	eg.Go(func() error {
		return d.dispatchFlushTasks(ctx, ch)
	})

	return eg.Wait()
}

// flushMessages flush messages from active tables to cloud storage.
// active means that a table has events since last flushing.
func (d *dmlWorker) flushMessages(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case task := <-d.flushNotifyCh:
			if atomic.LoadUint64(&d.isClosed) == 1 {
				return nil
			}
			for _, tbl := range task.targetTables {
				table := versionedTable{
					TableName: tbl.tableName,
					version:   tbl.tableInfo.Version,
				}
				d.tableEvents.mu.Lock()
				events := make([]eventFragment, len(d.tableEvents.fragments[table]))
				copy(events, d.tableEvents.fragments[table])
				d.tableEvents.fragments[table] = nil
				d.tableEvents.mu.Unlock()
				if len(events) == 0 {
					continue
				}

				// generate scheme.json file before generating the first data file if necessary
				err := d.writeSchemaFile(ctx, table, tbl.tableInfo)
				if err != nil {
					log.Error("failed to write schema file to external storage",
						zap.Int("workerID", d.id),
						zap.String("namespace", d.changeFeedID.Namespace),
						zap.String("changefeed", d.changeFeedID.ID),
						zap.Error(err))
					return errors.Trace(err)
				}

				path := d.generateDataFilePath(table)
				err = d.writeDataFile(ctx, path, events)
				if err != nil {
					log.Error("failed to write data file to external storage",
						zap.Int("workerID", d.id),
						zap.String("namespace", d.changeFeedID.Namespace),
						zap.String("changefeed", d.changeFeedID.ID),
						zap.Error(err))
					return errors.Trace(err)
				}

				log.Debug("write file to storage success", zap.Int("workerID", d.id),
					zap.String("namespace", d.changeFeedID.Namespace),
					zap.String("changefeed", d.changeFeedID.ID),
					zap.String("schema", table.Schema),
					zap.String("table", table.Table),
					zap.String("path", path),
				)
			}
		}
	}
}

// In order to avoid spending so much time lookuping directory and getting last write point
// (i.e. which dir and which file) when the changefeed is restarted, we'd rather switch to
// a new dir and start writing. In this case, schema file should be created in the new dir
// if it hasn't been created when a DDL event was executed.
func (d *dmlWorker) writeSchemaFile(
	ctx context.Context,
	table versionedTable,
	tableInfo *model.TableInfo,
) error {
	if _, ok := d.fileIndex[table]; !ok {
		var tableDetail cloudstorage.TableDefinition
		tableDetail.FromTableInfo(tableInfo)
		path := generateSchemaFilePath(tableDetail)
		// the file may have been created when a DDL event was executed.
		exist, err := d.storage.FileExists(ctx, path)
		if err != nil {
			return err
		}
		if exist {
			return nil
		}

		encodedDetail, err := json.MarshalIndent(tableDetail, "", "    ")
		if err != nil {
			return err
		}

		err = d.storage.WriteFile(ctx, path, encodedDetail)
		if err != nil {
			return err
		}
	}

	return nil
}

func (d *dmlWorker) writeDataFile(ctx context.Context, path string, events []eventFragment) error {
	var callbacks []func()

	rowsCnt := 0
	buf := d.bufferPool.Get().(*bytes.Buffer)
	defer d.bufferPool.Put(buf)
	buf.Reset()

	for _, frag := range events {
		msgs := frag.encodedMsgs
		d.statistics.ObserveRows(frag.event.Event.Rows...)
		for _, msg := range msgs {
			d.metricWriteBytes.Add(float64(len(msg.Value)))
			rowsCnt += msg.GetRowsCount()
			buf.Write(msg.Value)
			callbacks = append(callbacks, msg.Callback)
		}
	}
	if err := d.statistics.RecordBatchExecution(func() (int, error) {
		err := d.storage.WriteFile(ctx, path, buf.Bytes())
		if err != nil {
			return 0, err
		}
		return rowsCnt, nil
	}); err != nil {
		return err
	}
	d.metricFileCount.Add(1)

	for _, cb := range callbacks {
		if cb != nil {
			cb()
		}
	}

	return nil
}

// dispatchFlushTasks dispatches flush tasks in two conditions:
// 1. the flush interval exceeds the upper limit.
// 2. the file size exceeds the upper limit.
func (d *dmlWorker) dispatchFlushTasks(ctx context.Context,
	ch *chann.DrainableChann[eventFragment],
) error {
	tableSet := make(map[wrappedTable]struct{})
	ticker := time.NewTicker(d.config.FlushInterval)

	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-ticker.C:
			if atomic.LoadUint64(&d.isClosed) == 1 {
				return nil
			}
			var readyTables []wrappedTable
			for tbl := range tableSet {
				readyTables = append(readyTables, tbl)
			}
			if len(readyTables) == 0 {
				continue
			}
			task := flushTask{
				targetTables: readyTables,
			}
			select {
			case <-ctx.Done():
				return errors.Trace(ctx.Err())
			case d.flushNotifyCh <- task:
				log.Debug("flush task is emitted successfully when flush interval exceeds",
					zap.Any("tables", task.targetTables))
				for elem := range tableSet {
					// we should get TableName using elem.tableName instead of
					// elem.tableInfo.TableName because the former one contains
					// the physical table id (useful for partition table)
					// recorded in mounter while the later one does not.
					// TODO: handle TableID of model.TableInfo.TableName properly.
					tbl := versionedTable{
						TableName: elem.tableName,
						version:   elem.tableInfo.Version,
					}
					d.fileSize[tbl] = 0
				}
				tableSet = make(map[wrappedTable]struct{})
			default:
			}
		case frag, ok := <-ch.Out():
			if !ok || atomic.LoadUint64(&d.isClosed) == 1 {
				return nil
			}
			table := frag.versionedTable
			d.tableEvents.mu.Lock()
			d.tableEvents.fragments[table] = append(d.tableEvents.fragments[table], frag)
			d.tableEvents.mu.Unlock()

			key := wrappedTable{
				tableName: frag.TableName,
				tableInfo: frag.event.Event.TableInfo,
			}

			tableSet[key] = struct{}{}
			for _, msg := range frag.encodedMsgs {
				if msg.Value != nil {
					d.fileSize[table] += uint64(len(msg.Value))
				}
			}
			// if the file size exceeds the upper limit, emit the flush task containing the table
			// as soon as possible.
			if d.fileSize[table] > uint64(d.config.FileSize) {
				task := flushTask{
					targetTables: []wrappedTable{key},
				}
				select {
				case <-ctx.Done():
					return errors.Trace(ctx.Err())
				case d.flushNotifyCh <- task:
					log.Debug("flush task is emitted successfully when file size exceeds",
						zap.Any("tables", table))
					d.fileSize[table] = 0
				default:
				}
			}
		}
	}
}

func generateSchemaFilePath(def cloudstorage.TableDefinition) string {
	return fmt.Sprintf("%s/%s/%d/schema.json", def.Schema, def.Table, def.TableVersion)
}

func (d *dmlWorker) generateDataFilePath(tbl versionedTable) string {
	var elems []string
	var dateStr string

	elems = append(elems, tbl.Schema)
	elems = append(elems, tbl.Table)
	elems = append(elems, fmt.Sprintf("%d", tbl.version))

	if d.config.EnablePartitionSeparator && tbl.TableName.IsPartition {
		elems = append(elems, fmt.Sprintf("%d", tbl.TableID))
	}
	currTime := d.clock.Now()
	switch d.config.DateSeparator {
	case config.DateSeparatorYear.String():
		dateStr = currTime.Format("2006")
		elems = append(elems, dateStr)
	case config.DateSeparatorMonth.String():
		dateStr = currTime.Format("2006-01")
		elems = append(elems, dateStr)
	case config.DateSeparatorDay.String():
		dateStr = currTime.Format("2006-01-02")
		elems = append(elems, dateStr)
	default:
	}

	if idx, ok := d.fileIndex[tbl]; !ok {
		d.fileIndex[tbl] = &indexWithDate{
			currDate: dateStr,
		}
	} else {
		idx.currDate = dateStr
	}

	// if date changed, reset the counter
	if d.fileIndex[tbl].prevDate != d.fileIndex[tbl].currDate {
		d.fileIndex[tbl].prevDate = d.fileIndex[tbl].currDate
		d.fileIndex[tbl].index = 0
	}
	d.fileIndex[tbl].index++
	elems = append(elems, fmt.Sprintf("CDC%06d%s", d.fileIndex[tbl].index, d.extension))

	return strings.Join(elems, "/")
}

func (d *dmlWorker) close() {
	if !atomic.CompareAndSwapUint64(&d.isClosed, 0, 1) {
		return
	}
}
