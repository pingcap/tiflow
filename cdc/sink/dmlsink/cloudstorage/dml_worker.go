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
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/metrics"
	mcloudstorage "github.com/pingcap/tiflow/cdc/sink/metrics/cloudstorage"
	"github.com/pingcap/tiflow/engine/pkg/clock"
	"github.com/pingcap/tiflow/pkg/chann"
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
	inputCh       *chann.DrainableChann[eventFragment]
	// tableEvents maintains a mapping of <table, []eventFragment>.
	tableEvents *tableEventsMap
	// fileSize maintains a mapping of <table, file size>.
	fileSize          map[cloudstorage.VersionedTableName]uint64
	isClosed          uint64
	statistics        *metrics.Statistics
	filePathGenerator *cloudstorage.FilePathGenerator
	bufferPool        sync.Pool
	metricWriteBytes  prometheus.Gauge
	metricFileCount   prometheus.Gauge
}

type tableEventsMap struct {
	mu        sync.Mutex
	fragments map[cloudstorage.VersionedTableName][]eventFragment
}

func newTableEventsMap() *tableEventsMap {
	return &tableEventsMap{
		fragments: make(map[cloudstorage.VersionedTableName][]eventFragment),
	}
}

type wrappedTable struct {
	cloudstorage.VersionedTableName
	tableInfo *model.TableInfo
}

// flushTask defines a task containing the tables to be flushed.
type flushTask struct {
	targetTables []wrappedTable
}

func newDMLWorker(
	id int,
	changefeedID model.ChangeFeedID,
	storage storage.ExternalStorage,
	config *cloudstorage.Config,
	extension string,
	inputCh *chann.DrainableChann[eventFragment],
	clock clock.Clock,
	statistics *metrics.Statistics,
) *dmlWorker {
	d := &dmlWorker{
		id:                id,
		changeFeedID:      changefeedID,
		storage:           storage,
		config:            config,
		inputCh:           inputCh,
		tableEvents:       newTableEventsMap(),
		flushNotifyCh:     make(chan flushTask, 1),
		fileSize:          make(map[cloudstorage.VersionedTableName]uint64),
		statistics:        statistics,
		filePathGenerator: cloudstorage.NewFilePathGenerator(config, storage, extension, clock),
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
func (d *dmlWorker) run(ctx context.Context) error {
	log.Debug("dml worker started", zap.Int("workerID", d.id),
		zap.String("namespace", d.changeFeedID.Namespace),
		zap.String("changefeed", d.changeFeedID.ID))

	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return d.flushMessages(ctx)
	})

	eg.Go(func() error {
		return d.dispatchFlushTasks(ctx, d.inputCh)
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
				table := tbl.VersionedTableName
				d.tableEvents.mu.Lock()
				events := make([]eventFragment, len(d.tableEvents.fragments[table]))
				copy(events, d.tableEvents.fragments[table])
				d.tableEvents.fragments[table] = nil
				d.tableEvents.mu.Unlock()
				if len(events) == 0 {
					continue
				}

				// generate scheme.json file before generating the first data file if necessary
				err := d.filePathGenerator.CheckOrWriteSchema(ctx, table, tbl.tableInfo)
				if err != nil {
					log.Error("failed to write schema file to external storage",
						zap.Int("workerID", d.id),
						zap.String("namespace", d.changeFeedID.Namespace),
						zap.String("changefeed", d.changeFeedID.ID),
						zap.Error(err))
					return errors.Trace(err)
				}

				// make sure that `generateDateStr()` is invoked ONLY once before
				// generating data file path and index file path. Because we don't expect the index
				// file is written to a different dir if date change happens between
				// generating data and index file.
				date := d.filePathGenerator.GenerateDateStr()
				dataFilePath, err := d.filePathGenerator.GenerateDataFilePath(ctx, table, date)
				if err != nil {
					log.Error("failed to generate data file path",
						zap.Int("workerID", d.id),
						zap.String("namespace", d.changeFeedID.Namespace),
						zap.String("changefeed", d.changeFeedID.ID),
						zap.Error(err))
					return errors.Trace(err)
				}
				indexFilePath := d.filePathGenerator.GenerateIndexFilePath(table, date)

				// first write the index file to external storage.
				// the file content is simply the last element of the data file path
				err = d.writeIndexFile(ctx, indexFilePath, path.Base(dataFilePath)+"\n")
				if err != nil {
					log.Error("failed to write index file to external storage",
						zap.Int("workerID", d.id),
						zap.String("namespace", d.changeFeedID.Namespace),
						zap.String("changefeed", d.changeFeedID.ID),
						zap.String("path", indexFilePath),
						zap.Error(err))
				}

				// then write the data file to external storage.
				// TODO: if system crashes when writing date file CDC000002.csv
				// (file is not generated at all), then after TiCDC recovers from the crash,
				// storage sink will generate a new file named CDC000003.csv,
				// we will optimize this issue later.
				err = d.writeDataFile(ctx, dataFilePath, events)
				if err != nil {
					log.Error("failed to write data file to external storage",
						zap.Int("workerID", d.id),
						zap.String("namespace", d.changeFeedID.Namespace),
						zap.String("changefeed", d.changeFeedID.ID),
						zap.String("path", dataFilePath),
						zap.Error(err))
					return errors.Trace(err)
				}

				log.Debug("write file to storage success", zap.Int("workerID", d.id),
					zap.String("namespace", d.changeFeedID.Namespace),
					zap.String("changefeed", d.changeFeedID.ID),
					zap.String("schema", table.TableNameWithPhysicTableID.Schema),
					zap.String("table", table.TableNameWithPhysicTableID.Table),
					zap.String("path", dataFilePath),
				)
			}
		}
	}
}

func (d *dmlWorker) writeIndexFile(ctx context.Context, path, content string) error {
	err := d.storage.WriteFile(ctx, path, []byte(content))
	return err
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
					tbl := elem.VersionedTableName
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
				VersionedTableName: table,
				tableInfo:          frag.event.Event.TableInfo,
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

func (d *dmlWorker) close() {
	if !atomic.CompareAndSwapUint64(&d.isClosed, 0, 1) {
		return
	}
}
