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
	"strconv"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/metrics"
	mcloudstorage "github.com/pingcap/tiflow/cdc/sink/metrics/cloudstorage"
	"github.com/pingcap/tiflow/engine/pkg/clock"
	"github.com/pingcap/tiflow/pkg/chann"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
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
	// toBeFlushedCh contains a set of batchedTask waiting to be flushed to cloud storage.
	toBeFlushedCh          chan batchedTask
	inputCh                *chann.DrainableChann[eventFragment]
	isClosed               uint64
	statistics             *metrics.Statistics
	filePathGenerator      *cloudstorage.FilePathGenerator
	metricWriteBytes       prometheus.Gauge
	metricFileCount        prometheus.Gauge
	metricWriteDuration    prometheus.Observer
	metricFlushDuration    prometheus.Observer
	metricsWorkerBusyRatio prometheus.Counter
}

// batchedTask contains a set of singleTableTask.
// We batch message of different tables together to reduce the overhead of calling external storage API.
type batchedTask struct {
	batch map[cloudstorage.VersionedTableName]*singleTableTask
}

// singleTableTask contains a set of messages belonging to the same table.
type singleTableTask struct {
	size      uint64
	tableInfo *model.TableInfo
	msgs      []*common.Message
}

func newBatchedTask() batchedTask {
	return batchedTask{
		batch: make(map[cloudstorage.VersionedTableName]*singleTableTask),
	}
}

func (t *batchedTask) handleSingleTableEvent(event eventFragment) {
	table := event.versionedTable
	if _, ok := t.batch[table]; !ok {
		t.batch[table] = &singleTableTask{
			size:      0,
			tableInfo: event.event.Event.TableInfo,
		}
	}

	v := t.batch[table]
	for _, msg := range event.encodedMsgs {
		v.size += uint64(len(msg.Value))
	}
	v.msgs = append(v.msgs, event.encodedMsgs...)
}

func (t *batchedTask) generateTaskByTable(table cloudstorage.VersionedTableName) batchedTask {
	v := t.batch[table]
	if v == nil {
		log.Panic("table not found in dml task", zap.Any("table", table), zap.Any("task", t))
	}
	delete(t.batch, table)

	return batchedTask{
		batch: map[cloudstorage.VersionedTableName]*singleTableTask{table: v},
	}
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
		toBeFlushedCh:     make(chan batchedTask, 64),
		statistics:        statistics,
		filePathGenerator: cloudstorage.NewFilePathGenerator(config, storage, extension, clock),
		metricWriteBytes: mcloudstorage.CloudStorageWriteBytesGauge.
			WithLabelValues(changefeedID.Namespace, changefeedID.ID),
		metricFileCount: mcloudstorage.CloudStorageFileCountGauge.
			WithLabelValues(changefeedID.Namespace, changefeedID.ID),
		metricWriteDuration: mcloudstorage.CloudStorageWriteDurationHistogram.
			WithLabelValues(changefeedID.Namespace, changefeedID.ID),
		metricFlushDuration: mcloudstorage.CloudStorageFlushDurationHistogram.
			WithLabelValues(changefeedID.Namespace, changefeedID.ID),
		metricsWorkerBusyRatio: mcloudstorage.CloudStorageWorkerBusyRatioCounter.
			WithLabelValues(changefeedID.Namespace, changefeedID.ID, strconv.Itoa(id)),
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
		return d.genAndDispatchTask(ctx, d.inputCh)
	})

	return eg.Wait()
}

// flushMessages flushed messages of active tables to cloud storage.
// active tables are those tables that have received events after the last flush.
func (d *dmlWorker) flushMessages(ctx context.Context) error {
	var flushTimeSlice, totalTimeSlice time.Duration
	overseerTicker := time.NewTicker(d.config.FlushInterval * 2)
	defer overseerTicker.Stop()
	startToWork := time.Now()
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case now := <-overseerTicker.C:
			totalTimeSlice = now.Sub(startToWork)
			busyRatio := flushTimeSlice.Seconds() / totalTimeSlice.Seconds() * 1000
			d.metricsWorkerBusyRatio.Add(busyRatio)
			startToWork = now
			flushTimeSlice = 0
		case batchedTask := <-d.toBeFlushedCh:
			if atomic.LoadUint64(&d.isClosed) == 1 {
				return nil
			}
			start := time.Now()
			for table, task := range batchedTask.batch {
				if len(task.msgs) == 0 {
					continue
				}

				// generate scheme.json file before generating the first data file if necessary
				err := d.filePathGenerator.CheckOrWriteSchema(ctx, table, task.tableInfo)
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
				err = d.writeDataFile(ctx, dataFilePath, task)
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
			flushTimeSlice += time.Since(start)
		}
	}
}

func (d *dmlWorker) writeIndexFile(ctx context.Context, path, content string) error {
	start := time.Now()
	err := d.storage.WriteFile(ctx, path, []byte(content))
	d.metricFlushDuration.Observe(time.Since(start).Seconds())
	return err
}

func (d *dmlWorker) writeDataFile(ctx context.Context, path string, task *singleTableTask) error {
	var callbacks []func()
	buf := bytes.NewBuffer(make([]byte, 0, task.size))
	rowsCnt := 0
	bytesCnt := int64(0)
	for _, msg := range task.msgs {
		bytesCnt += int64(len(msg.Value))
		rowsCnt += msg.GetRowsCount()
		buf.Write(msg.Value)
		callbacks = append(callbacks, msg.Callback)
	}

	if err := d.statistics.RecordBatchExecution(func() (int, int64, error) {
		start := time.Now()
		if d.config.FlushConcurrency <= 1 {
			return rowsCnt, bytesCnt, d.storage.WriteFile(ctx, path, buf.Bytes())
		}

		writer, inErr := d.storage.Create(ctx, path, &storage.WriterOption{
			Concurrency: d.config.FlushConcurrency,
		})
		if inErr != nil {
			return 0, 0, inErr
		}

		defer func() {
			closeErr := writer.Close(ctx)
			if inErr != nil {
				log.Error("failed to close writer", zap.Error(closeErr),
					zap.Int("workerID", d.id),
					zap.Any("table", task.tableInfo.TableName),
					zap.String("namespace", d.changeFeedID.Namespace),
					zap.String("changefeed", d.changeFeedID.ID))
				if inErr == nil {
					inErr = closeErr
				}
			}
		}()
		if _, inErr = writer.Write(ctx, buf.Bytes()); inErr != nil {
			return 0, 0, inErr
		}

		d.metricFlushDuration.Observe(time.Since(start).Seconds())
		return rowsCnt, bytesCnt, nil
	}); err != nil {
		return err
	}

	d.metricWriteBytes.Add(float64(bytesCnt))
	d.metricFileCount.Add(1)
	for _, cb := range callbacks {
		if cb != nil {
			cb()
		}
	}

	return nil
}

// genAndDispatchTask dispatches flush tasks in two conditions:
// 1. the flush interval exceeds the upper limit.
// 2. the file size exceeds the upper limit.
func (d *dmlWorker) genAndDispatchTask(ctx context.Context,
	ch *chann.DrainableChann[eventFragment],
) error {
	batchedTask := newBatchedTask()
	ticker := time.NewTicker(d.config.FlushInterval)

	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-ticker.C:
			if atomic.LoadUint64(&d.isClosed) == 1 {
				return nil
			}
			select {
			case <-ctx.Done():
				return errors.Trace(ctx.Err())
			case d.toBeFlushedCh <- batchedTask:
				log.Debug("flush task is emitted successfully when flush interval exceeds",
					zap.Int("tablesLength", len(batchedTask.batch)))
				batchedTask = newBatchedTask()
			default:
			}
		case frag, ok := <-ch.Out():
			if !ok || atomic.LoadUint64(&d.isClosed) == 1 {
				return nil
			}
			batchedTask.handleSingleTableEvent(frag)
			// if the file size exceeds the upper limit, emit the flush task containing the table
			// as soon as possible.
			table := frag.versionedTable
			if batchedTask.batch[table].size >= uint64(d.config.FileSize) {
				task := batchedTask.generateTaskByTable(table)
				select {
				case <-ctx.Done():
					return errors.Trace(ctx.Err())
				case d.toBeFlushedCh <- task:
					log.Debug("flush task is emitted successfully when file size exceeds",
						zap.Any("table", table),
						zap.Int("eventsLenth", len(task.batch[table].msgs)))
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
