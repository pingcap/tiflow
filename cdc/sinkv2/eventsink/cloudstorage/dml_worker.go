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

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sinkv2/metrics"
	mcloudstorage "github.com/pingcap/tiflow/cdc/sinkv2/metrics/cloudstorage"
	"github.com/pingcap/tiflow/pkg/chann"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
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
	// fileIndex maintains a mapping of <table, index number>.
	fileIndex map[versionedTable]uint64
	// fileSize maintains a mapping of <table, file size>.
	fileSize         map[versionedTable]uint64
	wg               sync.WaitGroup
	isClosed         uint64
	errCh            chan<- error
	extension        string
	statistics       *metrics.Statistics
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

// flushTask defines a task containing the tables to be flushed.
type flushTask struct {
	targetTables []*model.TableInfo
}

func newDMLWorker(
	id int,
	changefeedID model.ChangeFeedID,
	storage storage.ExternalStorage,
	config *cloudstorage.Config,
	extension string,
	statistics *metrics.Statistics,
	errCh chan<- error,
) *dmlWorker {
	d := &dmlWorker{
		id:               id,
		changeFeedID:     changefeedID,
		storage:          storage,
		config:           config,
		tableEvents:      newTableEventsMap(),
		flushNotifyCh:    make(chan flushTask, 1),
		fileIndex:        make(map[versionedTable]uint64),
		fileSize:         make(map[versionedTable]uint64),
		extension:        extension,
		errCh:            errCh,
		statistics:       statistics,
		metricWriteBytes: mcloudstorage.CloudStorageWriteBytesGauge.WithLabelValues(changefeedID.Namespace, changefeedID.ID),
		metricFileCount:  mcloudstorage.CloudStorageFileCountGauge.WithLabelValues(changefeedID.Namespace, changefeedID.ID),
	}

	return d
}

// run creates a set of background goroutines.
func (d *dmlWorker) run(ctx context.Context, ch *chann.Chann[eventFragment]) {
	d.backgroundFlushMsgs(ctx)
	d.backgroundDispatchTasks(ctx, ch)
}

// backgroundFlushMsgs flush messages from active tables to cloud storage.
// active means that a table has events since last flushing.
func (d *dmlWorker) backgroundFlushMsgs(ctx context.Context) {
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()

		var buf bytes.Buffer
		for {
			select {
			case <-ctx.Done():
				return
			case task := <-d.flushNotifyCh:
				if atomic.LoadUint64(&d.isClosed) == 1 {
					return
				}
				for _, tableInfo := range task.targetTables {
					buf.Reset()
					var callbacks []func()
					table := versionedTable{
						TableName: tableInfo.TableName,
						version:   tableInfo.Version,
					}
					d.tableEvents.mu.Lock()
					events := d.tableEvents.fragments[table]
					d.tableEvents.fragments[table] = d.tableEvents.fragments[table][:0]
					d.tableEvents.mu.Unlock()
					if len(events) == 0 {
						continue
					}

					rowsCnt := 0
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

					// mandatorily generate scheme.json file before generating the first data file
					if d.fileIndex[table] == 0 {
						var tableDetail cloudstorage.TableDetail
						tableDetail.FromTableInfo(tableInfo)
						path := d.generateSchemaFilePath(tableDetail)
						encodedDetail, err := json.MarshalIndent(tableDetail, "", "    ")
						if err != nil {
							d.errCh <- err
							return
						}

						err = d.storage.WriteFile(ctx, path, encodedDetail)
						if err != nil {
							d.errCh <- err
							return
						}
					}

					path := d.generateDataFilePath(table)
					if err := d.statistics.RecordBatchExecution(func() (int, error) {
						err := d.storage.WriteFile(ctx, path, buf.Bytes())
						if err != nil {
							return 0, err
						}
						return rowsCnt, nil
					}); err != nil {
						d.errCh <- err
						return
					}
					d.metricFileCount.Add(1)

					for _, cb := range callbacks {
						if cb != nil {
							cb()
						}
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
	}()
}

// backgroundDispatchTasks dispatches flush tasks in two conditions:
// 1. the flush interval exceeds the upper limit.
// 2. the file size exceeds the upper limit.
func (d *dmlWorker) backgroundDispatchTasks(ctx context.Context, ch *chann.Chann[eventFragment]) {
	tableSet := make(map[*model.TableInfo]struct{})
	ticker := time.NewTicker(d.config.FlushInterval)

	d.wg.Add(1)
	go func() {
		log.Debug("dml worker started", zap.Int("workerID", d.id),
			zap.String("namespace", d.changeFeedID.Namespace),
			zap.String("changefeed", d.changeFeedID.ID))
		defer d.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if atomic.LoadUint64(&d.isClosed) == 1 {
					return
				}
				var readyTables []*model.TableInfo
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
					return
				case d.flushNotifyCh <- task:
					log.Debug("flush task is emitted successfully when flush interval exceeds",
						zap.Any("tables", task.targetTables))
					for tableInfo := range tableSet {
						tbl := versionedTable{
							TableName: tableInfo.TableName,
							version:   tableInfo.Version,
						}
						d.fileSize[tbl] = 0
					}
					tableSet = make(map[*model.TableInfo]struct{})
				default:
				}
			case frag, ok := <-ch.Out():
				if !ok || atomic.LoadUint64(&d.isClosed) == 1 {
					return
				}
				table := frag.versionedTable
				d.tableEvents.mu.Lock()
				d.tableEvents.fragments[table] = append(d.tableEvents.fragments[table], frag)
				d.tableEvents.mu.Unlock()

				tableSet[frag.event.Event.TableInfo] = struct{}{}
				for _, msg := range frag.encodedMsgs {
					if msg.Value != nil {
						d.fileSize[table] += uint64(len(msg.Value))
					}
				}
				// if the file size exceeds the upper limit, emit the flush task containing the table
				// as soon as possible.
				if d.fileSize[table] > uint64(d.config.FileSize) {
					task := flushTask{
						targetTables: []*model.TableInfo{frag.event.Event.TableInfo},
					}
					select {
					case <-ctx.Done():
						return
					case d.flushNotifyCh <- task:
						log.Debug("flush task is emitted successfully when file size exceeds",
							zap.Any("tables", table))
						d.fileSize[table] = 0
					default:
					}
				}
			}
		}
	}()
}

func (d *dmlWorker) generateSchemaFilePath(def cloudstorage.TableDetail) string {
	return fmt.Sprintf("%s/%s/%d/schema.json", def.Schema, def.Table, def.Version)
}

func (d *dmlWorker) generateDataFilePath(tbl versionedTable) string {
	var elems []string

	d.fileIndex[tbl]++
	elems = append(elems, tbl.Schema)
	elems = append(elems, tbl.Table)
	elems = append(elems, fmt.Sprintf("%d", tbl.version))

	if d.config.EnablePartitionSeparator && tbl.TableName.IsPartition {
		elems = append(elems, fmt.Sprintf("%d", tbl.TableID))
	}
	currTime := time.Now()
	switch d.config.DateSeparator {
	case config.DateSeparatorYear.String():
		elems = append(elems, currTime.Format("2006"))
	case config.DateSeparatorMonth.String():
		elems = append(elems, currTime.Format("2006-01"))
	case config.DateSeparatorDay.String():
		elems = append(elems, currTime.Format("2006-01-02"))
	}
	elems = append(elems, fmt.Sprintf("CDC%06d%s", d.fileIndex[tbl], d.extension))

	return strings.Join(elems, "/")
}

func (d *dmlWorker) close() {
	if !atomic.CompareAndSwapUint64(&d.isClosed, 0, 1) {
		return
	}

	d.wg.Wait()
}
