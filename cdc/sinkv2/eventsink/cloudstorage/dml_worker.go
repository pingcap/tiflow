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
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/chann"
	"go.uber.org/zap"
)

// dmlWorker denotes a worker responsible for writing messages to cloud storage.
type dmlWorker struct {
	// worker id
	id            int
	changeFeedID  model.ChangeFeedID
	storage       storage.ExternalStorage
	flushInterval time.Duration
	// flushNotifyCh is used to notify that several tables can be flushed.
	flushNotifyCh chan flushTask
	// defragmenters maintains a mapping of <table, defragmenter>.
	defragmenters sync.Map
	// fileIndex maintains a mapping of <table, index number>.
	fileIndex map[versionedTable]uint64
	wg        sync.WaitGroup
	isClosed  uint64
	errCh     chan<- error
	extension string
}

// flushTask defines a task containing the tables to be flushed.
type flushTask struct {
	activeTables map[versionedTable]struct{}
}

func newDMLWorker(
	id int,
	changefeedID model.ChangeFeedID,
	storage storage.ExternalStorage,
	flushInterval time.Duration,
	extension string,
	errCh chan<- error,
) *dmlWorker {
	d := &dmlWorker{
		id:            id,
		changeFeedID:  changefeedID,
		storage:       storage,
		flushInterval: flushInterval,
		flushNotifyCh: make(chan flushTask, 1),
		fileIndex:     make(map[versionedTable]uint64),
		extension:     extension,
		errCh:         errCh,
	}

	return d
}

// run creates a set of background goroutines.
func (d *dmlWorker) run(ctx context.Context, ch *chann.Chann[eventFragment]) {
	d.backgroundFlushMsgs(ctx)
	d.backgroundDispatchMsgs(ctx, ch)
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
				for table := range task.activeTables {
					buf.Reset()
					var callbacks []func()
					v, ok := d.defragmenters.Load(table)
					if !ok {
						log.Panic("failed to load defragmenter for table",
							zap.Int("workerID", d.id),
							zap.String("namespace", d.changeFeedID.ID),
							zap.String("changefeed", d.changeFeedID.ID),
							zap.String("schema", table.Schema),
							zap.String("table", table.Table),
						)
					}
					defrag := v.(*defragmenter)
					msgs := defrag.reassmebleFrag()

					for _, msg := range msgs {
						buf.Write(msg.Value)
						callbacks = append(callbacks, msg.Callback)
					}

					path := d.generateCloudStoragePath(table)
					err := d.storage.WriteFile(ctx, path, buf.Bytes())
					if err != nil {
						d.errCh <- err
						return
					}

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

// backgroundDispatchMsgs dispatch eventFragment to each table's defragmenter,
// and it periodically emits flush tasks.
func (d *dmlWorker) backgroundDispatchMsgs(ctx context.Context, ch *chann.Chann[eventFragment]) {
	readyTables := make(map[versionedTable]struct{})
	ticker := time.NewTicker(d.flushInterval)

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
				if len(readyTables) == 0 {
					continue
				}
				task := flushTask{
					activeTables: readyTables,
				}
				select {
				case <-ctx.Done():
					return
				case d.flushNotifyCh <- task:
					readyTables = nil
				default:
				}
			case frag := <-ch.Out():
				if atomic.LoadUint64(&d.isClosed) == 1 {
					return
				}
				table := versionedTable{
					TableName:    frag.tableName,
					TableVersion: frag.tableVersion,
				}

				if _, ok := d.defragmenters.Load(table); !ok {
					d.defragmenters.Store(table, newDefragmenter())
				}
				v, _ := d.defragmenters.Load(table)
				defrag := v.(*defragmenter)
				readyTables[table] = struct{}{}
				defrag.registerFrag(frag)
			}
		}
	}()
}

func (d *dmlWorker) generateCloudStoragePath(tbl versionedTable) string {
	d.fileIndex[tbl]++
	return fmt.Sprintf("%s/%s/%d/CDC%06d%s", tbl.Schema, tbl.Table, tbl.TableVersion,
		d.fileIndex[tbl], d.extension)
}

func (d *dmlWorker) close() {
	if !atomic.CompareAndSwapUint64(&d.isClosed, 0, 1) {
		return
	}

	d.wg.Wait()
}
