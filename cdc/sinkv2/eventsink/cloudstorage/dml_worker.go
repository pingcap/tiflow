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
	"github.com/pingcap/tiflow/cdc/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/chann"
	"go.uber.org/zap"
)

type dmlWorker struct {
	id             int
	changeFeedID   model.ChangeFeedID
	storage        storage.ExternalStorage
	flushInterval  time.Duration
	msgChannels    map[versionedTable]*chann.Chann[*common.Message]
	flushNotifyCh  chan flushTask
	defragmenters  map[versionedTable]*defragmenter
	fileIndex      map[versionedTable]uint64
	activeTablesCh *chann.Chann[versionedTable]
	wg             sync.WaitGroup
	isClosed       uint64
	errCh          chan<- error
	extension      string
}

type flushTask struct {
	activeTables []versionedTable
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
		id:             id,
		changeFeedID:   changefeedID,
		storage:        storage,
		flushInterval:  flushInterval,
		msgChannels:    make(map[versionedTable]*chann.Chann[*common.Message]),
		defragmenters:  make(map[versionedTable]*defragmenter),
		flushNotifyCh:  make(chan flushTask, 1),
		activeTablesCh: chann.New[versionedTable](),
		fileIndex:      make(map[versionedTable]uint64),
		extension:      extension,
		errCh:          errCh,
	}

	return d
}

func (d *dmlWorker) run(ctx context.Context, ch *chann.Chann[eventFragment]) {
	d.backgroundDefragmentMsgs(ctx)
	d.backgroundFlushMsgs(ctx)
	d.backgroundDispatchMsgs(ctx, ch)
}

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
				for _, tbl := range task.activeTables {
					buf.Reset()
					var callbacks []func()
					for msg := range d.msgChannels[tbl].Out() {
						if msg == nil {
							break
						}
						buf.Write(msg.Value)
						callbacks = append(callbacks, msg.Callback)
					}

					path := d.generateCloudStoragePath(tbl)
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
					log.Debug("write file to storage success", zap.String("path", path),
						zap.Int("workerID", d.id),
						zap.String("namespace", d.changeFeedID.Namespace),
						zap.String("changefeed", d.changeFeedID.ID))
				}
			}
		}
	}()

}

func (d *dmlWorker) backgroundDefragmentMsgs(ctx context.Context) {
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case table := <-d.activeTablesCh.Out():
				if atomic.LoadUint64(&d.isClosed) == 1 {
					return
				}
				_, err := d.defragmenters[table].writeMsgs(ctx, d.msgChannels[table])
				if err != nil {
					log.Error("dml worker failed to flush messages to cloud storage sink",
						zap.Int("id", d.id),
						zap.String("namespace", d.changeFeedID.ID),
						zap.String("changefeed", d.changeFeedID.ID),
						zap.Error(err))

					d.errCh <- err
					return
				}
			}
		}
	}()
}

func (d *dmlWorker) backgroundDispatchMsgs(ctx context.Context, ch *chann.Chann[eventFragment]) {
	var readyTables []versionedTable
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
				if _, ok := d.defragmenters[table]; !ok {
					d.defragmenters[table] = newDefragmenter()
					d.msgChannels[table] = chann.New[*common.Message]()
				}
				d.defragmenters[table].register(frag)
				if frag.event == nil {
					readyTables = append(readyTables, table)
					d.activeTablesCh.In() <- table
				}
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
	d.activeTablesCh.Close()
	for range d.activeTablesCh.Out() {
		// drain the activeTablesCh
	}
	d.wg.Wait()
}
