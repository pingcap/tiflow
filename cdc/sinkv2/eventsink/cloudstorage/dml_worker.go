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

// dmlWorker denotes a worker responsible for writing messages to cloud storage.
type dmlWorker struct {
	// worker id
	id            int
	changeFeedID  model.ChangeFeedID
	storage       storage.ExternalStorage
	flushInterval time.Duration
	// msgChannels maintains a mapping of <table, unbounded message channel>.
	msgChannels sync.Map
	// flushNotifyCh is used to notify that several tables can be flushed.
	flushNotifyCh chan flushTask
	// defragmenters maintains a mapping of <table, defragmenter>.
	defragmenters sync.Map
	// fileIndex maintains a mapping of <table, index number>.
	fileIndex map[versionedTable]uint64
	// activeTablesCh is used to notify that a group of eventFragments are
	// sent to the corresponding defragmenter and we can reap messages from
	// the defragmenter right now.
	activeTablesCh *chann.Chann[versionedTable]
	wg             sync.WaitGroup
	isClosed       uint64
	errCh          chan<- error
	extension      string
}

// flushTask defines a task containing the tables to be flushed.
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
		flushNotifyCh:  make(chan flushTask, 1),
		activeTablesCh: chann.New[versionedTable](),
		fileIndex:      make(map[versionedTable]uint64),
		extension:      extension,
		errCh:          errCh,
	}

	return d
}

// run creates a set of background goroutines.
func (d *dmlWorker) run(ctx context.Context, ch *chann.Chann[eventFragment]) {
	d.backgroundDefragmentMsgs(ctx)
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
				for _, tbl := range task.activeTables {
					buf.Reset()
					var callbacks []func()
					v, ok := d.msgChannels.Load(tbl)
					if !ok {
						log.Panic("failed to load msg channel for table",
							zap.Int("workerID", d.id),
							zap.String("namespace", d.changeFeedID.ID),
							zap.String("changefeed", d.changeFeedID.ID),
							zap.String("schema", tbl.Schema),
							zap.String("table", tbl.Table),
						)
					}
					ch := v.(*chann.Chann[*common.Message])
					for msg := range ch.Out() {
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
					log.Debug("write file to storage success", zap.Int("workerID", d.id),
						zap.String("namespace", d.changeFeedID.Namespace),
						zap.String("changefeed", d.changeFeedID.ID),
						zap.String("schema", tbl.Schema),
						zap.String("table", tbl.Table),
						zap.String("path", path),
					)
				}
			}
		}
	}()
}

// backgroundDefragmentMsgs defragments eventFragments for each active table.
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
				v, ok = d.msgChannels.Load(table)
				if !ok {
					log.Panic("failed to load msg channel for table",
						zap.Int("workerID", d.id),
						zap.String("namespace", d.changeFeedID.ID),
						zap.String("changefeed", d.changeFeedID.ID),
						zap.String("schema", table.Schema),
						zap.String("table", table.Table),
					)
				}
				msgChan := v.(*chann.Chann[*common.Message])
				written, err := defrag.writeMsgs(ctx, msgChan)
				if err != nil {
					log.Error("dml worker failed to defragment messages",
						zap.Int("workerID", d.id),
						zap.String("namespace", d.changeFeedID.ID),
						zap.String("changefeed", d.changeFeedID.ID),
						zap.String("schema", table.Schema),
						zap.String("table", table.Table),
						zap.Error(err))

					d.errCh <- err
					return
				}
				log.Debug("dml worker defragments messages success",
					zap.Int("workerID", d.id),
					zap.String("namespace", d.changeFeedID.ID),
					zap.String("changefeed", d.changeFeedID.ID),
					zap.String("schema", table.Schema),
					zap.String("table", table.Table),
					zap.Uint64("written", written),
				)
			}
		}
	}()
}

// backgroundDispatchMsgs dispatch eventFragment to each table's defragmenter,
// and it periodically emits flush tasks.
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

				if _, ok := d.defragmenters.Load(table); !ok {
					d.defragmenters.Store(table, newDefragmenter())
					d.msgChannels.Store(table, chann.New[*common.Message]())
				}
				v, _ := d.defragmenters.Load(table)
				defrag := v.(*defragmenter)
				defrag.register(frag)

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
	// close the unbounded channel for defragmenter properly.
	d.defragmenters.Range(func(key, value any) bool {
		defrag := value.(*defragmenter)
		defrag.close()
		return true
	})
	// close the unbounded message channel properly.
	d.msgChannels.Range(func(key, value any) bool {
		msgCh := value.(*chann.Chann[*common.Message])
		msgCh.Close()
		for range msgCh.Out() {
			// drain the msgCh
		}
		return true
	})
	d.wg.Wait()
}
