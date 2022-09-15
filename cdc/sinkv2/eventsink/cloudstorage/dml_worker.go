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

const defaultFlushInterval = 5 * time.Second

type dmlWorker struct {
	id           int
	changeFeedID model.ChangeFeedID
	storage      storage.ExternalStorage
	msgChannels  map[versionedTable]*chann.Chann[*common.Message]

	defragmenters  map[versionedTable]*defragmenter
	fileIndex      map[versionedTable]uint64
	activeTablesCh *chann.Chann[versionedTable]
	wg             sync.WaitGroup
	errCh          chan<- error
	extension      string
	flushing       uint64
}

type versionedTable struct {
	*model.TableName
	TableVersion uint64
}

func newDMLWorker(
	id int,
	changefeedID model.ChangeFeedID,
	storage storage.ExternalStorage,
	extension string,
	errCh chan<- error,
) *dmlWorker {
	d := &dmlWorker{
		id:             id,
		changeFeedID:   changefeedID,
		storage:        storage,
		msgChannels:    make(map[versionedTable]*chann.Chann[*common.Message]),
		defragmenters:  make(map[versionedTable]*defragmenter),
		activeTablesCh: chann.New[versionedTable](),
		fileIndex:      make(map[versionedTable]uint64),
		extension:      extension,
		errCh:          errCh,
	}

	return d
}

func (d *dmlWorker) flushEvents(ctx context.Context,
	readyTables []versionedTable,
	handleErr func(err error),
) {
	if !atomic.CompareAndSwapUint64(&d.flushing, 0, 1) {
		log.Debug("the previous flush run hasn't been finished yet")
		return
	}

	go func() {
		defer atomic.StoreUint64(&d.flushing, 0)

		var buf bytes.Buffer
		for _, tbl := range readyTables {
			buf.Reset()
			for msg := range d.msgChannels[tbl].Out() {
				if msg == nil {
					break
				}
				buf.Write(msg.Value)
			}

			err := d.storage.WriteFile(ctx, d.generateCloudStoragePath(tbl), buf.Bytes())
			if err != nil {
				handleErr(err)
				return
			}
		}
	}()

}

func (d *dmlWorker) backgroundDefragmenter(ctx context.Context) {
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case table := <-d.activeTablesCh.Out():
				_, err := d.defragmenters[table].writeMsgs(ctx, d.msgChannels[table])
				if err != nil {
					d.errCh <- err
					return
				}
			}
		}
	}()
}

func (d *dmlWorker) run(ctx context.Context, ch *chann.Chann[eventFragment]) {
	var readyTables []versionedTable
	var err error

	errCh := make(chan error, 1)
	handleErr := func(err error) { errCh <- err }
	d.backgroundDefragmenter(ctx)
	ticker := time.NewTicker(defaultFlushInterval)

	d.wg.Add(1)
	go func() {
		log.Debug("dml worker started", zap.Int("id", d.id),
			zap.String("namespace", d.changeFeedID.Namespace),
			zap.String("changefeed", d.changeFeedID.ID))
		defer d.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case err = <-errCh:
			case <-ticker.C:
				d.flushEvents(ctx, readyTables, handleErr)
				readyTables = nil
			case frag := <-ch.Out():
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

			if err != nil {
				break
			}
		}

		select {
		case <-ctx.Done():
		case d.errCh <- err:
		}
	}()
}

func (d *dmlWorker) generateCloudStoragePath(tbl versionedTable) string {
	d.fileIndex[tbl]++
	return fmt.Sprintf("%s/%s/%d/CDC%06d%s", tbl.Schema, tbl.Table, tbl.TableVersion,
		d.fileIndex[tbl], d.extension)
}

func (d *dmlWorker) stop() {
	d.activeTablesCh.Close()
	for range d.activeTablesCh.Out() {
		// drain the activeTablesCh
	}
	d.wg.Wait()
}
