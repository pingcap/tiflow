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
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/chann"
	"go.uber.org/zap"
)

const defaultFlushInterval = 5 * time.Second

type dmlWorker struct {
	storage        storage.ExternalStorage
	msgChannels    map[*model.TableName]*chann.Chann[*common.Message]
	defragmenters  map[*model.TableName]*defragmenter
	fileIndex      map[*model.TableName]uint64
	activeTablesCh *chann.Chann[*model.TableName]
	wg             sync.WaitGroup
	errCh          chan<- error
	extension      string
	id             int
}

func newDMLWorker(
	id int,
	storage storage.ExternalStorage,
	extension string,
	errCh chan<- error,
) *dmlWorker {
	d := &dmlWorker{
		id:             id,
		storage:        storage,
		msgChannels:    make(map[*model.TableName]*chann.Chann[*common.Message]),
		defragmenters:  make(map[*model.TableName]*defragmenter),
		activeTablesCh: chann.New[*model.TableName](),
		fileIndex:      make(map[*model.TableName]uint64),
		extension:      extension,
		errCh:          errCh,
	}

	return d
}

func (d *dmlWorker) flushEvents(ctx context.Context, readyTables []*model.TableName) error {
	for _, table := range readyTables {
		var data []byte
	LOOP:
		for {
			select {
			case msg := <-d.msgChannels[table].Out():
				data = append(data, msg.Value...)
			default:
				break LOOP
			}
		}

		err := d.storage.WriteFile(ctx, d.generateCloudStoragePath(table), data)
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
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
	d.backgroundDefragmenter(ctx)
	d.wg.Add(1)
	ticker := time.NewTicker(defaultFlushInterval)
	var readyTables []*model.TableName
	go func() {
		log.Info("dml worker started", zap.Int("id", d.id))
		defer d.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				err := d.flushEvents(ctx, readyTables)
				if err != nil {
					d.errCh <- err
					return
				}
				readyTables = nil
			case frag := <-ch.Out():
				tableName := frag.tableName
				if _, ok := d.defragmenters[tableName]; !ok {
					d.defragmenters[tableName] = newDefragmenter()
					d.msgChannels[tableName] = chann.New[*common.Message]()
				}
				d.defragmenters[tableName].register(frag)
				if frag.event == nil {
					readyTables = append(readyTables, tableName)
					d.activeTablesCh.In() <- tableName
				}
			}
		}
	}()
}

func (d *dmlWorker) generateCloudStoragePath(table *model.TableName) string {
	d.fileIndex[table]++
	return fmt.Sprintf("%s/%s/CDC%6d.%s", table.Schema, table.Table, d.fileIndex[table], d.extension)
}

func (d *dmlWorker) stop() {
	d.wg.Wait()
}
