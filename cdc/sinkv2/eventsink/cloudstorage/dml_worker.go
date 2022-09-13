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
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/chann"
)

type dmlWorker struct {
	storage       storage.ExternalStorage
	msgChannels   map[*model.TableName]*chann.Chann[*common.Message]
	defragmenters map[*model.TableName]*defragmenter
	wg            sync.WaitGroup
	id            int
}

func newDMLWorker(id int, storage storage.ExternalStorage) *dmlWorker {
	return &dmlWorker{
		id:            id,
		storage:       storage,
		msgChannels:   make(map[*model.TableName]*chann.Chann[*common.Message]),
		defragmenters: make(map[*model.TableName]*defragmenter),
	}
}

func (d *dmlWorker) flushEvents(ctx context.Context, readyTables []*model.TableName) error {
	for _, table := range readyTables {
		var data []byte
		_, err := d.defragmenters[table].output(ctx, d.msgChannels[table])
		if err != nil {
			return errors.Trace(err)
		}
		for msg := range d.msgChannels[table].Out() {
			data = append(data, msg.Value...)
		}

		d.storage.WriteFile(ctx, d.generateCloudStoragePath(table), data)
	}

	return nil
}

func (d *dmlWorker) run(ctx context.Context, ch *chann.Chann[eventFragment]) error {
	d.wg.Add(1)
	ticker := time.NewTicker(5 * time.Second)
	var readyTables []*model.TableName
	go func() {
		defer d.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				d.flushEvents(ctx, readyTables)
				readyTables = nil
			case frag := <-ch.Out():
				tableName := frag.tableName
				if frag.event == nil {
					d.defragmenters[tableName].setLast(frag.seqNumber)
					readyTables = append(readyTables, tableName)
					continue
				}

				if _, ok := d.defragmenters[tableName]; !ok {
					d.defragmenters[tableName] = newDefragmenter()
					d.msgChannels[tableName] = chann.New[*common.Message]()
				}
				d.defragmenters[tableName].register(frag)
			}
		}
	}()
	return nil
}

func (d *dmlWorker) generateCloudStoragePath(table *model.TableName) string {
	return ""
}

func (d *dmlWorker) stop() {
	d.wg.Wait()
}
