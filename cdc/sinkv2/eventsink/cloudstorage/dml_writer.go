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

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/chann"
	"github.com/pingcap/tiflow/pkg/hash"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
)

// dmlWriter manages a set of dmlWorkers and dispatches eventFragment to
// the dmlWorker according to hash algorithm.
type dmlWriter struct {
	workers        []*dmlWorker
	workerChannels []*chann.Chann[eventFragment]
	hasher         *hash.PositionInertia
	storage        storage.ExternalStorage
	config         *cloudstorage.Config
	extension      string
	wg             sync.WaitGroup
	inputCh        *chann.Chann[eventFragment]
	errCh          chan<- error
}

func newDMLWriter(ctx context.Context,
	changefeedID model.ChangeFeedID,
	storage storage.ExternalStorage,
	config *cloudstorage.Config,
	extension string,
	inputCh *chann.Chann[eventFragment],
	errCh chan<- error,
) *dmlWriter {
	w := &dmlWriter{
		storage:        storage,
		workerChannels: make([]*chann.Chann[eventFragment], config.WorkerCount),
		hasher:         hash.NewPositionInertia(),
		config:         config,
		extension:      extension,
		inputCh:        inputCh,
		errCh:          errCh,
	}

	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		w.dispatchFragToDMLWorker(ctx)
	}()

	for i := 0; i < config.WorkerCount; i++ {
		d := newDMLWorker(i, changefeedID, storage, w.config, extension, errCh)
		w.workerChannels[i] = chann.New[eventFragment]()
		d.run(ctx, w.workerChannels[i])
		w.workers = append(w.workers, d)
	}

	return w
}

func (d *dmlWriter) dispatchFragToDMLWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case frag, ok := <-d.inputCh.Out():
			if !ok {
				return
			}
			tableName := frag.TableName
			d.hasher.Reset()
			d.hasher.Write([]byte(tableName.Schema), []byte(tableName.Table))
			workerID := d.hasher.Sum32() % uint32(d.config.WorkerCount)
			d.workerChannels[workerID].In() <- frag
		}
	}
}

func (d *dmlWriter) close() {
	d.wg.Wait()
	for _, w := range d.workers {
		w.close()
	}
	for _, ch := range d.workerChannels {
		ch.Close()
		for range ch.Out() {
			// drain the worker channel
		}
	}
}
