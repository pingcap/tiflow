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

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/chann"
	"github.com/pingcap/tiflow/pkg/hash"
)

type dmlWriter struct {
	workers        []*dmlWorker
	workerChannels []*chann.Chann[eventFragment]
	hasher         *hash.PositionInertia
	storage        storage.ExternalStorage
	concurrency    int
	extension      string
	errCh          chan<- error
}

func newDMLWriter(ctx context.Context,
	changefeedID model.ChangeFeedID,
	storage storage.ExternalStorage,
	concurrency int,
	extension string,
	errCh chan<- error,
) *dmlWriter {
	w := &dmlWriter{
		storage:        storage,
		workerChannels: make([]*chann.Chann[eventFragment], concurrency),
		hasher:         hash.NewPositionInertia(),
		concurrency:    concurrency,
		extension:      extension,
		errCh:          errCh,
	}

	for i := 0; i < concurrency; i++ {
		d := newDMLWorker(i+1, changefeedID, storage, extension, errCh)
		w.workerChannels[i] = chann.New[eventFragment]()
		d.run(ctx, w.workerChannels[i])
		w.workers = append(w.workers, d)
	}

	return w
}

func (d *dmlWriter) dispatchFragToDMLWorker(frag eventFragment) {
	tableName := frag.tableName
	d.hasher.Reset()
	d.hasher.Write([]byte(tableName.Schema), []byte(tableName.Table))
	workerID := d.hasher.Sum32() % uint32(d.concurrency)
	d.workerChannels[workerID].In() <- frag
}

func (d *dmlWriter) stop() {
	for _, w := range d.workers {
		w.stop()
	}

	for _, ch := range d.workerChannels {
		ch.Close()
		for range ch.Out() {
			// drain the worker channel
		}
	}
}
