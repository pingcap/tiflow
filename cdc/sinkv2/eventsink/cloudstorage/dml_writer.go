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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sinkv2/metrics"
	"github.com/pingcap/tiflow/pkg/chann"
	"github.com/pingcap/tiflow/pkg/hash"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
	"golang.org/x/sync/errgroup"
)

// dmlWriter manages a set of dmlWorkers and dispatches eventFragment to
// the dmlWorker according to hash algorithm.
type dmlWriter struct {
	changefeedID   model.ChangeFeedID
	workers        []*dmlWorker
	workerChannels []*chann.Chann[eventFragment]
	hasher         *hash.PositionInertia
	storage        storage.ExternalStorage
	config         *cloudstorage.Config
	extension      string
	statistics     *metrics.Statistics
	inputCh        <-chan eventFragment
	errCh          chan<- error
}

func newDMLWriter(
	changefeedID model.ChangeFeedID,
	storage storage.ExternalStorage,
	config *cloudstorage.Config,
	extension string,
	statistics *metrics.Statistics,
	inputCh <-chan eventFragment,
	errCh chan<- error,
) *dmlWriter {
	w := &dmlWriter{
		changefeedID:   changefeedID,
		storage:        storage,
		workerChannels: make([]*chann.Chann[eventFragment], config.WorkerCount),
		workers:        make([]*dmlWorker, config.WorkerCount),
		hasher:         hash.NewPositionInertia(),
		config:         config,
		extension:      extension,
		statistics:     statistics,
		inputCh:        inputCh,
		errCh:          errCh,
	}

	return w
}

func (d *dmlWriter) run(ctx context.Context) error {
	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return d.dispatchFragToDMLWorker(ctx)
	})

	for i := 0; i < d.config.WorkerCount; i++ {
		worker := newDMLWorker(i, d.changefeedID, d.storage, d.config, d.extension, d.statistics)
		d.workers[i] = worker
		d.workerChannels[i] = chann.New[eventFragment]()
		ch := d.workerChannels[i]
		eg.Go(func() error {
			return worker.run(ctx, ch)
		})
	}

	return eg.Wait()
}

func (d *dmlWriter) dispatchFragToDMLWorker(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case frag, ok := <-d.inputCh:
			if !ok {
				return nil
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
