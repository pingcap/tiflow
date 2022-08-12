// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package internal

import (
	"context"

	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"
)

type PoolImpl struct {
	workers       []*worker
	nextWorkerIdx atomic.Int64
}

func NewPoolImpl(workerNum int) *PoolImpl {
	var workers []*worker
	for i := 0; i < workerNum; i++ {
		workers = append(workers, newWorker())
	}
	return &PoolImpl{
		workers: workers,
	}
}

func (p *PoolImpl) Run(ctx context.Context) error {
	errg, subctx := errgroup.WithContext(ctx)
	for _, worker := range p.workers {
		worker := worker
		errg.Go(func() error {
			return worker.Run(subctx)
		})
	}
	return errg.Wait()
}

func (p *PoolImpl) RegisterHandler(handler Poller) {
	idx := (p.nextWorkerIdx.Add(1) - 1) % int64(len(p.workers))
	p.workers[idx].AddPoller(handler)
}
