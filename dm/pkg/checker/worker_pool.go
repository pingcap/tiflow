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

package checker

import (
	"context"

	"golang.org/x/sync/errgroup"
)

// WorkerPool is a easy-to-use worker pool that can start workers by Go then use
// PutJob to send jobs to worker. After worker finished a job, the result is
// sequentially called by resultHandler function which is the parameter of
// NewWorkerPool or NewWorkerPoolWithContext. After caller send all jobs, it can
// call Wait to make sure all jobs are finished.
// The type parameter J means job, R means result. Type J MUST only share
// concurrent-safe member like *sql.DB.
type WorkerPool[J, R any] struct {
	ctx context.Context

	// the closing order is inCh -> outCh -> done
	inCh     chan J
	outCh    chan R
	done     chan struct{}
	errGroup *errgroup.Group
}

// NewWorkerPool creates a new worker pool.
// The type parameter J means job, R means result. Type J MUST only share
// concurrent-safe member like *sql.DB.
func NewWorkerPool[J, R any](resultHandler func(R)) *WorkerPool[J, R] {
	return NewWorkerPoolWithContext[J, R](context.Background(), resultHandler)
}

// NewWorkerPoolWithContext creates a new worker pool with a context which may
// be canceled from caller.
// The type parameter J means job, R means result. Type J MUST only share
// concurrent-safe member like *sql.DB.
func NewWorkerPoolWithContext[J, R any](
	ctx context.Context,
	resultHandler func(R),
) *WorkerPool[J, R] {
	group, groupCtx := errgroup.WithContext(ctx)
	ret := &WorkerPool[J, R]{
		ctx:      groupCtx,
		errGroup: group,
		inCh:     make(chan J),
		outCh:    make(chan R),
		done:     make(chan struct{}),
	}
	go func() {
		for r := range ret.outCh {
			resultHandler(r)
		}
		close(ret.done)
	}()

	return ret
}

// Go is like a builtin go keyword. handler represents the logic of worker, if
// the worker has initializing logic, caller can use method of structure or
// closure to refer to the initialized part.
func (p *WorkerPool[J, R]) Go(handler func(ctx context.Context, job J) (R, error)) {
	p.errGroup.Go(func() error {
		for {
			select {
			case <-p.ctx.Done():
				return p.ctx.Err()
			case job, ok := <-p.inCh:
				if !ok {
					return nil
				}
				result, err := handler(p.ctx, job)
				if err != nil {
					return err
				}
				p.outCh <- result
			}
		}
	})
}

// PutJob sends a job to worker pool. The return value means whether the workers
// are stopped so caller can stop early.
func (p *WorkerPool[J, R]) PutJob(job J) bool {
	select {
	case <-p.ctx.Done():
		return false
	case p.inCh <- job:
		return true
	}
}

// Wait waits all workers to finish. It will return the first error occurred in
// workers, or nil if no error.
// Other methods should not be called concurrent with Wait or after Wait.
func (p *WorkerPool[J, R]) Wait() error {
	close(p.inCh)
	err := p.errGroup.Wait()
	close(p.outCh)
	<-p.done
	return err
}
