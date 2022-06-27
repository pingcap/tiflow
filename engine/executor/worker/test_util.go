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

package worker

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/atomic"

	"github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/clock"
)

type dummyWorker struct {
	id RunnableID

	needQuit atomic.Bool

	blockMu   sync.Mutex
	blockCond *sync.Cond
	blocked   bool

	submitTime atomic.Duration
}

func newDummyWorker(id RunnableID) *dummyWorker {
	ret := &dummyWorker{
		id:         id,
		submitTime: *atomic.NewDuration(0),
	}
	ret.blockCond = sync.NewCond(&ret.blockMu)
	return ret
}

func (d *dummyWorker) Init(ctx context.Context) error {
	d.blockMu.Lock()
	for d.blocked {
		d.blockCond.Wait()
	}
	d.blockMu.Unlock()

	rctx, ok := ToRuntimeCtx(ctx)
	if !ok {
		log.L().Panic("A RuntimeContext is expected to be used in unit tests")
	}
	d.submitTime.Store(time.Duration(rctx.SubmitTime()))

	return nil
}

func (d *dummyWorker) Poll(ctx context.Context) error {
	if d.needQuit.Load() {
		return errors.New("worker is finished")
	}
	return nil
}

func (d *dummyWorker) ID() RunnableID {
	return d.id
}

func (d *dummyWorker) Workload() model.RescUnit {
	return model.RescUnit(1)
}

func (d *dummyWorker) Close(ctx context.Context) error {
	return nil
}

func (d *dummyWorker) NotifyExit(ctx context.Context, errIn error) error {
	return nil
}

func (d *dummyWorker) SetFinished() {
	d.needQuit.Store(true)
}

func (d *dummyWorker) BlockInit() {
	d.blockMu.Lock()
	defer d.blockMu.Unlock()

	d.blocked = true
}

func (d *dummyWorker) UnblockInit() {
	d.blockMu.Lock()
	defer d.blockMu.Unlock()

	d.blocked = false
	d.blockCond.Broadcast()
}

func (d *dummyWorker) SubmitTime() clock.MonotonicTime {
	return clock.MonotonicTime(d.submitTime.Load())
}
