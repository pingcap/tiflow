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

package entry

import (
	"context"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/filter"
	"golang.org/x/sync/errgroup"
)

// MounterGroup is a group of mounter workers
type MounterGroup interface {
	Run(ctx context.Context) error
	AddEvent(ctx context.Context, event *model.PolymorphicEvent, outputCh chan<- *Future) error
}

type mounterGroup struct {
	schemaStorage  SchemaStorage
	inputCh        []chan *Future
	tz             *time.Location
	filter         filter.Filter
	enableOldValue bool

	workerNum int
	index     uint64

	changefeedID model.ChangeFeedID
}

const (
	defaultMounterWorkerNum = 16
	defaultInputChanSize    = 256
	defaultMetricInterval   = 15 * time.Second
)

// NewMounterGroup return a group of mounters.
func NewMounterGroup(
	schemaStorage SchemaStorage,
	workerNum int,
	enableOldValue bool,
	filter filter.Filter,
	tz *time.Location,
	changefeedID model.ChangeFeedID,
) *mounterGroup {
	if workerNum <= 0 {
		workerNum = defaultMounterWorkerNum
	}
	inputCh := make([]chan *Future, workerNum)
	for i := 0; i < workerNum; i++ {
		inputCh[i] = make(chan *Future, defaultInputChanSize)
	}
	return &mounterGroup{
		schemaStorage:  schemaStorage,
		inputCh:        inputCh,
		enableOldValue: enableOldValue,
		filter:         filter,
		tz:             tz,

		workerNum: workerNum,

		changefeedID: changefeedID,
	}
}

func (m *mounterGroup) Run(ctx context.Context) error {
	defer func() {
		mounterGroupInputChanSizeGauge.DeleteLabelValues(m.changefeedID.Namespace, m.changefeedID.ID)
	}()
	g, ctx := errgroup.WithContext(ctx)
	for i := 0; i < m.workerNum; i++ {
		idx := i
		g.Go(func() error {
			return m.runWorker(ctx, idx)
		})
	}
	return g.Wait()
}

func (m *mounterGroup) runWorker(ctx context.Context, index int) error {
	mounter := NewMounter(m.schemaStorage, m.changefeedID, m.tz, m.filter, m.enableOldValue)
	inputCh := m.inputCh[index]
	metrics := mounterGroupInputChanSizeGauge.
		WithLabelValues(m.changefeedID.Namespace, m.changefeedID.ID, strconv.Itoa(index))
	ticker := time.NewTicker(defaultMetricInterval)
	defer ticker.Stop()
	for {
		var future *Future
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-ticker.C:
			metrics.Set(float64(len(inputCh)))
		case future = <-inputCh:
			err := mounter.DecodeEvent(ctx, future.Raw)
			if err != nil {
				return errors.Trace(err)
			}
			future.ready()
		}
	}
}

func (m *mounterGroup) AddEvent(
	ctx context.Context,
	event *model.PolymorphicEvent,
	outputCh chan<- *Future,
) error {
	index := atomic.AddUint64(&m.index, 1) % uint64(m.workerNum)
	future := newFuture(event)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case m.inputCh[index] <- future:
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case outputCh <- future:
	}
	return nil
}

func NewMounterFutureOutput() chan *Future {
	return make(chan *Future, 1024)
}

type Future struct {
	Raw *model.PolymorphicEvent

	Result *model.RowChangedEvent
	done   chan struct{}
}

func newFuture(raw *model.PolymorphicEvent) *Future {
	return &Future{
		Raw:  raw,
		done: make(chan struct{}),
	}
}

func (f *Future) ready() {
	close(f.done)
}

func (f *Future) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-f.done:
	}
	return nil
}

// MockMountGroup is used for tests.
type MockMountGroup struct{}

// Run implements MountGroup.
func (m *MockMountGroup) Run(ctx context.Context) error {
	return nil
}

// AddEvent implements MountGroup.
func (m *MockMountGroup) AddEvent(ctx context.Context, event *model.PolymorphicEvent, outputCh chan<- *Future) error {
	return nil
}
