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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/filter"
	"github.com/pingcap/tiflow/pkg/integrity"
	"github.com/pingcap/tiflow/pkg/util"
	"golang.org/x/sync/errgroup"
)

// MounterGroup is a group of mounter workers
type MounterGroup interface {
	util.Runnable

	AddEvent(ctx context.Context, event *model.PolymorphicEvent) error
	TryAddEvent(ctx context.Context, event *model.PolymorphicEvent) (bool, error)
}

type mounterGroup struct {
	schemaStorage SchemaStorage
	inputCh       chan *model.PolymorphicEvent
	tz            *time.Location
	filter        filter.Filter
	integrity     *integrity.Config

	workerNum int

	changefeedID model.ChangeFeedID
}

const (
	defaultMounterWorkerNum = 16
	defaultInputChanSize    = 1024
	defaultMetricInterval   = 15 * time.Second
)

// NewMounterGroup return a group of mounters.
func NewMounterGroup(
	schemaStorage SchemaStorage,
	workerNum int,
	filter filter.Filter,
	tz *time.Location,
	changefeedID model.ChangeFeedID,
	integrity *integrity.Config,
) *mounterGroup {
	if workerNum <= 0 {
		workerNum = defaultMounterWorkerNum
	}
	return &mounterGroup{
		schemaStorage: schemaStorage,
		inputCh:       make(chan *model.PolymorphicEvent, defaultInputChanSize),
		filter:        filter,
		tz:            tz,

		integrity: integrity,

		workerNum: workerNum,

		changefeedID: changefeedID,
	}
}

func (m *mounterGroup) Run(ctx context.Context, _ ...chan<- error) error {
	defer func() {
		mounterGroupInputChanSizeGauge.DeleteLabelValues(m.changefeedID.Namespace, m.changefeedID.ID)
	}()
	g, ctx := errgroup.WithContext(ctx)
	for i := 0; i < m.workerNum; i++ {
		g.Go(func() error {
			return m.runWorker(ctx)
		})
	}
	g.Go(func() error {
		metrics := mounterGroupInputChanSizeGauge.WithLabelValues(m.changefeedID.Namespace, m.changefeedID.ID)
		ticker := time.NewTicker(defaultMetricInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return errors.Trace(ctx.Err())
			case <-ticker.C:
				metrics.Set(float64(len(m.inputCh)))
			}
		}
	})
	return g.Wait()
}

func (m *mounterGroup) WaitForReady(_ context.Context) {}

func (m *mounterGroup) Close() {}

func (m *mounterGroup) runWorker(ctx context.Context) error {
	mounter := NewMounter(m.schemaStorage, m.changefeedID, m.tz, m.filter, m.integrity)
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case pEvent := <-m.inputCh:
			if pEvent.RawKV.OpType == model.OpTypeResolved {
				pEvent.MarkFinished()
				continue
			}
			err := mounter.DecodeEvent(ctx, pEvent)
			if err != nil {
				return errors.Trace(err)
			}
			pEvent.MarkFinished()
		}
	}
}

func (m *mounterGroup) AddEvent(ctx context.Context, event *model.PolymorphicEvent) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case m.inputCh <- event:
		return nil
	}
}

func (m *mounterGroup) TryAddEvent(ctx context.Context, event *model.PolymorphicEvent) (bool, error) {
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case m.inputCh <- event:
		return true, nil
	default:
		return false, nil
	}
}

// MockMountGroup is used for tests.
type MockMountGroup struct {
	IsFull bool
}

// Run implements util.Runnable.
func (m *MockMountGroup) Run(ctx context.Context, _ ...chan<- error) error {
	return nil
}

// WaitForReady implements util.Runnable.
func (m *MockMountGroup) WaitForReady(_ context.Context) {}

// Close implements util.Runnable.
func (m *MockMountGroup) Close() {}

// AddEvent implements MountGroup.
func (m *MockMountGroup) AddEvent(ctx context.Context, event *model.PolymorphicEvent) error {
	event.MarkFinished()
	return nil
}

// TryAddEvent implements MountGroup.
func (m *MockMountGroup) TryAddEvent(ctx context.Context, event *model.PolymorphicEvent) (bool, error) {
	if !m.IsFull {
		event.MarkFinished()
		return true, nil
	}
	return false, nil
}
