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
	"github.com/pingcap/tiflow/pkg/chann"
	"github.com/pingcap/tiflow/pkg/filter"
	"golang.org/x/sync/errgroup"
)

// MounterGroup is a group of mounter workers
type MounterGroup interface {
	Run(ctx context.Context) error
	AddEvent(ctx context.Context, event *model.PolymorphicEvent) error
}

type mounterGroup struct {
	schemaStorage  SchemaStorage
	inputCh        *chann.Chann[*model.PolymorphicEvent]
	tz             *time.Location
	filter         filter.Filter
	enableOldValue bool

	workerNum int

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
	return &mounterGroup{
		schemaStorage:  schemaStorage,
		inputCh:        chann.New[*model.PolymorphicEvent](),
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
				metrics.Set(float64(m.inputCh.Len()))
			}
		}
	})
	return g.Wait()
}

func (m *mounterGroup) runWorker(ctx context.Context) error {
	mounter := NewMounter(m.schemaStorage, m.changefeedID, m.tz, m.filter, m.enableOldValue)
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case pEvent := <-m.inputCh.Out():
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
	case m.inputCh.In() <- event:
		return nil
	}
}

// MockMountGroup is used for tests.
type MockMountGroup struct{}

// Run implements MountGroup.
func (m *MockMountGroup) Run(ctx context.Context) error {
	return nil
}

// AddEvent implements MountGroup.
func (m *MockMountGroup) AddEvent(ctx context.Context, event *model.PolymorphicEvent) error {
	event.MarkFinished()
	return nil
}
