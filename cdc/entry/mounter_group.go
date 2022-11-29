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
	"golang.org/x/sync/errgroup"
)

// MounterGroup is a group of mounter workers
type MounterGroup interface {
	Run(ctx context.Context) error
	AddEvent(ctx context.Context, event *model.PolymorphicEvent) error
}

type mounterGroup struct {
	schemaStorage  SchemaStorage
	inputCh        []chan *model.PolymorphicEvent
	tz             *time.Location
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
	tz *time.Location,
	changefeedID model.ChangeFeedID,
) *mounterGroup {
	if workerNum <= 0 {
		workerNum = defaultMounterWorkerNum
	}
	inputCh := make([]chan *model.PolymorphicEvent, workerNum)
	for i := 0; i < workerNum; i++ {
		inputCh[i] = make(chan *model.PolymorphicEvent, defaultInputChanSize)
	}
	return &mounterGroup{
		schemaStorage:  schemaStorage,
		inputCh:        inputCh,
		enableOldValue: enableOldValue,
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
	mounter := NewMounter(m.schemaStorage, m.changefeedID, m.tz, m.enableOldValue)
	rawCh := m.inputCh[index]
	metrics := mounterGroupInputChanSizeGauge.
		WithLabelValues(m.changefeedID.Namespace, m.changefeedID.ID, strconv.Itoa(index))
	ticker := time.NewTicker(defaultMetricInterval)
	defer ticker.Stop()
	for {
		var pEvent *model.PolymorphicEvent
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-ticker.C:
			metrics.Set(float64(len(rawCh)))
		case pEvent = <-rawCh:
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
	index := atomic.AddUint64(&m.index, 1) % uint64(m.workerNum)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case m.inputCh[index] <- event:
		return nil
	}
}
