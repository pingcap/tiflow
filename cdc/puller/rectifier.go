// Copyright 2020 PingCAP, Inc.
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

package puller

import (
	"context"
	"sync/atomic"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"golang.org/x/sync/errgroup"
)

// Rectifier filters and collates the output stream from the sorter
type Rectifier struct {
	EventSorter

	status            model.SorterStatus
	maxSentResolvedTs model.Ts
	targetTs          model.Ts

	outputCh chan *model.PolymorphicEvent
}

// NewRectifier creates a new Rectifier
func NewRectifier(s EventSorter, targetTs model.Ts) *Rectifier {
	return &Rectifier{
		EventSorter: s,
		targetTs:    targetTs,
		outputCh:    make(chan *model.PolymorphicEvent),
	}
}

// GetStatus returns the state of the Rectifier
func (r *Rectifier) GetStatus() model.SorterStatus {
	return atomic.LoadInt32(&r.status)
}

// GetMaxResolvedTs returns the maximum resolved ts sent from the Rectifier
func (r *Rectifier) GetMaxResolvedTs() model.Ts {
	return atomic.LoadUint64(&r.maxSentResolvedTs)
}

// SafeStop stops the Rectifier and Sorter safety
func (r *Rectifier) SafeStop() {
	atomic.CompareAndSwapInt32(&r.status,
		model.SorterStatusWorking,
		model.SorterStatusStopping)
}

// Run running the Rectifier
func (r *Rectifier) Run(ctx context.Context) error {
	output := func(event *model.PolymorphicEvent) {
		select {
		case <-ctx.Done():
			log.Warn("")
		case r.outputCh <- event:
		}
	}
	errg, ctx := errgroup.WithContext(ctx)
	errg.Go(func() error {
		return r.EventSorter.Run(ctx)
	})
	errg.Go(func() error {
		defer close(r.outputCh)
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case event := <-r.EventSorter.Output():
				if event == nil {
					return nil
				}
				if event.CRTs > r.targetTs {
					output(model.NewResolvedPolymorphicEvent(r.targetTs))
					atomic.StoreUint64(&r.maxSentResolvedTs, r.targetTs)
					atomic.StoreInt32(&r.status, model.SorterStatusFinished)
					return nil
				}
				output(event)
				if event.RawKV.OpType == model.OpTypeResolved {
					atomic.StoreUint64(&r.maxSentResolvedTs, event.CRTs)
					switch atomic.LoadInt32(&r.status) {
					case model.SorterStatusStopping:
						atomic.StoreInt32(&r.status, model.SorterStatusStopped)
						return nil
					case model.SorterStatusStopped:
						return nil
					case model.SorterStatusWorking:
					}
				}
			}
		}
	})
	return errg.Wait()
}

// Output returns the output streams
func (r *Rectifier) Output() <-chan *model.PolymorphicEvent {
	return r.outputCh
}
