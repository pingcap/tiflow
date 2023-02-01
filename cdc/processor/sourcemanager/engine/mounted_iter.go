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

package engine

import (
	"context"
	"time"

	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	metrics "github.com/pingcap/tiflow/cdc/sorter"
	"github.com/prometheus/client_golang/prometheus"
)

// MountedEventIter is just like EventIterator, but returns mounted events.
type MountedEventIter struct {
	iter         EventIterator
	mg           entry.MounterGroup
	maxBatchSize int

	rawEvents      []rawEvent
	nextToEmit     int
	savedIterError error

	waitMount chan struct{}

	mountWaitDuration prometheus.Observer
}

// NewMountedEventIter creates a MountedEventIter instance.
func NewMountedEventIter(
	changefeedID model.ChangeFeedID,
	iter EventIterator,
	mg entry.MounterGroup,
	maxBatchSize int,
) *MountedEventIter {
	return &MountedEventIter{
		iter:         iter,
		mg:           mg,
		maxBatchSize: maxBatchSize,

		waitMount: make(chan struct{}, 1),

		mountWaitDuration: metrics.MountWaitDuration.WithLabelValues(changefeedID.Namespace, changefeedID.ID),
	}
}

// Next returns the next mounted event.
func (i *MountedEventIter) Next(ctx context.Context) (event *model.PolymorphicEvent, txnFinished Position, err error) {
	// There are no events in mounting. Fetch more events and mounting them.
	// The batch size is determined by `maxBatchSize`.
	if i.nextToEmit >= len(i.rawEvents) {
		if err = i.readBatch(ctx); err != nil {
			return
		}
	}

	// Check whether there are events in mounting or not.
	if i.nextToEmit < len(i.rawEvents) {
		idx := i.nextToEmit

		mountStart := time.Now()
		for !i.rawEvents[idx].event.Finished1.Load() {
			select {
			case <-i.waitMount:
			case <-ctx.Done():
				err = ctx.Err()
				return
			}
		}
		i.mountWaitDuration.Observe(time.Since(mountStart).Seconds())

		event = i.rawEvents[idx].event
		txnFinished = i.rawEvents[idx].txnFinished
		i.nextToEmit += 1
	}
	return
}

func (i *MountedEventIter) readBatch(ctx context.Context) error {
	if i.mg == nil || i.iter == nil {
		return nil
	}

	i.nextToEmit = 0
	if cap(i.rawEvents) == 0 {
		i.rawEvents = make([]rawEvent, 0, i.maxBatchSize)
	} else {
		i.rawEvents = i.rawEvents[:0]
	}

	for len(i.rawEvents) < cap(i.rawEvents) {
		event, txnFinished, err := i.iter.Next()
		if err != nil {
			return err
		}
		if event == nil {
			i.savedIterError = i.iter.Close()
			i.iter = nil
			break
		}
		task := entry.MountTask{Event: event, Finished: i.waitMount}
		if err := i.mg.AddEvent(ctx, task); err != nil {
			i.mg = nil
			return err
		}
		i.rawEvents = append(i.rawEvents, rawEvent{event, txnFinished})
	}
	return nil
}

// Close implements sorter.EventIterator.
func (i *MountedEventIter) Close() error {
	if i.savedIterError != nil {
		return i.savedIterError
	}
	if i.iter != nil {
		return i.iter.Close()
	}
	return nil
}

type rawEvent struct {
	event       *model.PolymorphicEvent
	txnFinished Position
}
