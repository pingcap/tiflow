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
	"github.com/prometheus/client_golang/prometheus"
)

// MountedEventIter is just like EventIterator, but returns mounted events.
type MountedEventIter struct {
	iter         EventIterator
	mg           entry.MounterGroup
	maxBatchSize int

	inMountingEvents []rawEvent
	toMountEvent     rawEvent
	nextToEmit       int
	savedIterError   error

	waitMount         chan struct{}
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

		waitMount:         make(chan struct{}, 1),
		mountWaitDuration: mountWaitDuration.WithLabelValues(changefeedID.Namespace, changefeedID.ID),
	}
}

// Next returns the next mounted event.
func (i *MountedEventIter) Next(ctx context.Context) (event *model.PolymorphicEvent, txnFinished Position, err error) {
	// There are no events in mounting. Fetch more events and mounting them.
	// The batch size is determined by `maxBatchSize`.
	if i.nextToEmit >= len(i.inMountingEvents) {
		if err = i.readBatch(ctx); err != nil {
			return
		}
	}

	// Check whether there are events in mounting or not.
	if i.nextToEmit < len(i.inMountingEvents) {
		idx := i.nextToEmit

		mountStart := time.Now()
		for !i.inMountingEvents[idx].event.Mounted.Load() {
			select {
			case <-i.waitMount:
			case <-ctx.Done():
				err = ctx.Err()
				return
			}
		}
		i.mountWaitDuration.Observe(time.Since(mountStart).Seconds())

		event = i.inMountingEvents[idx].event
		txnFinished = i.inMountingEvents[idx].txnFinished
		i.nextToEmit += 1
	}
	return
}

func (i *MountedEventIter) readBatch(ctx context.Context) error {
	if i.mg == nil || i.iter == nil {
		return nil
	}

	i.nextToEmit = 0
	if cap(i.inMountingEvents) == 0 {
		i.inMountingEvents = make([]rawEvent, 0, i.maxBatchSize)
	} else {
		i.inMountingEvents = i.inMountingEvents[:0]
	}

	if i.toMountEvent.event != nil {
		if err := i.mg.AddEvent(ctx, i.eventToTask(i.toMountEvent.event)); err != nil {
			return err
		}
		i.inMountingEvents = append(i.inMountingEvents, i.toMountEvent)
		i.toMountEvent.event = nil
	}

	for len(i.inMountingEvents) < i.maxBatchSize {
		event, txnFinished, err := i.iter.Next()
		if err != nil {
			return err
		}
		if event == nil {
			i.savedIterError = i.iter.Close()
			i.iter = nil
			break
		}

		inMounting := false
		if len(i.inMountingEvents) > 0 {
			inMounting, err = i.mg.TryAddEvent(ctx, i.eventToTask(event))
		} else {
			err = i.mg.AddEvent(ctx, i.eventToTask(event))
			inMounting = true
		}
		if err != nil {
			return err
		}
		if !inMounting {
			i.toMountEvent = rawEvent{event, txnFinished}
			break
		}
	}
	return nil
}

func (i *MountedEventIter) eventToTask(event *model.PolymorphicEvent) entry.MountTask {
	return entry.MountTask{
		Event: event,
		PostFinish: func() {
			select {
			case i.waitMount <- struct{}{}:
			default:
			}
		},
	}
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
