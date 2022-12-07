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

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/zap"
)

// MountedEventIter is just like EventIterator, but returns mounted events.
type MountedEventIter struct {
	iter         EventIterator
	mg           entry.MounterGroup
	maxBatchSize int

	rawEvents      []rawEvent
	nextToMount    int
	nextToEmit     int
	savedIterError error
}

// NewMountedEventIter creates a MountedEventIter instance.
func NewMountedEventIter(
	iter EventIterator,
	mg entry.MounterGroup,
	maxBatchSize int,
) *MountedEventIter {
	return &MountedEventIter{
		iter:         iter,
		mg:           mg,
		maxBatchSize: maxBatchSize,
	}
}

// Next returns the next mounted event.
func (i *MountedEventIter) Next(ctx context.Context) (event *model.PolymorphicEvent, txnFinished Position, err error) {
	defer func() {
		if event != nil {
			log.Info("[AAA] sorter output event", zap.Any("event", event))
		}
	}()
	// Check whether there are events in mounting or not.
	for idx := i.nextToEmit; idx < i.nextToMount; idx++ {
		if err = i.rawEvents[idx].event.WaitFinished(ctx); err == nil {
			event = i.rawEvents[idx].event
			txnFinished = i.rawEvents[idx].txnFinished
			i.nextToEmit += 1
		}
		return
	}

	// There are no events in mounting. Fetch more events and mounting them.
	// The batch size is determined by `maxBatchSize`.
	if i.mg != nil && i.iter != nil {
		i.nextToMount = 0
		i.nextToEmit = 0
		if cap(i.rawEvents) == 0 {
			i.rawEvents = make([]rawEvent, 0, i.maxBatchSize)
		} else {
			i.rawEvents = i.rawEvents[:0]
		}

		for len(i.rawEvents) < cap(i.rawEvents) {
			event, txnFinished, err = i.iter.Next()
			if err != nil {
				return
			}
			if event == nil {
				i.savedIterError = i.iter.Close()
				i.iter = nil
				break
			}
			i.rawEvents = append(i.rawEvents, rawEvent{event, txnFinished})
		}
		for idx := i.nextToMount; idx < len(i.rawEvents); idx++ {
			i.rawEvents[idx].event.SetUpFinishedCh()
			if err = i.mg.AddEvent(ctx, i.rawEvents[idx].event); err != nil {
				i.mg = nil
				return
			}
			i.nextToMount += 1
		}

		// More events are fetched and in mounting. So re-call this function to wait them.
		if i.nextToEmit < i.nextToMount {
			return i.Next(ctx)
		}
	}
	return
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
