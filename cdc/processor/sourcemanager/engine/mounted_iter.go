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
	"github.com/pingcap/tiflow/cdc/processor/memquota"
	"github.com/prometheus/client_golang/prometheus"
)

// MountedEventIter is just like EventIterator, but returns mounted events.
type MountedEventIter struct {
	iter  EventIterator
	mg    entry.MounterGroup
	quota *memquota.MemQuota

	rawEvents      []rawEvent
	nextToEmit     int
	savedIterError error

	waitMount         chan struct{}
	mountWaitDuration prometheus.Observer
}

// NewMountedEventIter creates a MountedEventIter instance.
func NewMountedEventIter(
	changefeedID model.ChangeFeedID,
	iter EventIterator,
	mg entry.MounterGroup,
	maxBatchSize int,
	quota *memquota.MemQuota,
) *MountedEventIter {
	return &MountedEventIter{
		iter:      iter,
		mg:        mg,
		quota:     quota,
		rawEvents: make([]rawEvent, 0, maxBatchSize),

		waitMount:         make(chan struct{}, 1),
		mountWaitDuration: mountWaitDuration.WithLabelValues(changefeedID.Namespace, changefeedID.ID),
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

		startWait := time.Now()
		for !i.rawEvents[idx].event.Mounted.Load() {
			select {
			case <-i.waitMount:
			case <-ctx.Done():
				err = ctx.Err()
				return
			}
		}
		i.mountWaitDuration.Observe(time.Since(startWait).Seconds())
		i.quota.Refund(uint64(i.rawEvents[idx].size))

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
	i.rawEvents = i.rawEvents[:0]
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

		task := entry.MountTask{
			Event: event,
			PostFinish: func() {
				select {
				case i.waitMount <- struct{}{}:
				default:
				}
			},
		}
		if err := i.mg.AddEvent(ctx, task); err != nil {
			i.mg = nil
			return err
		}

		var size int64
		if event.RawKV != nil {
			size = event.RawKV.ApproximateDataSize()
		}
		i.rawEvents = append(i.rawEvents, rawEvent{event, txnFinished, size})
		if !i.quota.TryAcquire(uint64(size)) {
			i.quota.ForceAcquire(uint64(size))
			break
		}
	}
	return nil
}

// Close implements sorter.EventIterator.
func (i *MountedEventIter) Close() error {
	for idx := i.nextToEmit; idx < len(i.rawEvents); idx++ {
		if i.rawEvents[idx].size != 0 {
			i.quota.Refund(uint64(i.rawEvents[idx].size))
		}
	}
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
	size        int64
}
