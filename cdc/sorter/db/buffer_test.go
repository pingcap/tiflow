// Copyright 2021 PingCAP, Inc.
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

package db

import (
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sorter/db/message"
	"github.com/stretchr/testify/require"
)

func TestOutputBufferMaybeShrink(t *testing.T) {
	t.Parallel()
	advisedCapacity := 4
	buf := newOutputBuffer(advisedCapacity)
	require.Equal(t, 0, len(buf.resolvedEvents))
	require.Equal(t, 0, len(buf.deleteKeys))
	require.Equal(t, advisedCapacity, cap(buf.resolvedEvents))
	require.Equal(t, advisedCapacity, cap(buf.deleteKeys))

	// len == cap == advisedCapacity.
	buf.resolvedEvents = make([]*model.PolymorphicEvent, advisedCapacity)
	buf.resolvedEvents[0] = model.NewResolvedPolymorphicEvent(0, 1)
	buf.deleteKeys = make([]message.Key, advisedCapacity)
	buf.deleteKeys[0] = message.Key([]byte{1})
	resolvedEvents := append([]*model.PolymorphicEvent{}, buf.resolvedEvents...)
	deleteKeys := append([]message.Key{}, buf.deleteKeys...)

	buf.maybeShrink()
	require.Equal(t, advisedCapacity, len(buf.resolvedEvents))
	require.Equal(t, advisedCapacity, cap(buf.resolvedEvents))
	require.EqualValues(t, resolvedEvents, buf.resolvedEvents)
	require.EqualValues(t, deleteKeys, buf.deleteKeys)

	// len < cap == 2*advisedCapacity.
	buf.resolvedEvents = make([]*model.PolymorphicEvent, 2*advisedCapacity-1, 2*advisedCapacity)
	buf.resolvedEvents[0] = model.NewResolvedPolymorphicEvent(0, 1)
	buf.deleteKeys = make([]message.Key, 2*advisedCapacity-1, 2*advisedCapacity)
	buf.deleteKeys[0] = message.Key([]byte{1})
	resolvedEvents = append([]*model.PolymorphicEvent{}, buf.resolvedEvents...)
	deleteKeys = append([]message.Key{}, buf.deleteKeys...)

	buf.maybeShrink()
	require.Equal(t, 2*advisedCapacity-1, len(buf.resolvedEvents))
	require.Equal(t, 2*advisedCapacity-1, len(buf.deleteKeys))
	require.EqualValues(t, resolvedEvents, buf.resolvedEvents)
	require.EqualValues(t, deleteKeys, buf.deleteKeys)

	// len < cap/2 == advisedCapacity.
	buf.resolvedEvents = make([]*model.PolymorphicEvent, advisedCapacity-1, 2*advisedCapacity)
	buf.resolvedEvents[0] = model.NewResolvedPolymorphicEvent(0, 1)
	buf.deleteKeys = make([]message.Key, advisedCapacity-1, 2*advisedCapacity)
	buf.deleteKeys[0] = message.Key([]byte{1})
	resolvedEvents = append([]*model.PolymorphicEvent{}, buf.resolvedEvents...)
	deleteKeys = append([]message.Key{}, buf.deleteKeys...)

	buf.maybeShrink()
	require.Equal(t, advisedCapacity-1, len(buf.resolvedEvents))
	require.Equal(t, advisedCapacity-1, len(buf.deleteKeys))
	require.EqualValues(t, resolvedEvents, buf.resolvedEvents)
	require.EqualValues(t, deleteKeys, buf.deleteKeys)
}

func TestOutputBufferShiftResolvedEvents(t *testing.T) {
	t.Parallel()
	advisedCapacity := 64
	buf := newOutputBuffer(advisedCapacity)

	events := make([]*model.PolymorphicEvent, advisedCapacity)
	for i := range events {
		events[i] = &model.PolymorphicEvent{CRTs: uint64(1)}
	}

	for i := 0; i < advisedCapacity; i++ {
		buf.resolvedEvents = append([]*model.PolymorphicEvent{}, events...)
		buf.shiftResolvedEvents(i)
		require.EqualValues(t, buf.resolvedEvents, events[i:])
	}
}

func TestOutputBufferTryAppendResolvedEvent(t *testing.T) {
	t.Parallel()

	advisedCapacity := 2
	buf := newOutputBuffer(advisedCapacity)
	require.False(t, buf.partialReadTxn)

	require.True(t, buf.tryAppendResolvedEvent(&model.PolymorphicEvent{}))
	require.False(t, buf.partialReadTxn)
	require.True(t, buf.tryAppendResolvedEvent(&model.PolymorphicEvent{}))
	require.False(t, buf.partialReadTxn)

	// Failed append sets partialReadTxn
	require.False(t, buf.tryAppendResolvedEvent(&model.PolymorphicEvent{}))
	require.True(t, buf.partialReadTxn)

	buf.shiftResolvedEvents(2)
	l, _ := buf.len()
	require.Equal(t, 0, l)

	// A successful append resets partialReadTxn
	require.True(t, buf.tryAppendResolvedEvent(&model.PolymorphicEvent{}))
	require.False(t, buf.partialReadTxn)
}
