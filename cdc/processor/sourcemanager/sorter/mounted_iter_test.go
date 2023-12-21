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

package sorter

import (
	"context"
	"testing"

	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/memquota"
	"github.com/stretchr/testify/require"
)

type mockIter struct {
	repeatItem func() *model.PolymorphicEvent
}

func (i *mockIter) Next() (*model.PolymorphicEvent, Position, error) {
	return i.repeatItem(), Position{}, nil
}

func (i *mockIter) Close() error {
	return nil
}

func TestMountedEventIter(t *testing.T) {
	t.Parallel()

	rawIter := &mockIter{
		repeatItem: func() *model.PolymorphicEvent {
			return &model.PolymorphicEvent{
				RawKV: &model.RawKVEntry{
					Key:   []byte("testbytes"),
					Value: []byte("testbytes"),
				},
				Row: &model.RowChangedEvent{
					Table:        &model.TableName{Schema: "schema", Table: "table"},
					IndexColumns: [][]int{{1}},
				},
			}
		},
	}

	mg := &entry.MockMountGroup{}
	quota := memquota.NewMemQuota(model.ChangeFeedID{}, 1024*1024, "test")
	defer quota.Close()
	iter := NewMountedEventIter(model.ChangeFeedID{}, rawIter, mg, 3, quota)

	for i := 0; i < 3; i++ {
		event, _, err := iter.Next(context.Background())
		require.NotNil(t, event)
		require.Nil(t, err)
		if i != 2 {
			require.NotZero(t, quota.GetUsedBytes())
		} else {
			require.Zero(t, quota.GetUsedBytes())
		}
	}
	require.Equal(t, 3, iter.nextToEmit)

	rawIter.repeatItem = func() *model.PolymorphicEvent { return nil }
	event, _, err := iter.Next(context.Background())
	require.Nil(t, event)
	require.Nil(t, err)
	require.Equal(t, iter.nextToEmit, 0)
	require.Nil(t, iter.iter)
	require.Nil(t, iter.Close())
}

func TestMountedEventIterReadBatch(t *testing.T) {
	t.Parallel()

	rawIter := &mockIter{
		repeatItem: func() *model.PolymorphicEvent {
			return &model.PolymorphicEvent{
				RawKV: &model.RawKVEntry{
					Key:   []byte("testbytes"),
					Value: []byte("testbytes"),
				},
			}
		},
	}

	quota := memquota.NewMemQuota(model.ChangeFeedID{}, 1024*1024, "test")
	defer quota.Close()

	mg := &entry.MockMountGroup{IsFull: true}
	iter := NewMountedEventIter(model.ChangeFeedID{}, rawIter, mg, 3, quota)

	iter.readBatch(context.Background())
	require.NotNil(t, iter.rawEventBuffer.event)
	require.Equal(t, 1, len(iter.rawEvents))

	rawIter.repeatItem = func() *model.PolymorphicEvent { return nil }
	iter.readBatch(context.Background())
	require.Nil(t, iter.rawEventBuffer.event)
	require.Equal(t, 1, len(iter.rawEvents))

	require.Equal(t, uint64(36), quota.GetUsedBytes())
}
