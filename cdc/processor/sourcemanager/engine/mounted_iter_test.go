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
	"testing"

	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
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
				Row: &model.RowChangedEvent{
					Table:        &model.TableName{Schema: "schema", Table: "table"},
					IndexColumns: [][]int{{1}},
				},
			}
		},
	}
	itemSize := uint64(rawIter.repeatItem().Row.ApproximateBytes())

	mg := &entry.MockMountGroup{}
	iter := NewMountedEventIter(rawIter, mg, itemSize*3, 8)

	for i := 0; i < 3; i++ {
		event, _, err := iter.Next(context.Background())
		require.NotNil(t, event)
		require.Nil(t, err)
		require.Equal(t, itemSize*uint64(2-i), iter.totalMemUsage)
	}
	require.Equal(t, iter.nextToMount, 3)
	require.Equal(t, iter.nextToEmit, 3)

	rawIter.repeatItem = func() *model.PolymorphicEvent { return nil }
	event, _, err := iter.Next(context.Background())
	require.Nil(t, event)
	require.Nil(t, err)
	require.Equal(t, iter.nextToMount, 0)
	require.Equal(t, iter.nextToEmit, 0)
	require.Nil(t, iter.iter)
}
