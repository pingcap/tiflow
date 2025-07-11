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

package partition

import (
	"testing"

	timodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestIndexValueDispatcher(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		row             *model.RowChangedEvent
		expectPartition int32
	}{
		{row: &model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "test",
				Table:  "t1",
			},
			Columns: []*model.Column{
				{
					Name:  "a",
					Value: 11,
					Flag:  model.HandleKeyFlag,
				}, {
					Name:  "b",
					Value: 22,
					Flag:  0,
				},
			},
		}, expectPartition: 2},
		{row: &model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "test",
				Table:  "t1",
			},
			Columns: []*model.Column{
				{
					Name:  "a",
					Value: 22,
					Flag:  model.HandleKeyFlag,
				}, {
					Name:  "b",
					Value: 22,
					Flag:  0,
				},
			},
		}, expectPartition: 11},
		{row: &model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "test",
				Table:  "t1",
			},
			Columns: []*model.Column{
				{
					Name:  "a",
					Value: 11,
					Flag:  model.HandleKeyFlag,
				}, {
					Name:  "b",
					Value: 33,
					Flag:  0,
				},
			},
		}, expectPartition: 2},
		{row: &model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "test",
				Table:  "t2",
			},
			Columns: []*model.Column{
				{
					Name:  "a",
					Value: 11,
					Flag:  model.HandleKeyFlag,
				}, {
					Name:  "b",
					Value: 22,
					Flag:  model.HandleKeyFlag,
				},
			},
		}, expectPartition: 5},
		{row: &model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "test",
				Table:  "t2",
			},
			Columns: []*model.Column{
				{
					Name:  "b",
					Value: 22,
					Flag:  model.HandleKeyFlag,
				}, {
					Name:  "a",
					Value: 11,
					Flag:  model.HandleKeyFlag,
				},
			},
		}, expectPartition: 5},
		{row: &model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "test",
				Table:  "t2",
			},
			Columns: []*model.Column{
				{
					Name:  "a",
					Value: 11,
					Flag:  model.HandleKeyFlag,
				}, {
					Name:  "b",
					Value: 0,
					Flag:  model.HandleKeyFlag,
				},
			},
		}, expectPartition: 14},
		{row: &model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "test",
				Table:  "t2",
			},
			Columns: []*model.Column{
				{
					Name:  "a",
					Value: 11,
					Flag:  model.HandleKeyFlag,
				}, {
					Name:  "b",
					Value: 33,
					Flag:  model.HandleKeyFlag,
				},
			},
		}, expectPartition: 2},
	}
	p := NewIndexValueDispatcher("")
	for _, tc := range testCases {
		index, _, err := p.DispatchRowChangedEvent(tc.row, 16)
		require.Equal(t, tc.expectPartition, index)
		require.NoError(t, err)
	}
}

func TestIndexValueDispatcherWithIndexName(t *testing.T) {
	t.Parallel()

	event := &model.RowChangedEvent{
		Table: &model.TableName{
			Schema: "test",
			Table:  "t1",
		},
		TableInfo: model.WrapTableInfo(100, "test", 100, &timodel.TableInfo{
			Indices: []*timodel.IndexInfo{
				{
					Name: timodel.NewCIStr("index1"),
					Columns: []*timodel.IndexColumn{
						{Name: timodel.NewCIStr("COL2"), Offset: 1},
						{Name: timodel.NewCIStr("Col1"), Offset: 0},
					},
					Primary: true,
				},
			},
			Columns: []*timodel.ColumnInfo{
				{
					ID:     0,
					Name:   timodel.NewCIStr("COL2"),
					Offset: 1,
				},
				{
					ID:     1,
					Name:   timodel.NewCIStr("Col1"),
					Offset: 0,
				},
				{
					ID:     2,
					Name:   timodel.NewCIStr("col3"),
					Offset: 2,
				},
			},
		}),
		Columns: []*model.Column{
			{
				Name:  "Col1",
				Value: 11,
				Flag:  model.HandleKeyFlag,
			},
			{
				Name:  "COL2",
				Value: 22,
				Flag:  model.HandleKeyFlag,
			},
			{
				Name:  "col3",
				Value: 33,
			},
		},
	}

	p := NewIndexValueDispatcher("index2")
	_, _, err := p.DispatchRowChangedEvent(event, 16)
	require.ErrorIs(t, err, errors.ErrDispatcherFailed)

	p = NewIndexValueDispatcher("index1")
	index, _, err := p.DispatchRowChangedEvent(event, 16)
	require.NoError(t, err)
	require.Equal(t, int32(5), index)

	idx := index
	p = NewIndexValueDispatcher("INDEX1")
	index, _, err = p.DispatchRowChangedEvent(event, 16)
	require.NoError(t, err)
	require.Equal(t, idx, index)

	p = NewIndexValueDispatcher("")
	index, _, err = p.DispatchRowChangedEvent(event, 16)
	require.NoError(t, err)
	require.Equal(t, idx, index)
}
