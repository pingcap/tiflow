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

<<<<<<< HEAD
	event := &model.RowChangedEvent{
		Table: &model.TableName{
			Schema: "test",
			Table:  "t1",
		},
		TableInfo: &model.TableInfo{
			TableInfo: &timodel.TableInfo{
				Indices: []*timodel.IndexInfo{
					{
						Name: timodel.CIStr{
							O: "index1",
						},
						Columns: []*timodel.IndexColumn{
							{
								Name: timodel.CIStr{
									O: "a",
								},
							},
						},
=======
	tidbTableInfo := &timodel.TableInfo{
		ID:         100,
		Name:       pmodel.NewCIStr("t1"),
		PKIsHandle: true,
		Columns: []*timodel.ColumnInfo{
			{ID: 1, Name: pmodel.NewCIStr("A"), FieldType: *types.NewFieldType(mysql.TypeLong)},
		},
		Indices: []*timodel.IndexInfo{
			{
				Primary: true,
				Name:    pmodel.NewCIStr("index1"),
				Columns: []*timodel.IndexColumn{
					{
						Name: pmodel.NewCIStr("A"),
>>>>>>> 0b2636a5ad (sink(ticdc): Treat column and index names as case insensitive (#12132))
					},
				},
			},
		},
		Columns: []*model.Column{
			{
				Name:  "a",
				Value: 11,
			},
		},
	}

	p := NewIndexValueDispatcher("index2")
	_, _, err := p.DispatchRowChangedEvent(event, 16)
	require.ErrorIs(t, err, errors.ErrDispatcherFailed)

	p = NewIndexValueDispatcher("index1")
	index, _, err := p.DispatchRowChangedEvent(event, 16)
	require.NoError(t, err)
	require.Equal(t, int32(2), index)

	p = NewIndexValueDispatcher("INDEX1")
	index, _, err = p.DispatchRowChangedEvent(event, 16)
	require.NoError(t, err)
	require.Equal(t, int32(2), index)

	p = NewIndexValueDispatcher("")
	index, _, err = p.DispatchRowChangedEvent(event, 3)
	require.NoError(t, err)
	require.Equal(t, int32(0), index)
}
