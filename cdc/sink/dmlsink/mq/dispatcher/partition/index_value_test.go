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

	timodel "github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestIndexValueDispatcher(t *testing.T) {
	t.Parallel()

	tableInfoWithSinglePK := model.BuildTableInfo("test", "t1", []*model.Column{
		{
			Name: "a",
			Flag: model.HandleKeyFlag | model.PrimaryKeyFlag,
		}, {
			Name: "b",
		},
	}, [][]int{{0}})

	tableInfoWithCompositePK := model.BuildTableInfo("test", "t2", []*model.Column{
		{
			Name: "a",
			Flag: model.HandleKeyFlag | model.PrimaryKeyFlag,
		}, {
			Name: "b",
			Flag: model.HandleKeyFlag | model.PrimaryKeyFlag,
		},
	}, [][]int{{0, 1}})
	testCases := []struct {
		row             *model.RowChangedEvent
		expectPartition int32
	}{
		{row: &model.RowChangedEvent{
			TableInfo: tableInfoWithSinglePK,
			Columns: model.Columns2ColumnDatas([]*model.Column{
				{
					Name:  "a",
					Value: 11,
				}, {
					Name:  "b",
					Value: 22,
				},
			}, tableInfoWithSinglePK),
		}, expectPartition: 2},
		{row: &model.RowChangedEvent{
			TableInfo: tableInfoWithSinglePK,
			Columns: model.Columns2ColumnDatas([]*model.Column{
				{
					Name:  "a",
					Value: 22,
				}, {
					Name:  "b",
					Value: 22,
				},
			}, tableInfoWithSinglePK),
		}, expectPartition: 11},
		{row: &model.RowChangedEvent{
			TableInfo: tableInfoWithSinglePK,
			Columns: model.Columns2ColumnDatas([]*model.Column{
				{
					Name:  "a",
					Value: 11,
				}, {
					Name:  "b",
					Value: 33,
				},
			}, tableInfoWithSinglePK),
		}, expectPartition: 2},
		{row: &model.RowChangedEvent{
			TableInfo: tableInfoWithCompositePK,
			Columns: model.Columns2ColumnDatas([]*model.Column{
				{
					Name:  "a",
					Value: 11,
				}, {
					Name:  "b",
					Value: 22,
				},
			}, tableInfoWithCompositePK),
		}, expectPartition: 5},
		{row: &model.RowChangedEvent{
			TableInfo: tableInfoWithCompositePK,
			Columns: model.Columns2ColumnDatas([]*model.Column{
				{
					Name:  "b",
					Value: 22,
				}, {
					Name:  "a",
					Value: 11,
				},
			}, tableInfoWithCompositePK),
		}, expectPartition: 5},
		{row: &model.RowChangedEvent{
			TableInfo: tableInfoWithCompositePK,
			Columns: model.Columns2ColumnDatas([]*model.Column{
				{
					Name:  "a",
					Value: 11,
				}, {
					Name:  "b",
					Value: 0,
				},
			}, tableInfoWithCompositePK),
		}, expectPartition: 14},
		{row: &model.RowChangedEvent{
			TableInfo: tableInfoWithCompositePK,
			Columns: model.Columns2ColumnDatas([]*model.Column{
				{
					Name:  "a",
					Value: 11,
				}, {
					Name:  "b",
					Value: 33,
				},
			}, tableInfoWithCompositePK),
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

	tidbTableInfo := &timodel.TableInfo{
		ID:   100,
		Name: pmodel.NewCIStr("t1"),
		Columns: []*timodel.ColumnInfo{
			{ID: 1, Name: pmodel.NewCIStr("a"), FieldType: *types.NewFieldType(mysql.TypeLong)},
		},
		Indices: []*timodel.IndexInfo{
			{
				Name: pmodel.CIStr{
					O: "index1",
				},
				Columns: []*timodel.IndexColumn{
					{
						Name: pmodel.CIStr{
							O: "a",
						},
					},
				},
			},
		},
	}
	tableInfo := model.WrapTableInfo(100, "test", 33, tidbTableInfo)

	event := &model.RowChangedEvent{
		TableInfo: tableInfo,
		Columns: []*model.ColumnData{
			{ColumnID: 1, Value: 11},
		},
	}

	p := NewIndexValueDispatcher("index2")
	_, _, err := p.DispatchRowChangedEvent(event, 16)
	require.ErrorIs(t, err, errors.ErrDispatcherFailed)

	p = NewIndexValueDispatcher("index1")
	index, _, err := p.DispatchRowChangedEvent(event, 16)
	require.NoError(t, err)
	require.Equal(t, int32(2), index)
}
