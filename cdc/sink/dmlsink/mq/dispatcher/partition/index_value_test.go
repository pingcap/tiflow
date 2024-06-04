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
	"context"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/sink/codec/simple"
	"testing"

	timodel "github.com/pingcap/tidb/pkg/parser/model"
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
		Name: timodel.NewCIStr("t1"),
		Columns: []*timodel.ColumnInfo{
			{ID: 1, Name: timodel.NewCIStr("a"), FieldType: *types.NewFieldType(mysql.TypeLong)},
		},
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

func TestIndexValueDispatcherXXX(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	createTableDDL := `CREATE TABLE test.sbtest3 (
	  id int(11) NOT NULL,
	  k int(11) NOT NULL DEFAULT '0',
	  PRIMARY KEY (id),
	  KEY k_3 (k)
	)`
	createTableDDLEvent := helper.DDL2Event(createTableDDL)

	insertDML := `insert into test.sbtest3 values (3670739, 5015099)`
	event := helper.DML2Event(insertDML, "test", "sbtest3")

	p := NewIndexValueDispatcher("")
	origin, _, err := p.DispatchRowChangedEvent(event, 4)
	require.NoError(t, err)
	require.Equal(t, origin, int32(2))

	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolSimple)
	codecConfig.EncodingFormat = common.EncodingFormatAvro

	builder, err := simple.NewBuilder(ctx, codecConfig)
	require.NoError(t, err)
	encoder := builder.Build()

	decoder, err := simple.NewDecoder(ctx, codecConfig, nil)
	require.NoError(t, err)

	m, err := encoder.EncodeDDLEvent(createTableDDLEvent)
	require.NoError(t, err)

	err = decoder.AddKeyValue(m.Key, m.Value)
	require.NoError(t, err)

	ty, hasNext, err := decoder.HasNext()
	require.NoError(t, err)
	require.True(t, hasNext)
	require.Equal(t, ty, model.MessageTypeDDL)

	decodedDDL, err := decoder.NextDDLEvent()
	require.NoError(t, err)

	originFlags := createTableDDLEvent.TableInfo.ColumnsFlag
	obtainedFlags := decodedDDL.TableInfo.ColumnsFlag

	for colID, expected := range originFlags {
		name := createTableDDLEvent.TableInfo.ForceGetColumnName(colID)
		actualID := decodedDDL.TableInfo.ForceGetColumnIDByName(name)
		actual := obtainedFlags[actualID]
		require.Equal(t, expected, actual)
	}

	err = encoder.AppendRowChangedEvent(ctx, "", event, func() {})
	require.NoError(t, err)

	m = encoder.Build()[0]

	err = decoder.AddKeyValue(m.Key, m.Value)
	require.NoError(t, err)

	ty, hasNext, err = decoder.HasNext()
	require.NoError(t, err)
	require.True(t, hasNext)
	require.Equal(t, ty, model.MessageTypeRow)

	decodedEvent, err := decoder.NextRowChangedEvent()
	require.NoError(t, err)

	obtained, _, err := p.DispatchRowChangedEvent(decodedEvent, 4)
	require.NoError(t, err)
	require.Equal(t, origin, obtained)
}
