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

func TestIndexValueDispatcherxx(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	_ = helper.DDL2Event("create database airbnbsim")
	ddlEvent := helper.DDL2Event("create table airbnbsim.t2(`pk` varchar(32) not null, `sk` varchar(32) not null, `ts` timestamp(6) not null, v varchar(10240) default null, primary key (`pk`, `sk`, `ts`))")

	event := helper.DML2Event(`insert into airbnbsim.t2 values ("76813043", "h9_21", "2024-05-20 21:59:18.706826", "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")`, "airbnbsim", "t2")

	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolSimple)
	codecConfig.EncodingFormat = common.EncodingFormatAvro
	builder, err := simple.NewBuilder(ctx, codecConfig)
	require.NoError(t, err)

	enc := builder.Build()

	dec, err := simple.NewDecoder(ctx, codecConfig, nil)
	require.NoError(t, err)

	m, err := enc.EncodeDDLEvent(ddlEvent)
	require.NoError(t, err)

	err = dec.AddKeyValue(m.Key, m.Value)
	require.NoError(t, err)

	messageType, hasNext, err := dec.HasNext()
	require.NoError(t, err)
	require.True(t, hasNext)
	require.Equal(t, model.MessageTypeDDL, messageType)

	decodedDDL, err := dec.NextDDLEvent()
	require.NoError(t, err)
	require.NotNil(t, decodedDDL)

	err = enc.AppendRowChangedEvent(ctx, "", event, func() {})
	require.NoError(t, err)

	m = enc.Build()[0]
	err = dec.AddKeyValue(m.Key, m.Value)
	require.NoError(t, err)

	messageType, hasNext, err = dec.HasNext()
	require.NoError(t, err)
	require.True(t, hasNext)
	require.Equal(t, model.MessageTypeRow, messageType)

	decoded, err := dec.NextRowChangedEvent()
	require.NoError(t, err)

	p := NewIndexValueDispatcher("")

	origin, _, err := p.DispatchRowChangedEvent(event, 3)
	require.NoError(t, err)

	target, _, err := p.DispatchRowChangedEvent(decoded, 3)
	require.NoError(t, err)
	require.Equal(t, origin, target)
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
