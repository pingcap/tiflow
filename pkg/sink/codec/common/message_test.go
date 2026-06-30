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

package common

import (
	"testing"

	timodel "github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestCreate(t *testing.T) {
	t.Parallel()
	tableInfo := model.BuildTableInfo("test", "t1", []*model.Column{
		{
			Name: "a",
			Flag: model.HandleKeyFlag | model.PrimaryKeyFlag,
		}, {
			Name: "b",
			Flag: 0,
		},
	}, [][]int{{0}})
	rowEvent := &model.RowChangedEvent{
		TableInfo: tableInfo,
		PreColumns: model.Columns2ColumnDatas([]*model.Column{
			{
				Name:  "a",
				Value: 1,
			}, {
				Name:  "b",
				Value: 2,
			},
		}, tableInfo),
		StartTs:  1234,
		CommitTs: 5678,
	}

	msg := NewMsg(config.ProtocolOpen,
		[]byte("key1"),
		[]byte("value1"),
		rowEvent.CommitTs,
		model.MessageTypeRow,
		&rowEvent.TableInfo.TableName.Schema,
		&rowEvent.TableInfo.TableName.Table)

	require.Equal(t, []byte("key1"), msg.Key)
	require.Equal(t, []byte("value1"), msg.Value)
	require.Equal(t, rowEvent.CommitTs, msg.Ts)
	require.Equal(t, model.MessageTypeRow, msg.Type)
	require.Equal(t, rowEvent.TableInfo.GetSchemaName(), *msg.Schema)
	require.Equal(t, rowEvent.TableInfo.GetTableName(), *msg.Table)
	require.Equal(t, config.ProtocolOpen, msg.Protocol)

	ft := types.NewFieldType(0)
	ft.SetFlag(mysql.PriKeyFlag)
	job := &timodel.Job{
		ID:         1071,
		TableID:    49,
		SchemaName: "test",
		Type:       timodel.ActionAddColumn,
		StartTS:    420536581131337731,
		Query:      "alter table t1 add column a int",
		BinlogInfo: &timodel.HistoryInfo{
			TableInfo: &timodel.TableInfo{
				ID:   49,
				Name: pmodel.CIStr{O: "t1"},
				Columns: []*timodel.ColumnInfo{
					{ID: 1, Name: pmodel.CIStr{O: "id"}, FieldType: *ft, State: timodel.StatePublic},
					{ID: 2, Name: pmodel.CIStr{O: "a"}, FieldType: types.FieldType{}, State: timodel.StatePublic},
				},
			},
			FinishedTS: 420536581196873729,
		},
	}
	preTableInfo := &model.TableInfo{
		TableName: model.TableName{
			Schema:  "test",
			Table:   "t1",
			TableID: 49,
		},
		TableInfo: &timodel.TableInfo{
			ID:   49,
			Name: pmodel.CIStr{O: "t1"},
			Columns: []*timodel.ColumnInfo{
				{ID: 1, Name: pmodel.CIStr{O: "id"}, FieldType: *ft, State: timodel.StatePublic},
			},
		},
	}
	tableInfo2 := model.WrapTableInfo(job.SchemaID, job.SchemaName, job.BinlogInfo.FinishedTS, job.BinlogInfo.TableInfo)
	ddlEvent := &model.DDLEvent{}
	ddlEvent.FromJob(job, preTableInfo, tableInfo2)

	msg = NewDDLMsg(config.ProtocolMaxwell, nil, []byte("value1"), ddlEvent)
	require.Nil(t, msg.Key)
	require.Equal(t, []byte("value1"), msg.Value)
	require.Equal(t, ddlEvent.CommitTs, msg.Ts)
	require.Equal(t, model.MessageTypeDDL, msg.Type)
	require.Equal(t, ddlEvent.TableInfo.TableName.Schema, *msg.Schema)
	require.Equal(t, ddlEvent.TableInfo.TableName.Table, *msg.Table)
	require.Equal(t, config.ProtocolMaxwell, msg.Protocol)

	msg = NewResolvedMsg(config.ProtocolCanal, []byte("key1"), nil, 1234)
	require.Equal(t, []byte("key1"), msg.Key)
	require.Nil(t, msg.Value)
	require.Equal(t, uint64(1234), msg.Ts)
	require.Equal(t, model.MessageTypeResolved, msg.Type)
	require.Nil(t, msg.Schema)
	require.Nil(t, msg.Table)
	require.Equal(t, config.ProtocolCanal, msg.Protocol)
}
