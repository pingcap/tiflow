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

package codec

import (
	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/types"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/util/testleak"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCreate(t *testing.T) {
	defer testleak.AfterTest(t)()
	rowEvent := &model.RowChangedEvent{
		Table: &model.TableName{
			Schema: "test",
			Table:  "t1",
		},
		PreColumns: []*model.Column{
			{
				Name:  "a",
				Value: 1,
				Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
			}, {
				Name:  "b",
				Value: 2,
				Flag:  0,
			},
		},
		StartTs:  1234,
		CommitTs: 5678,
	}

	msg := NewMQMessage(config.ProtocolOpen, []byte("key1"), []byte("value1"), rowEvent.CommitTs, model.MqMessageTypeRow, &rowEvent.Table.Schema, &rowEvent.Table.Table)
	require.Equal(t, msg.Key, []byte("key1"))
	require.Equal(t, msg.Value, []byte("value1"))
	require.Equal(t, msg.Ts, rowEvent.CommitTs)
	require.Equal(t, msg.Type, model.MqMessageTypeRow)
	require.Equal(t, *msg.Schema, rowEvent.Table.Schema)
	require.Equal(t, *msg.Table, rowEvent.Table.Table)
	require.Equal(t, msg.Protocol, config.ProtocolOpen)

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
				Name: timodel.CIStr{O: "t1"},
				Columns: []*timodel.ColumnInfo{
					{ID: 1, Name: timodel.CIStr{O: "id"}, FieldType: types.FieldType{Flag: mysql.PriKeyFlag}, State: timodel.StatePublic},
					{ID: 2, Name: timodel.CIStr{O: "a"}, FieldType: types.FieldType{}, State: timodel.StatePublic},
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
			Name: timodel.CIStr{O: "t1"},
			Columns: []*timodel.ColumnInfo{
				{ID: 1, Name: timodel.CIStr{O: "id"}, FieldType: types.FieldType{Flag: mysql.PriKeyFlag}, State: timodel.StatePublic},
			},
		},
	}
	ddlEvent := &model.DDLEvent{}
	ddlEvent.FromJob(job, preTableInfo)

	msg = newDDLMQMessage(config.ProtocolMaxwell, nil, []byte("value1"), ddlEvent)
	require.Nil(t, msg.Key)
	require.Equal(t, msg.Value, []byte("value1"))
	require.Equal(t, msg.Ts, ddlEvent.CommitTs)
	require.Equal(t, msg.Type, model.MqMessageTypeDDL)
	require.Equal(t, *msg.Schema, ddlEvent.TableInfo.Schema)
	require.Equal(t, *msg.Table, ddlEvent.TableInfo.Table)
	require.Equal(t, msg.Protocol, config.ProtocolMaxwell)

	msg = newResolvedMQMessage(config.ProtocolCanal, []byte("key1"), nil, 1234)
	require.Equal(t, msg.Key, []byte("key1"))
	require.Nil(t, msg.Value)
	require.Equal(t, msg.Ts, uint64(1234))
	require.Equal(t, msg.Type, model.MqMessageTypeResolved)
	require.Nil(t, msg.Schema)
	require.Nil(t, msg.Table)
	require.Equal(t, msg.Protocol, config.ProtocolCanal)
}
