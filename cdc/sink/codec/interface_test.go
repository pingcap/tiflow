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
	"github.com/pingcap/check"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/types"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/util/testleak"
)

type codecInterfaceSuite struct {
}

var _ = check.Suite(&codecInterfaceSuite{})

func (s *codecInterfaceSuite) SetUpSuite(c *check.C) {
}

func (s *codecInterfaceSuite) TearDownSuite(c *check.C) {
}

func (s *codecInterfaceSuite) TestCreate(c *check.C) {
	defer testleak.AfterTest(c)()
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

	msg := NewMQMessage(ProtocolDefault, []byte("key1"), []byte("value1"), rowEvent.CommitTs, model.MqMessageTypeRow, &rowEvent.Table.Schema, &rowEvent.Table.Table)

	c.Assert(msg.Key, check.BytesEquals, []byte("key1"))
	c.Assert(msg.Value, check.BytesEquals, []byte("value1"))
	c.Assert(msg.Ts, check.Equals, rowEvent.CommitTs)
	c.Assert(msg.Type, check.Equals, model.MqMessageTypeRow)
	c.Assert(*msg.Schema, check.Equals, rowEvent.Table.Schema)
	c.Assert(*msg.Table, check.Equals, rowEvent.Table.Table)
	c.Assert(msg.Protocol, check.Equals, ProtocolDefault)

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

	msg = newDDLMQMessage(ProtocolMaxwell, nil, []byte("value1"), ddlEvent)
	c.Assert(msg.Key, check.IsNil)
	c.Assert(msg.Value, check.BytesEquals, []byte("value1"))
	c.Assert(msg.Ts, check.Equals, ddlEvent.CommitTs)
	c.Assert(msg.Type, check.Equals, model.MqMessageTypeDDL)
	c.Assert(*msg.Schema, check.Equals, ddlEvent.TableInfo.Schema)
	c.Assert(*msg.Table, check.Equals, ddlEvent.TableInfo.Table)
	c.Assert(msg.Protocol, check.Equals, ProtocolMaxwell)

	msg = newResolvedMQMessage(ProtocolCanal, []byte("key1"), nil, 1234)
	c.Assert(msg.Key, check.BytesEquals, []byte("key1"))
	c.Assert(msg.Value, check.IsNil)
	c.Assert(msg.Ts, check.Equals, uint64(1234))
	c.Assert(msg.Type, check.Equals, model.MqMessageTypeResolved)
	c.Assert(msg.Schema, check.IsNil)
	c.Assert(msg.Table, check.IsNil)
	c.Assert(msg.Protocol, check.Equals, ProtocolCanal)
}
