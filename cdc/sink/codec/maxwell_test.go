// Copyright 2020 PingCAP, Inc.
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
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/util/testleak"
)

type maxwellbatchSuite struct {
	rowCases [][]*model.RowChangedEvent
	ddlCases [][]*model.DDLEvent
}

var _ = check.Suite(&maxwellbatchSuite{
	rowCases: [][]*model.RowChangedEvent{{{
		CommitTs: 1,
		Table:    &model.TableName{Schema: "a", Table: "b"},
		Columns:  []*model.Column{{Name: "col1", Type: 3, Value: 10}},
	}}, {}},
	ddlCases: [][]*model.DDLEvent{{{
		CommitTs: 1,
		TableInfo: &model.SimpleTableInfo{
			Schema: "a", Table: "b",
		},
		Query: "create table a",
		Type:  1,
	}}},
})

func (s *maxwellbatchSuite) testmaxwellBatchCodec(c *check.C, newEncoder func() EventBatchEncoder) {
	for _, cs := range s.rowCases {
		encoder := newEncoder()
		for _, row := range cs {
			_, err := encoder.AppendRowChangedEvent(row)
			c.Assert(err, check.IsNil)
		}
		size := encoder.Size()
		messages := encoder.Build()
		if len(cs) == 0 {
			c.Assert(messages, check.IsNil)
			continue
		}
		c.Assert(messages, check.HasLen, 1)
		c.Assert(messages[0].GetRowsCount(), check.Equals, len(cs))
		c.Assert(len(messages[0].Key)+len(messages[0].Value), check.Equals, size)
	}

	for _, cs := range s.ddlCases {
		encoder := newEncoder()
		for _, ddl := range cs {
			msg, err := encoder.EncodeDDLEvent(ddl)
			c.Assert(err, check.IsNil)
			c.Assert(msg, check.NotNil)
		}

	}
}

func (s *maxwellbatchSuite) TestmaxwellEventBatchCodec(c *check.C) {
	defer testleak.AfterTest(c)()
	s.testmaxwellBatchCodec(c, NewMaxwellEventBatchEncoder)
}

var _ = check.Suite(&maxwellcolumnSuite{})

type maxwellcolumnSuite struct{}

func (s *maxwellcolumnSuite) TestMaxwellFormatCol(c *check.C) {
	defer testleak.AfterTest(c)()
	row := &maxwellMessage{
		Ts:       1,
		Database: "a",
		Table:    "b",
		Type:     "delete",
		Xid:      1,
		Xoffset:  1,
		Position: "",
		Gtid:     "",
		Data: map[string]interface{}{
			"id": "1",
		},
	}
	rowEncode, err := row.Encode()
	c.Assert(err, check.IsNil)
	c.Assert(rowEncode, check.NotNil)
}
