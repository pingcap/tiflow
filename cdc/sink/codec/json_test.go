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
	"testing"

	"github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/ticdc/cdc/model"
)

func Test(t *testing.T) { check.TestingT(t) }

type batchSuite struct {
	rowCases        [][]*model.RowChangedEvent
	ddlCases        [][]*model.DDLEvent
	resolvedTsCases [][]uint64
}

var _ = check.Suite(&batchSuite{
	rowCases: [][]*model.RowChangedEvent{{{
		CommitTs: 1,
		Table:    &model.TableName{Schema: "a", Table: "b"},
		Columns:  map[string]*model.Column{"col1": {Type: 1, Value: "aa"}},
	}}, {{
		CommitTs: 1,
		Table:    &model.TableName{Schema: "a", Table: "b"},
		Columns:  map[string]*model.Column{"col1": {Type: 1, Value: "aa"}},
	}, {
		CommitTs: 2,
		Table:    &model.TableName{Schema: "a", Table: "b"},
		Columns:  map[string]*model.Column{"col1": {Type: 1, Value: "bb"}},
	}, {
		CommitTs: 3,
		Table:    &model.TableName{Schema: "a", Table: "b"},
		Columns:  map[string]*model.Column{"col1": {Type: 1, Value: "bb"}},
	}, {
		CommitTs: 4,
		Table:    &model.TableName{Schema: "a", Table: "c", Partition: 6},
		Columns:  map[string]*model.Column{"col1": {Type: 1, Value: "cc"}},
	}}, {}},
	ddlCases: [][]*model.DDLEvent{{{
		CommitTs: 1,
		Schema:   "a",
		Table:    "b",
		Query:    "create table a",
		Type:     1,
	}}, {{
		CommitTs: 1,
		Schema:   "a",
		Table:    "b",
		Query:    "create table a",
		Type:     1,
	}, {
		CommitTs: 2,
		Schema:   "a",
		Table:    "b",
		Query:    "create table b",
		Type:     2,
	}, {
		CommitTs: 3,
		Schema:   "a",
		Table:    "b",
		Query:    "create table c",
		Type:     3,
	}}, {}},
	resolvedTsCases: [][]uint64{{1}, {1, 2, 3}, {}},
})

func (s *batchSuite) testBatchCodec(c *check.C, newEncoder func() EventBatchEncoder, newDecoder func(key []byte, value []byte) (EventBatchDecoder, error)) {
	for _, cs := range s.rowCases {
		encoder := newEncoder()
		for _, row := range cs {
			err := encoder.AppendRowChangedEvent(row)
			c.Assert(err, check.IsNil)
		}
		key, value := encoder.Build()
		c.Assert(len(key)+len(value), check.Equals, encoder.Size())
		decoder, err := newDecoder(key, value)
		c.Assert(err, check.IsNil)
		index := 0
		for {
			tp, hasNext, err := decoder.HasNext()
			c.Assert(err, check.IsNil)
			if !hasNext {
				break
			}
			c.Assert(tp, check.Equals, model.MqMessageTypeRow)
			row, err := decoder.NextRowChangedEvent()
			c.Assert(err, check.IsNil)
			c.Assert(row, check.DeepEquals, cs[index])
			index++
		}
	}

	for _, cs := range s.ddlCases {
		encoder := newEncoder()
		for _, ddl := range cs {
			err := encoder.AppendDDLEvent(ddl)
			c.Assert(err, check.IsNil)
		}
		key, value := encoder.Build()
		c.Assert(len(key)+len(value), check.Equals, encoder.Size())
		decoder, err := newDecoder(key, value)
		c.Assert(err, check.IsNil)
		index := 0
		for {
			tp, hasNext, err := decoder.HasNext()
			c.Assert(err, check.IsNil)
			if !hasNext {
				break
			}
			c.Assert(tp, check.Equals, model.MqMessageTypeDDL)
			ddl, err := decoder.NextDDLEvent()
			c.Assert(err, check.IsNil)
			c.Assert(ddl, check.DeepEquals, cs[index])
			index++
		}
	}

	for _, cs := range s.resolvedTsCases {
		encoder := newEncoder()
		for _, ts := range cs {
			err := encoder.AppendResolvedEvent(ts)
			c.Assert(err, check.IsNil)
		}
		key, value := encoder.Build()
		c.Assert(len(key)+len(value), check.Equals, encoder.Size())
		decoder, err := newDecoder(key, value)
		c.Assert(err, check.IsNil)
		index := 0
		for {
			tp, hasNext, err := decoder.HasNext()
			c.Assert(err, check.IsNil)
			if !hasNext {
				break
			}
			c.Assert(tp, check.Equals, model.MqMessageTypeResolved)
			ts, err := decoder.NextResolvedEvent()
			c.Assert(err, check.IsNil)
			c.Assert(ts, check.DeepEquals, cs[index])
			index++
		}
	}
}

func (s *batchSuite) TestDefaultEventBatchCodec(c *check.C) {
	s.testBatchCodec(c, NewJSONEventBatchEncoder, NewJSONEventBatchDecoder)
}

var _ = check.Suite(&columnSuite{})

type columnSuite struct{}

func (s *columnSuite) TestFormatCol(c *check.C) {
	row := &mqMessageRow{Update: map[string]*column{"test": {
		Type:  mysql.TypeString,
		Value: "测",
	}}}
	rowEncode, err := row.Encode()
	c.Assert(err, check.IsNil)
	row2 := new(mqMessageRow)
	err = row2.Decode(rowEncode)
	c.Assert(err, check.IsNil)
	c.Assert(row2, check.DeepEquals, row)

	row = &mqMessageRow{Update: map[string]*column{"test": {
		Type:  mysql.TypeBlob,
		Value: []byte("测"),
	}}}
	rowEncode, err = row.Encode()
	c.Assert(err, check.IsNil)
	row2 = new(mqMessageRow)
	err = row2.Decode(rowEncode)
	c.Assert(err, check.IsNil)
	c.Assert(row2, check.DeepEquals, row)
}
