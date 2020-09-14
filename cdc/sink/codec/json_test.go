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
		Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "aa"}},
	}}, {{
		CommitTs: 1,
		Table:    &model.TableName{Schema: "a", Table: "b"},
		Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "aa"}},
	}, {
		CommitTs: 2,
		Table:    &model.TableName{Schema: "a", Table: "b"},
		Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "bb"}},
	}, {
		CommitTs: 3,
		Table:    &model.TableName{Schema: "a", Table: "b"},
		Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "bb"}},
	}, {
		CommitTs: 4,
		Table:    &model.TableName{Schema: "a", Table: "c", TableID: 6, IsPartition: true},
		Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "cc"}},
	}}, {}},
	ddlCases: [][]*model.DDLEvent{{{
		CommitTs: 1,
		TableInfo: &model.SimpleTableInfo{
			Schema: "a", Table: "b",
		},
		Query: "create table a",
		Type:  1,
	}}, {{
		CommitTs: 1,
		TableInfo: &model.SimpleTableInfo{
			Schema: "a", Table: "b",
		},
		Query: "create table a",
		Type:  1,
	}, {
		CommitTs: 2,
		TableInfo: &model.SimpleTableInfo{
			Schema: "a", Table: "b",
		},
		Query: "create table b",
		Type:  2,
	}, {
		CommitTs: 3,
		TableInfo: &model.SimpleTableInfo{
			Schema: "a", Table: "b",
		},
		Query: "create table c",
		Type:  3,
	}}, {}},
	resolvedTsCases: [][]uint64{{1}, {1, 2, 3}, {}},
})

func (s *batchSuite) testBatchCodec(c *check.C, newEncoder func() EventBatchEncoder, newDecoder func(key []byte, value []byte) (EventBatchDecoder, error)) {
	checkRowDecoder := func(decoder EventBatchDecoder, cs []*model.RowChangedEvent) {
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

	checkDDLDecoder := func(decoder EventBatchDecoder, cs []*model.DDLEvent) {
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

	checkTSDecoder := func(decoder EventBatchDecoder, cs []uint64) {
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

	for _, cs := range s.rowCases {
		encoder := newEncoder()
		mixedEncoder := newEncoder()
		mixedEncoder.(*JSONEventBatchEncoder).SetMixedBuildSupport(true)
		for _, row := range cs {
			_, err := encoder.AppendRowChangedEvent(row)
			c.Assert(err, check.IsNil)

			op, err := mixedEncoder.AppendRowChangedEvent(row)
			c.Assert(op, check.Equals, EncoderNoOperation)
			c.Assert(err, check.IsNil)
		}

		// test mixed decode
		mixed := mixedEncoder.MixedBuild(true)
		c.Assert(len(mixed), check.Equals, mixedEncoder.Size())
		mixedDecoder, err := newDecoder(mixed, nil)
		c.Assert(err, check.IsNil)
		checkRowDecoder(mixedDecoder, cs)

		// test normal decode
		if len(cs) > 0 {
			size := encoder.Size()
			res := encoder.Build()
			c.Assert(res, check.HasLen, 1)
			c.Assert(len(res[0].Key)+len(res[0].Value), check.Equals, size)
			decoder, err := newDecoder(res[0].Key, res[0].Value)
			c.Assert(err, check.IsNil)
			checkRowDecoder(decoder, cs)
		}
	}

	for _, cs := range s.ddlCases {
		encoder := newEncoder()
		mixedEncoder := newEncoder()
		mixedEncoder.(*JSONEventBatchEncoder).SetMixedBuildSupport(true)
		for i, ddl := range cs {
			msg, err := encoder.EncodeDDLEvent(ddl)
			c.Assert(err, check.IsNil)
			c.Assert(msg, check.NotNil)
			decoder, err := newDecoder(msg.Key, msg.Value)
			c.Assert(err, check.IsNil)
			checkDDLDecoder(decoder, cs[i:i+1])

			msg, err = mixedEncoder.EncodeDDLEvent(ddl)
			c.Assert(msg, check.IsNil)
			c.Assert(err, check.IsNil)
		}

		// test mixed encode
		mixed := mixedEncoder.MixedBuild(true)
		c.Assert(len(mixed), check.Equals, mixedEncoder.Size())
		mixedDecoder, err := newDecoder(mixed, nil)
		c.Assert(err, check.IsNil)
		checkDDLDecoder(mixedDecoder, cs)
	}

	for _, cs := range s.resolvedTsCases {
		encoder := newEncoder()
		mixedEncoder := newEncoder()
		mixedEncoder.(*JSONEventBatchEncoder).SetMixedBuildSupport(true)
		for i, ts := range cs {
			msg, err := encoder.EncodeCheckpointEvent(ts)
			c.Assert(err, check.IsNil)
			c.Assert(msg, check.NotNil)
			decoder, err := newDecoder(msg.Key, msg.Value)
			c.Assert(err, check.IsNil)
			checkTSDecoder(decoder, cs[i:i+1])

			msg, err = mixedEncoder.EncodeCheckpointEvent(ts)
			c.Assert(msg, check.IsNil)
			c.Assert(err, check.IsNil)
		}

		// test mixed encode
		mixed := mixedEncoder.MixedBuild(true)
		c.Assert(len(mixed), check.Equals, mixedEncoder.Size())
		mixedDecoder, err := newDecoder(mixed, nil)
		c.Assert(err, check.IsNil)
		checkTSDecoder(mixedDecoder, cs)
	}
}

func (s *batchSuite) TestDefaultEventBatchCodec(c *check.C) {
	s.testBatchCodec(c, func() EventBatchEncoder {
		encoder := NewJSONEventBatchEncoder()
		return encoder
	}, NewJSONEventBatchDecoder)
}

var _ = check.Suite(&columnSuite{})

type columnSuite struct{}

func (s *columnSuite) TestFormatCol(c *check.C) {
	row := &mqMessageRow{Update: map[string]column{"test": {
		Type:  mysql.TypeString,
		Value: "测",
	}}}
	rowEncode, err := row.Encode()
	c.Assert(err, check.IsNil)
	row2 := new(mqMessageRow)
	err = row2.Decode(rowEncode)
	c.Assert(err, check.IsNil)
	c.Assert(row2, check.DeepEquals, row)

	row = &mqMessageRow{Update: map[string]column{"test": {
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

func (s *columnSuite) TestVarBinaryCol(c *check.C) {
	col := &model.Column{
		Name:  "test",
		Type:  mysql.TypeString,
		Flag:  model.BinaryFlag,
		Value: []byte{0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A},
	}
	jsonCol := column{}
	jsonCol.FromSinkColumn(col)
	row := &mqMessageRow{Update: map[string]column{"test": jsonCol}}
	rowEncode, err := row.Encode()
	c.Assert(err, check.IsNil)
	row2 := new(mqMessageRow)
	err = row2.Decode(rowEncode)
	c.Assert(err, check.IsNil)
	c.Assert(row2, check.DeepEquals, row)
	jsonCol2 := row2.Update["test"]
	col2 := jsonCol2.ToSinkColumn("test")
	c.Assert(col2, check.DeepEquals, col)
}
