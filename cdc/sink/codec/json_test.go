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
	"math"
	"strconv"
	"testing"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/util/testleak"
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
		err := encoder.SetParams(map[string]string{"max-message-bytes": "8192", "max-batch-size": "64"})
		c.Assert(err, check.IsNil)

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
			res := encoder.Build()
			c.Assert(res, check.HasLen, 1)
			c.Assert(res[0].GetRowsCount(), check.Equals, len(cs))
			decoder, err := newDecoder(res[0].Key, res[0].Value)
			c.Assert(err, check.IsNil)
			checkRowDecoder(decoder, cs)
		}
	}

	for _, cs := range s.ddlCases {
		encoder := newEncoder()
		mixedEncoder := newEncoder()
		err := encoder.SetParams(map[string]string{"max-message-bytes": "8192", "max-batch-size": "64"})
		c.Assert(err, check.IsNil)

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
		err := encoder.SetParams(map[string]string{"max-message-bytes": "8192", "max-batch-size": "64"})
		c.Assert(err, check.IsNil)

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

func (s *batchSuite) TestParamsEdgeCases(c *check.C) {
	defer testleak.AfterTest(c)()
	encoder := NewJSONEventBatchEncoder().(*JSONEventBatchEncoder)
	err := encoder.SetParams(map[string]string{"max-message-bytes": "10485760"})
	c.Assert(err, check.IsNil)
	c.Assert(encoder.maxBatchSize, check.Equals, DefaultMaxBatchSize)
	c.Assert(encoder.maxMessageBytes, check.Equals, config.DefaultMaxMessageBytes)

	err = encoder.SetParams(map[string]string{"max-message-bytes": "0"})
	c.Assert(err, check.ErrorMatches, ".*invalid.*")

	err = encoder.SetParams(map[string]string{"max-message-bytes": "-1"})
	c.Assert(err, check.ErrorMatches, ".*invalid.*")

	err = encoder.SetParams(map[string]string{"max-message-bytes": strconv.Itoa(math.MaxInt32)})
	c.Assert(err, check.IsNil)
	c.Assert(encoder.maxBatchSize, check.Equals, DefaultMaxBatchSize)
	c.Assert(encoder.maxMessageBytes, check.Equals, math.MaxInt32)

	err = encoder.SetParams(map[string]string{"max-message-bytes": strconv.Itoa(math.MaxUint32)})
	c.Assert(err, check.IsNil)
	c.Assert(encoder.maxBatchSize, check.Equals, DefaultMaxBatchSize)
	c.Assert(encoder.maxMessageBytes, check.Equals, math.MaxUint32)

	err = encoder.SetParams(map[string]string{"max-message-bytes": "10485760", "max-batch-size": "0"})
	c.Assert(err, check.ErrorMatches, ".*invalid.*")

	err = encoder.SetParams(map[string]string{"max-message-bytes": "10485760", "max-batch-size": "-1"})
	c.Assert(err, check.ErrorMatches, ".*invalid.*")

	err = encoder.SetParams(map[string]string{"max-message-bytes": "10485760", "max-batch-size": strconv.Itoa(math.MaxInt32)})
	c.Assert(err, check.IsNil)
	c.Assert(encoder.maxBatchSize, check.Equals, math.MaxInt32)
	c.Assert(encoder.maxMessageBytes, check.Equals, config.DefaultMaxMessageBytes)

	err = encoder.SetParams(map[string]string{"max-message-bytes": "10485760", "max-batch-size": strconv.Itoa(math.MaxUint32)})
	c.Assert(err, check.IsNil)
	c.Assert(encoder.maxBatchSize, check.Equals, math.MaxUint32)
	c.Assert(encoder.maxMessageBytes, check.Equals, config.DefaultMaxMessageBytes)
}

func (s *batchSuite) TestSetParams(c *check.C) {
	defer testleak.AfterTest(c)

	opts := make(map[string]string)
	encoder := NewJSONEventBatchEncoder()
	err := encoder.SetParams(opts)
	c.Assert(
		errors.Cause(err),
		check.ErrorMatches,
		".*max-message-bytes not found.*",
	)

	opts["max-message-bytes"] = "1"
	encoder = NewJSONEventBatchEncoder()
	err = encoder.SetParams(opts)
	c.Assert(err, check.IsNil)
	c.Assert(encoder, check.NotNil)
	jsonEncoder, ok := encoder.(*JSONEventBatchEncoder)
	c.Assert(ok, check.IsTrue)
	c.Assert(jsonEncoder.GetMaxMessageBytes(), check.Equals, 1)
}

func (s *batchSuite) TestMaxMessageBytes(c *check.C) {
	defer testleak.AfterTest(c)()
	encoder := NewJSONEventBatchEncoder()

	// the size of `testEvent` is 87
	testEvent := &model.RowChangedEvent{
		CommitTs: 1,
		Table:    &model.TableName{Schema: "a", Table: "b"},
		Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "aa"}},
	}

	// for a single message, the overhead is 36(maximumRecordOverhead) + 8(versionHea) = 44, just can hold it.
	a := strconv.Itoa(87 + 44)
	err := encoder.SetParams(map[string]string{"max-message-bytes": a})
	c.Check(err, check.IsNil)
	r, err := encoder.AppendRowChangedEvent(testEvent)
	c.Check(err, check.IsNil)
	c.Check(r, check.Equals, EncoderNoOperation)

	a = strconv.Itoa(87 + 43)
	err = encoder.SetParams(map[string]string{"max-message-bytes": a})
	c.Assert(err, check.IsNil)
	r, err = encoder.AppendRowChangedEvent(testEvent)
	c.Check(err, check.NotNil)
	c.Check(r, check.Equals, EncoderNoOperation)

	// make sure each batch's `Length` not greater than `max-message-bytes`
	err = encoder.SetParams(map[string]string{"max-message-bytes": "256"})
	c.Check(err, check.IsNil)

	for i := 0; i < 10000; i++ {
		r, err := encoder.AppendRowChangedEvent(testEvent)
		c.Check(r, check.Equals, EncoderNoOperation)
		c.Check(err, check.IsNil)
	}

	messages := encoder.Build()
	for _, msg := range messages {
		c.Assert(msg.Length(), check.LessEqual, 256)
	}
}

func (s *batchSuite) TestMaxBatchSize(c *check.C) {
	defer testleak.AfterTest(c)()
	encoder := NewJSONEventBatchEncoder()
	err := encoder.SetParams(map[string]string{"max-message-bytes": "1048576", "max-batch-size": "64"})
	c.Assert(encoder, check.NotNil)
	c.Assert(err, check.IsNil)

	jsonEncoder, ok := encoder.(*JSONEventBatchEncoder)
	c.Assert(ok, check.IsTrue)
	c.Assert(jsonEncoder.GetMaxMessageBytes(), check.Equals, 1048576)

	testEvent := &model.RowChangedEvent{
		CommitTs: 1,
		Table:    &model.TableName{Schema: "a", Table: "b"},
		Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "aa"}},
	}

	for i := 0; i < 10000; i++ {
		r, err := encoder.AppendRowChangedEvent(testEvent)
		c.Check(r, check.Equals, EncoderNoOperation)
		c.Check(err, check.IsNil)
	}

	messages := encoder.Build()
	sum := 0
	for _, msg := range messages {
		decoder, err := NewJSONEventBatchDecoder(msg.Key, msg.Value)
		c.Check(err, check.IsNil)
		count := 0
		for {
			t, hasNext, err := decoder.HasNext()
			c.Check(err, check.IsNil)
			if !hasNext {
				break
			}

			c.Check(t, check.Equals, model.MqMessageTypeRow)
			_, err = decoder.NextRowChangedEvent()
			c.Check(err, check.IsNil)
			count++
		}
		c.Check(count, check.LessEqual, 64)
		sum += count
	}
	c.Check(sum, check.Equals, 10000)
}

func (s *batchSuite) TestDefaultEventBatchCodec(c *check.C) {
	defer testleak.AfterTest(c)()
	s.testBatchCodec(c, func() EventBatchEncoder {
		encoder := NewJSONEventBatchEncoder()
		return encoder
	}, NewJSONEventBatchDecoder)
}

var _ = check.Suite(&columnSuite{})

type columnSuite struct{}

func (s *columnSuite) TestFormatCol(c *check.C) {
	defer testleak.AfterTest(c)()
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

func (s *columnSuite) TestNonBinaryStringCol(c *check.C) {
	defer testleak.AfterTest(c)()
	col := &model.Column{
		Name:  "test",
		Type:  mysql.TypeString,
		Value: "value",
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
	col2.Value = string(col2.Value.([]byte))
	c.Assert(col2, check.DeepEquals, col)
}

func (s *columnSuite) TestVarBinaryCol(c *check.C) {
	defer testleak.AfterTest(c)()
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
