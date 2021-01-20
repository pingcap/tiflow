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
	"bytes"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"strings"
	"testing"

	"github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/util/testleak"
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

func (s *batchSuite) TestMaxMessageBytes(c *check.C) {
	defer testleak.AfterTest(c)()
	encoder := NewJSONEventBatchEncoder()
	err := encoder.SetParams(map[string]string{"max-message-bytes": "256"})
	c.Check(err, check.IsNil)

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
	for _, msg := range messages {
		c.Assert(msg.Length(), check.LessEqual, 256)
	}
}

func (s *batchSuite) TestMaxBatchSize(c *check.C) {
	defer testleak.AfterTest(c)()
	encoder := NewJSONEventBatchEncoder()
	err := encoder.SetParams(map[string]string{"max-batch-size": "64"})
	c.Check(err, check.IsNil)

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

func (s *columnSuite) TestBuildTestCaseSet(c *check.C) {
	generateTestCase(c, "simple", func(encoder EventBatchEncoder) {
		rows := []*model.RowChangedEvent{{
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
		}}
		for _, row := range rows {
			_, err := encoder.AppendRowChangedEvent(row)
			c.Assert(err, check.IsNil)
		}
	})
	generateString := func(n int) string {
		var sb strings.Builder
		for i := 0; i < n; i++ {
			sb.WriteRune('8')
		}
		return sb.String()
	}
	generateTestCase(c, "lang_event", func(encoder EventBatchEncoder) {
		for i := 0; i < 1024; i++ {
			_, err := encoder.AppendRowChangedEvent(&model.RowChangedEvent{
				CommitTs: 1,
				Table:    &model.TableName{Schema: "a", Table: generateString(i)},
				Columns:  []*model.Column{{Name: "col1", Type: 1, Value: generateString(i)}},
			})
			c.Assert(err, check.IsNil)
		}
	})
	generateTestCase(c, "expand_char", func(encoder EventBatchEncoder) {
		rows := []*model.RowChangedEvent{{
			CommitTs: 1,
			Table:    &model.TableName{Schema: "a", Table: "b"},
			Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "你好《》？：「」|+——￥"}},
		}, {
			CommitTs: 2,
			Table:    &model.TableName{Schema: "a", Table: "b"},
			Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "💋💼🕶💼👛💄💋💇"}},
		}, {
			CommitTs: 3,
			Table:    &model.TableName{Schema: "a", Table: "b"},
			Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "にほんご"}},
		}, {
			CommitTs: 4,
			Table:    &model.TableName{Schema: "a", Table: "b"},
			Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "~!@#$%^&*()_+\"'[]\\;',./|:>?`"}},
		}, {
			CommitTs: 5,
			Table:    &model.TableName{Schema: "a", Table: "b"},
			Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "Ω≈ç√∫µ≤≥µæ…¬˚∆˙©ƒ∂ßåœ∑®†¥øπ“‘«≠–ºª•¶§∞¢£™¡"}},
		}}
		for _, row := range rows {
			_, err := encoder.AppendRowChangedEvent(row)
			c.Assert(err, check.IsNil)
		}
	})

}

func generateTestCase(c *check.C, name string, fn func(encoder EventBatchEncoder)) {
	encoder := NewJSONEventBatchEncoder()
	err := encoder.SetParams(map[string]string{
		"max-message-bytes": fmt.Sprintf("%d", math.MaxInt64),
		"max-batch-size":    fmt.Sprintf("%d", math.MaxInt64),
	})
	c.Assert(err, check.IsNil)
	fn(encoder)
	mqs := encoder.Build()
	c.Assert(len(mqs), check.Equals, 1)
	mq := mqs[0]
	os.RemoveAll("./case_set/" + name)
	err = os.MkdirAll("./case_set/"+name, 0755)
	c.Assert(err, check.IsNil)
	err = ioutil.WriteFile("./case_set/"+name+"/key.bin", mq.Key, 0644)
	c.Assert(err, check.IsNil)
	err = ioutil.WriteFile("./case_set/"+name+"/value.bin", mq.Value, 0644)
	c.Assert(err, check.IsNil)
	generateExpected(c, name, mq.Key, mq.Value)
}

func generateExpected(c *check.C, name string, key, value []byte) {
	version := binary.BigEndian.Uint64(key[:8])
	c.Assert(version, check.Equals, uint64(1))
	key = key[8:]
	var bb bytes.Buffer
	for {
		if len(key) == 0 {
			c.Assert(len(value), check.Equals, 0)
			break
		}
		keyLength := binary.BigEndian.Uint64(key[:8])
		valueLength := binary.BigEndian.Uint64(value[:8])
		key = key[8:]
		value = value[8:]
		bb.Write(key[:keyLength])
		bb.WriteRune('\n')
		bb.Write(value[:valueLength])
		bb.WriteRune('\n')
		bb.WriteRune('\n')
		key = key[keyLength:]
		value = value[valueLength:]
	}
	err := ioutil.WriteFile("./case_set/"+name+"/expected.txt", bb.Bytes(), 0644)
	c.Assert(err, check.IsNil)
}
