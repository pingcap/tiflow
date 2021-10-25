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
	"math"
	"strconv"

	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/util/testleak"
	"github.com/pingcap/tidb/parser/mysql"
)

type craftBatchSuite struct {
	rowCases        [][]*model.RowChangedEvent
	ddlCases        [][]*model.DDLEvent
	resolvedTsCases [][]uint64
}

var _ = check.Suite(&craftBatchSuite{
	rowCases:        codecRowCases,
	ddlCases:        codecDDLCases,
	resolvedTsCases: codecResolvedTSCases,
})

func (s *craftBatchSuite) testBatchCodec(c *check.C, newEncoder func() EventBatchEncoder, newDecoder func(value []byte) (EventBatchDecoder, error)) {
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

		events := 0
		for _, row := range cs {
			op, err := encoder.AppendRowChangedEvent(row)
			events++
			c.Assert(err, check.IsNil)
			c.Assert(op, check.Equals, EncoderNoOperation)
		}
		// test normal decode
		if len(cs) > 0 {
			res := encoder.Build()
			c.Assert(res, check.HasLen, 1)
			decoder, err := newDecoder(res[0].Value)
			c.Assert(err, check.IsNil)
			checkRowDecoder(decoder, cs)
		}
	}

	for _, cs := range s.ddlCases {
		encoder := newEncoder()
		err := encoder.SetParams(map[string]string{"max-message-bytes": "8192", "max-batch-size": "64"})
		c.Assert(err, check.IsNil)

		for i, ddl := range cs {
			msg, err := encoder.EncodeDDLEvent(ddl)
			c.Assert(err, check.IsNil)
			c.Assert(msg, check.NotNil)
			decoder, err := newDecoder(msg.Value)
			c.Assert(err, check.IsNil)
			checkDDLDecoder(decoder, cs[i:i+1])
		}
	}

	for _, cs := range s.resolvedTsCases {
		encoder := newEncoder()
		err := encoder.SetParams(map[string]string{"max-message-bytes": "8192", "max-batch-size": "64"})
		c.Assert(err, check.IsNil)

		for i, ts := range cs {
			msg, err := encoder.EncodeCheckpointEvent(ts)
			c.Assert(err, check.IsNil)
			c.Assert(msg, check.NotNil)
			decoder, err := newDecoder(msg.Value)
			c.Assert(err, check.IsNil)
			checkTSDecoder(decoder, cs[i:i+1])
		}
	}
}

func (s *craftBatchSuite) TestParamsEdgeCases(c *check.C) {
	defer testleak.AfterTest(c)()
	encoder := NewCraftEventBatchEncoder().(*CraftEventBatchEncoder)
	err := encoder.SetParams(map[string]string{})
	c.Assert(err, check.IsNil)
	c.Assert(encoder.maxBatchSize, check.Equals, DefaultMaxBatchSize)
	c.Assert(encoder.maxMessageSize, check.Equals, 64*1024*1024)

	err = encoder.SetParams(map[string]string{"max-message-bytes": "0"})
	c.Assert(err, check.ErrorMatches, ".*invalid.*")

	err = encoder.SetParams(map[string]string{"max-message-bytes": "-1"})
	c.Assert(err, check.ErrorMatches, ".*invalid.*")

	err = encoder.SetParams(map[string]string{"max-message-bytes": strconv.Itoa(math.MaxInt32)})
	c.Assert(err, check.IsNil)
	c.Assert(encoder.maxBatchSize, check.Equals, DefaultMaxBatchSize)
	c.Assert(encoder.maxMessageSize, check.Equals, math.MaxInt32)

	err = encoder.SetParams(map[string]string{"max-message-bytes": strconv.Itoa(math.MaxUint32)})
	c.Assert(err, check.NotNil)

	err = encoder.SetParams(map[string]string{"max-batch-size": "0"})
	c.Assert(err, check.ErrorMatches, ".*invalid.*")

	err = encoder.SetParams(map[string]string{"max-batch-size": "-1"})
	c.Assert(err, check.ErrorMatches, ".*invalid.*")

	err = encoder.SetParams(map[string]string{"max-batch-size": strconv.Itoa(math.MaxUint16)})
	c.Assert(err, check.IsNil)
	c.Assert(encoder.maxBatchSize, check.Equals, int(math.MaxUint16))
	c.Assert(encoder.maxMessageSize, check.Equals, 64*1024*1024)

	err = encoder.SetParams(map[string]string{"max-batch-size": strconv.Itoa(math.MaxInt32)})
	c.Assert(err, check.NotNil)

	err = encoder.SetParams(map[string]string{"max-batch-size": strconv.Itoa(math.MaxUint32)})
	c.Assert(err, check.NotNil)
}

func (s *craftBatchSuite) TestMaxMessageBytes(c *check.C) {
	defer testleak.AfterTest(c)()
	encoder := NewCraftEventBatchEncoder()
	err := encoder.SetParams(map[string]string{"max-message-bytes": "256"})
	c.Check(err, check.IsNil)

	testEvent := &model.RowChangedEvent{
		CommitTs: 1,
		Table:    &model.TableName{Schema: "a", Table: "b"},
		Columns:  []*model.Column{{Name: "col1", Type: mysql.TypeVarchar, Value: []byte("aa")}},
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

func (s *craftBatchSuite) TestMaxBatchSize(c *check.C) {
	defer testleak.AfterTest(c)()
	encoder := NewCraftEventBatchEncoder()
	err := encoder.SetParams(map[string]string{"max-batch-size": "64"})
	c.Check(err, check.IsNil)

	testEvent := &model.RowChangedEvent{
		CommitTs: 1,
		Table:    &model.TableName{Schema: "a", Table: "b"},
		Columns:  []*model.Column{{Name: "col1", Type: mysql.TypeVarchar, Value: []byte("aa")}},
	}

	for i := 0; i < 10000; i++ {
		r, err := encoder.AppendRowChangedEvent(testEvent)
		c.Check(r, check.Equals, EncoderNoOperation)
		c.Check(err, check.IsNil)
	}

	messages := encoder.Build()
	sum := 0
	for _, msg := range messages {
		decoder, err := NewCraftEventBatchDecoder(msg.Value)
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

func (s *craftBatchSuite) TestDefaultEventBatchCodec(c *check.C) {
	defer testleak.AfterTest(c)()
	s.testBatchCodec(c, func() EventBatchEncoder {
		encoder := NewCraftEventBatchEncoder()
		return encoder
	}, NewCraftEventBatchDecoder)
}
