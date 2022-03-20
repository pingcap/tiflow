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
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/util/timeutil"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/util/testleak"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"testing"
)

type craftBatchSuite struct {
	suite.Suite
	rowCases        [][]*model.RowChangedEvent
	ddlCases        [][]*model.DDLEvent
	resolvedTsCases [][]uint64
}

func TestCraftBatchSuite(t *testing.T) {
	suite.Run(t, &craftBatchSuite{
		rowCases:        codecRowCases,
		ddlCases:        codecDDLCases,
		resolvedTsCases: codecResolvedTSCases,
	})
}

func (s *craftBatchSuite) testBatchCodec(encoderBuilder EncoderBuilder, newDecoder func(value []byte) (EventBatchDecoder, error)) {
	checkRowDecoder := func(decoder EventBatchDecoder, cs []*model.RowChangedEvent) {
		index := 0
		for {
			tp, hasNext, err := decoder.HasNext()
			require.Nil(s.T(), err)
			if !hasNext {
				break
			}
			require.Equal(s.T(), tp, model.MqMessageTypeRow)
			row, err := decoder.NextRowChangedEvent()
			require.Nil(s.T(), err)
			require.Equal(s.T(), row, cs[index])
			index++
		}
	}
	checkDDLDecoder := func(decoder EventBatchDecoder, cs []*model.DDLEvent) {
		index := 0
		for {
			tp, hasNext, err := decoder.HasNext()
			require.Nil(s.T(), err)
			if !hasNext {
				break
			}
			require.Equal(s.T(), tp, model.MqMessageTypeDDL)
			ddl, err := decoder.NextDDLEvent()
			require.Nil(s.T(), err)
			require.Equal(s.T(), ddl, cs[index])
			index++
		}
	}
	checkTSDecoder := func(decoder EventBatchDecoder, cs []uint64) {
		index := 0
		for {
			tp, hasNext, err := decoder.HasNext()
			require.Nil(s.T(), err)
			if !hasNext {
				break
			}
			require.Equal(s.T(), tp, model.MqMessageTypeResolved)
			ts, err := decoder.NextResolvedEvent()
			require.Nil(s.T(), err)
			require.Equal(s.T(), ts, cs[index])
			index++
		}
	}

	encoder := encoderBuilder.Build()
	for _, cs := range s.rowCases {
		events := 0
		for _, row := range cs {
			err := encoder.AppendRowChangedEvent(row)
			events++
			require.Nil(s.T(), err)
		}
		// test normal decode
		if len(cs) > 0 {
			res := encoder.Build()
			require.Len(s.T(), res, 1)
			decoder, err := newDecoder(res[0].Value)
			require.Nil(s.T(), err)
			checkRowDecoder(decoder, cs)
		}
	}

	encoder = encoderBuilder.Build()
	for _, cs := range s.ddlCases {
		for i, ddl := range cs {
			msg, err := encoder.EncodeDDLEvent(ddl)
			require.Nil(s.T(), err)
			require.NotNil(s.T(), msg)
			decoder, err := newDecoder(msg.Value)
			require.Nil(s.T(), err)
			checkDDLDecoder(decoder, cs[i:i+1])
		}
	}

	encoder = encoderBuilder.Build()
	for _, cs := range s.resolvedTsCases {
		for i, ts := range cs {
			msg, err := encoder.EncodeCheckpointEvent(ts)
			require.Nil(s.T(), err)
			require.NotNil(s.T(), msg)
			decoder, err := newDecoder(msg.Value)
			require.Nil(s.T(), err)
			checkTSDecoder(decoder, cs[i:i+1])
		}
	}
}

func (s *craftBatchSuite) TestMaxMessageBytes() {
	defer testleak.AfterTest(s.T())()
	config := NewConfig(config.ProtocolCraft, timeutil.SystemLocation()).WithMaxMessageBytes(256)
	encoder := newCraftEventBatchEncoderBuilder(config).Build()

	testEvent := &model.RowChangedEvent{
		CommitTs: 1,
		Table:    &model.TableName{Schema: "a", Table: "b"},
		Columns:  []*model.Column{{Name: "col1", Type: mysql.TypeVarchar, Value: []byte("aa")}},
	}

	for i := 0; i < 10000; i++ {
		err := encoder.AppendRowChangedEvent(testEvent)
		require.Nil(s.T(), err)
	}

	messages := encoder.Build()
	for _, msg := range messages {
		require.LessOrEqual(s.T(), msg.Length(), 256)
	}
}

func (s *craftBatchSuite) TestMaxBatchSize() {
	defer testleak.AfterTest(s.T())()
	config := NewConfig(config.ProtocolCraft, timeutil.SystemLocation()).WithMaxMessageBytes(10485760)
	config.maxBatchSize = 64
	encoder := newCraftEventBatchEncoderBuilder(config).Build()

	testEvent := &model.RowChangedEvent{
		CommitTs: 1,
		Table:    &model.TableName{Schema: "a", Table: "b"},
		Columns:  []*model.Column{{Name: "col1", Type: mysql.TypeVarchar, Value: []byte("aa")}},
	}

	for i := 0; i < 10000; i++ {
		err := encoder.AppendRowChangedEvent(testEvent)
		require.Nil(s.T(), err)
	}

	messages := encoder.Build()
	sum := 0
	for _, msg := range messages {
		decoder, err := NewCraftEventBatchDecoder(msg.Value)
		require.Nil(s.T(), err)
		count := 0
		for {
			t, hasNext, err := decoder.HasNext()
			require.Nil(s.T(), err)
			if !hasNext {
				break
			}

			require.Equal(s.T(), t, model.MqMessageTypeRow)
			_, err = decoder.NextRowChangedEvent()
			require.Nil(s.T(), err)
			count++
		}
		require.LessOrEqual(s.T(), count, 64)
		sum += count
	}
	require.Equal(s.T(), sum, 10000)
}

func (s *craftBatchSuite) TestDefaultEventBatchCodec() {
	defer testleak.AfterTest(s.T())()
	config := NewConfig(config.ProtocolCraft, timeutil.SystemLocation()).WithMaxMessageBytes(8192)
	config.maxBatchSize = 64

	s.testBatchCodec(newCraftEventBatchEncoderBuilder(config), NewCraftEventBatchDecoder)
}

func (s *craftBatchSuite) TestBuildCraftEventBatchEncoder() {
	defer testleak.AfterTest(s.T())()
	config := NewConfig(config.ProtocolCraft, timeutil.SystemLocation())

	builder := &craftEventBatchEncoderBuilder{config: config}
	encoder, ok := builder.Build().(*CraftEventBatchEncoder)
	require.True(s.T(), ok)
	require.Equal(s.T(), encoder.maxBatchSize, config.maxBatchSize)
	require.Equal(s.T(), encoder.maxMessageBytes, config.maxMessageBytes)
}
