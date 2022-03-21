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
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"sort"
	"testing"

	"github.com/pingcap/check"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/util/timeutil"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/util/testleak"
)

func Test(t *testing.T) { check.TestingT(t) }

type batchSuite struct {
	suite.Suite
	rowCases        [][]*model.RowChangedEvent
	ddlCases        [][]*model.DDLEvent
	resolvedTsCases [][]uint64
}

func TestColumnChangeSuite(t *testing.T) {
	suite.Run(t, &batchSuite{
		rowCases:        codecRowCases,
		ddlCases:        codecDDLCases,
		resolvedTsCases: codecResolvedTSCases,
	})
}

type columnsArray []*model.Column

func (a columnsArray) Len() int {
	return len(a)
}

func (a columnsArray) Less(i, j int) bool {
	return a[i].Name < a[j].Name
}

func (a columnsArray) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func sortColumnsArrays(arrays ...[]*model.Column) {
	for _, array := range arrays {
		if array != nil {
			sort.Sort(columnsArray(array))
		}
	}
}

func (s *batchSuite) testBatchCodec(encoderBuilder EncoderBuilder, newDecoder func(key []byte, value []byte) (EventBatchDecoder, error)) {
	checkRowDecoder := func(decoder EventBatchDecoder, cs []*model.RowChangedEvent) {
		index := 0
		for {
			tp, hasNext, err := decoder.HasNext()
			require.Nil(s.T(), err)
			if !hasNext {
				break
			}
			require.Equal(s.T(), model.MqMessageTypeRow, tp)
			row, err := decoder.NextRowChangedEvent()
			require.Nil(s.T(), err)
			sortColumnsArrays(row.Columns, row.PreColumns, cs[index].Columns, cs[index].PreColumns)
			require.Equal(s.T(), cs[index], row)
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
			require.Equal(s.T(), model.MqMessageTypeDDL, tp)
			ddl, err := decoder.NextDDLEvent()
			require.Nil(s.T(), err)
			require.Equal(s.T(), cs[index], ddl)
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
			require.Equal(s.T(), model.MqMessageTypeResolved, tp)
			ts, err := decoder.NextResolvedEvent()
			require.Nil(s.T(), err)
			require.Equal(s.T(), cs[index], ts)
			index++
		}
	}

	for _, cs := range s.rowCases {
		encoder := encoderBuilder.Build()

		for _, row := range cs {
			err := encoder.AppendRowChangedEvent(row)
			require.Nil(s.T(), err)
		}

		if len(cs) > 0 {
			res := encoder.Build()
			require.Len(s.T(), res, 1)
			require.Equal(s.T(), len(cs), res[0].GetRowsCount())
			decoder, err := newDecoder(res[0].Key, res[0].Value)
			require.Nil(s.T(), err)
			checkRowDecoder(decoder, cs)
		}
	}
	for _, cs := range s.ddlCases {
		encoder := encoderBuilder.Build()
		for i, ddl := range cs {
			msg, err := encoder.EncodeDDLEvent(ddl)
			require.Nil(s.T(), err)
			require.NotNil(s.T(), msg)
			decoder, err := newDecoder(msg.Key, msg.Value)
			require.Nil(s.T(), err)
			checkDDLDecoder(decoder, cs[i:i+1])

		}
	}

	for _, cs := range s.resolvedTsCases {
		encoder := encoderBuilder.Build()
		for i, ts := range cs {
			msg, err := encoder.EncodeCheckpointEvent(ts)
			require.Nil(s.T(), err)
			require.NotNil(s.T(), msg)
			decoder, err := newDecoder(msg.Key, msg.Value)
			require.Nil(s.T(), err)
			checkTSDecoder(decoder, cs[i:i+1])
		}
	}
}

func (s *batchSuite) TestBuildJSONEventBatchEncoder() {
	defer testleak.AfterTest(s.T())()
	config := NewConfig(config.ProtocolOpen, timeutil.SystemLocation())
	builder := &jsonEventBatchEncoderBuilder{config: config}
	encoder, ok := builder.Build().(*JSONEventBatchEncoder)
	require.True(s.T(), ok)
	require.Equal(s.T(), config.maxBatchSize, encoder.maxBatchSize)
	require.Equal(s.T(), config.maxMessageBytes, encoder.maxMessageBytes)
}

func (s *batchSuite) TestMaxMessageBytes() {
	defer testleak.AfterTest(s.T())()

	// the size of `testEvent` is 87
	testEvent := &model.RowChangedEvent{
		CommitTs: 1,
		Table:    &model.TableName{Schema: "a", Table: "b"},
		Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "aa"}},
	}

	// for a single message, the overhead is 36(maximumRecordOverhead) + 8(versionHea) = 44, just can hold it.
	a := 87 + 44
	config := NewConfig(config.ProtocolOpen, timeutil.SystemLocation()).WithMaxMessageBytes(a)
	encoder := newJSONEventBatchEncoderBuilder(config).Build()
	err := encoder.AppendRowChangedEvent(testEvent)
	require.Nil(s.T(), err)

	// cannot hold a single message
	config = config.WithMaxMessageBytes(a - 1)
	encoder = newJSONEventBatchEncoderBuilder(config).Build()
	err = encoder.AppendRowChangedEvent(testEvent)
	require.NotNil(s.T(), err)

	// make sure each batch's `Length` not greater than `max-message-bytes`
	config = config.WithMaxMessageBytes(256)
	encoder = newJSONEventBatchEncoderBuilder(config).Build()
	for i := 0; i < 10000; i++ {
		err := encoder.AppendRowChangedEvent(testEvent)
		require.Nil(s.T(), err)
	}

	messages := encoder.Build()
	for _, msg := range messages {
		require.LessOrEqual(s.T(), msg.Length(), 256)
	}
}

func (s *batchSuite) TestMaxBatchSize() {
	defer testleak.AfterTest(s.T())()

	config := NewConfig(config.ProtocolOpen, timeutil.SystemLocation()).WithMaxMessageBytes(1048576)
	config.maxBatchSize = 64
	encoder := newJSONEventBatchEncoderBuilder(config).Build()

	testEvent := &model.RowChangedEvent{
		CommitTs: 1,
		Table:    &model.TableName{Schema: "a", Table: "b"},
		Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "aa"}},
	}

	for i := 0; i < 10000; i++ {
		err := encoder.AppendRowChangedEvent(testEvent)
		require.Nil(s.T(), err)
	}

	messages := encoder.Build()
	sum := 0
	for _, msg := range messages {
		decoder, err := NewJSONEventBatchDecoder(msg.Key, msg.Value)
		require.Nil(s.T(), err)
		count := 0
		for {
			t, hasNext, err := decoder.HasNext()
			require.Nil(s.T(), err)
			if !hasNext {
				break
			}
			require.Equal(s.T(), model.MqMessageTypeRow, t)
			_, err = decoder.NextRowChangedEvent()
			require.Nil(s.T(), err)
			count++
		}
		require.LessOrEqual(s.T(), count,64)
		sum += count
	}
	require.Equal(s.T(), 10000, sum)
}

func (s *batchSuite) TestDefaultEventBatchCodec() {
	defer testleak.AfterTest(s.T())()

	config := NewConfig(config.ProtocolOpen, timeutil.SystemLocation()).WithMaxMessageBytes(8192)
	config.maxBatchSize = 64
	s.testBatchCodec(newJSONEventBatchEncoderBuilder(config), NewJSONEventBatchDecoder)
}

func TestFormatCol(t *testing.T) {
	defer testleak.AfterTest(t)()
	row := &mqMessageRow{Update: map[string]column{"test": {
		Type:  mysql.TypeString,
		Value: "测",
	}}}
	rowEncode, err := row.Encode()
	require.Nil(t, err)
	row2 := new(mqMessageRow)
	err = row2.Decode(rowEncode)
	require.Nil(t, err)
	require.Equal(t, row, row2)

	row = &mqMessageRow{Update: map[string]column{"test": {
		Type:  mysql.TypeBlob,
		Value: []byte("测"),
	}}}
	rowEncode, err = row.Encode()
	require.Nil(t, err)
	row2 = new(mqMessageRow)
	err = row2.Decode(rowEncode)
	require.Nil(t, err)
	require.Equal(t, row, row2)
}

func TestNonBinaryStringCol(t *testing.T) {
	defer testleak.AfterTest(t)()
	col := &model.Column{
		Name:  "test",
		Type:  mysql.TypeString,
		Value: "value",
	}
	jsonCol := column{}
	jsonCol.FromSinkColumn(col)
	row := &mqMessageRow{Update: map[string]column{"test": jsonCol}}
	rowEncode, err := row.Encode()
	require.Nil(t, err)
	row2 := new(mqMessageRow)
	err = row2.Decode(rowEncode)
	require.Nil(t, err)
	require.Equal(t, row, row2)
	jsonCol2 := row2.Update["test"]
	col2 := jsonCol2.ToSinkColumn("test")
	col2.Value = string(col2.Value.([]byte))
	require.Equal(t, col, col2)
}

func TestVarBinaryCol(t *testing.T) {
	defer testleak.AfterTest(t)()
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
	require.Nil(t, err)
	row2 := new(mqMessageRow)
	err = row2.Decode(rowEncode)
	require.Nil(t, err)
	require.Equal(t, row, row2)
	jsonCol2 := row2.Update["test"]
	col2 := jsonCol2.ToSinkColumn("test")
	require.Equal(t, col, col2)
}
