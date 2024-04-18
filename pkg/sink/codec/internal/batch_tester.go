// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package internal

import (
	"context"
	"testing"

	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/sink/codec"
	"github.com/stretchr/testify/require"
)

// NewDMLTestCases returns a bunch of DML test cases.
func NewDMLTestCases(t testing.TB) [][]*model.RowChangedEvent {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	sql := "create table test.t(a varchar(255), b text, c date, d timestamp, " +
		"e datetime, f float, g bigint, h int, primary key(a))"
	_ = helper.DDL2Event(sql)

	sql = `insert into test.t values ("varchar1", "string1", "2021-01-02", "2021-01-02 00:00:00", "2021-01-02 00:00:00", 2.0, 2000, null)`
	event := helper.DML2Event(sql, "test", "t")
	event.PreColumns = event.Columns

	result := [][]*model.RowChangedEvent{
		{event},
		{event, event, event, event},
		{},
	}
	return result
}

func newDDLTestCases(t *testing.T) [][]*model.DDLEvent {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	sql := "create table test.t(a int primary key, b int, c int)"
	ddlEvent := helper.DDL2Event(sql)

	result := [][]*model.DDLEvent{
		{ddlEvent},
		{ddlEvent, ddlEvent, ddlEvent},
		{},
	}
	return result
}

// TestBatchCodec tests bunch of cases for RowEventDecoder.
func TestBatchCodec(
	t *testing.T,
	encoderBuilder codec.RowEventEncoderBuilder,
	newDecoder func(key []byte, value []byte) (codec.RowEventDecoder, error),
) {
	checkRowDecoder := func(decoder codec.RowEventDecoder) {
		for {
			tp, hasNext, err := decoder.HasNext()
			require.Nil(t, err)
			if !hasNext {
				break
			}
			require.Equal(t, model.MessageTypeRow, tp)
			decodedEvent, err := decoder.NextRowChangedEvent()
			require.NoError(t, err)
			require.NotNil(t, decodedEvent)
		}
	}
	checkDDLDecoder := func(decoder codec.RowEventDecoder) {
		for {
			tp, hasNext, err := decoder.HasNext()
			require.Nil(t, err)
			if !hasNext {
				break
			}
			require.Equal(t, model.MessageTypeDDL, tp)
			ddl, err := decoder.NextDDLEvent()
			require.Nil(t, err)
			require.NotNil(t, ddl)
		}
	}
	checkTSDecoder := func(decoder codec.RowEventDecoder, cs []uint64) {
		index := 0
		for {
			tp, hasNext, err := decoder.HasNext()
			require.Nil(t, err)
			if !hasNext {
				break
			}
			require.Equal(t, model.MessageTypeResolved, tp)
			ts, err := decoder.NextResolvedEvent()
			require.Nil(t, err)
			require.Equal(t, cs[index], ts)
			index++
		}
	}

	dmlCases := NewDMLTestCases(t)
	for _, cs := range dmlCases {
		encoder := encoderBuilder.Build()

		for _, event := range cs {
			err := encoder.AppendRowChangedEvent(context.Background(), "", event, nil)
			require.NoError(t, err)
		}

		if len(cs) > 0 {
			res := encoder.Build()
			require.Len(t, res, 1)
			require.Equal(t, len(cs), res[0].GetRowsCount())

			decoder, err := newDecoder(res[0].Key, res[0].Value)
			require.NoError(t, err)
			checkRowDecoder(decoder)
		}
	}

	ddlCases := newDDLTestCases(t)
	for _, cs := range ddlCases {
		encoder := encoderBuilder.Build()
		for _, ddl := range cs {
			msg, err := encoder.EncodeDDLEvent(ddl)
			require.Nil(t, err)
			require.NotNil(t, msg)

			decoder, err := newDecoder(msg.Key, msg.Value)
			require.NoError(t, err)

			checkDDLDecoder(decoder)
		}
	}

	resolvedTsCases := [][]uint64{{424316592563683329}, {424316594097225729, 424316594214141953, 424316594345213953}, {}}
	for _, cs := range resolvedTsCases {
		encoder := encoderBuilder.Build()
		for i, ts := range cs {
			msg, err := encoder.EncodeCheckpointEvent(ts)
			require.Nil(t, err)
			require.NotNil(t, msg)

			decoder, err := newDecoder(msg.Key, msg.Value)
			require.NoError(t, err)

			checkTSDecoder(decoder, cs[i:i+1])
		}
	}
}
