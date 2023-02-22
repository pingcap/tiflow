// Copyright 2022 PingCAP, Inc.
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

package canal

import (
	"context"
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestNewCanalJSONBatchDecoder4RowMessage(t *testing.T) {
	t.Parallel()
	expectedDecodedValue := collectExpectedDecodedValue(testColumnsTable)
	for _, encodeEnable := range []bool{false, true} {
		encoder := newJSONBatchEncoder(&common.Config{
			EnableTiDBExtension: encodeEnable,
			Terminator:          config.CRLF,
			MaxMessageBytes:     config.DefaultMaxMessageBytes,
		})
		require.NotNil(t, encoder)

		err := encoder.AppendRowChangedEvent(context.Background(), "", testCaseInsert, nil)
		require.Nil(t, err)

		messages := encoder.Build()
		require.Equal(t, 1, len(messages))
		msg := messages[0]

		for _, decodeEnable := range []bool{false, true} {
			decoder := NewBatchDecoder(msg.Value, decodeEnable, "")

			ty, hasNext, err := decoder.HasNext()
			require.Nil(t, err)
			require.True(t, hasNext)
			require.Equal(t, model.MessageTypeRow, ty)

			consumed, err := decoder.NextRowChangedEvent()
			require.Nil(t, err)

			require.Equal(t, testCaseInsert.Table, consumed.Table)
			if encodeEnable && decodeEnable {
				require.Equal(t, testCaseInsert.CommitTs, consumed.CommitTs)
			} else {
				require.Equal(t, uint64(0), consumed.CommitTs)
			}

			for _, col := range consumed.Columns {
				expected, ok := expectedDecodedValue[col.Name]
				require.True(t, ok)
				require.Equal(t, expected, col.Value)

				for _, item := range testCaseInsert.Columns {
					if item.Name == col.Name {
						require.Equal(t, item.Type, col.Type)
					}
				}
			}

			_, hasNext, _ = decoder.HasNext()
			require.False(t, hasNext)

			consumed, err = decoder.NextRowChangedEvent()
			require.NotNil(t, err)
			require.Nil(t, consumed)
		}
	}
}

func TestNewCanalJSONBatchDecoder4DDLMessage(t *testing.T) {
	t.Parallel()
	for _, encodeEnable := range []bool{false, true} {
		encoder := &JSONBatchEncoder{builder: newCanalEntryBuilder(), enableTiDBExtension: encodeEnable}
		require.NotNil(t, encoder)

		result, err := encoder.EncodeDDLEvent(testCaseDDL)
		require.Nil(t, err)
		require.NotNil(t, result)

		for _, decodeEnable := range []bool{false, true} {
			decoder := NewBatchDecoder(result.Value, decodeEnable, "")

			ty, hasNext, err := decoder.HasNext()
			require.Nil(t, err)
			require.True(t, hasNext)
			require.Equal(t, model.MessageTypeDDL, ty)

			consumed, err := decoder.NextDDLEvent()
			require.Nil(t, err)

			if encodeEnable && decodeEnable {
				require.Equal(t, testCaseDDL.CommitTs, consumed.CommitTs)
			} else {
				require.Equal(t, uint64(0), consumed.CommitTs)
			}

			require.Equal(t, testCaseDDL.TableInfo, consumed.TableInfo)
			require.Equal(t, testCaseDDL.Query, consumed.Query)

			ty, hasNext, err = decoder.HasNext()
			require.Nil(t, err)
			require.False(t, hasNext)
			require.Equal(t, model.MessageTypeUnknown, ty)

			consumed, err = decoder.NextDDLEvent()
			require.NotNil(t, err)
			require.Nil(t, consumed)
		}
	}
}

func TestCanalJSONBatchDecoderWithTerminator(t *testing.T) {
	encodedValue := `{"id":0,"database":"test","table":"employee","pkNames":["id"],"isDdl":false,"type":"INSERT","es":1668067205238,"ts":1668067206650,"sql":"","sqlType":{"FirstName":12,"HireDate":91,"LastName":12,"OfficeLocation":12,"id":4},"mysqlType":{"FirstName":"varchar","HireDate":"date","LastName":"varchar","OfficeLocation":"varchar","id":"int"},"data":[{"FirstName":"Bob","HireDate":"2014-06-04","LastName":"Smith","OfficeLocation":"New York","id":"101"}],"old":null}
{"id":0,"database":"test","table":"employee","pkNames":["id"],"isDdl":false,"type":"UPDATE","es":1668067229137,"ts":1668067230720,"sql":"","sqlType":{"FirstName":12,"HireDate":91,"LastName":12,"OfficeLocation":12,"id":4},"mysqlType":{"FirstName":"varchar","HireDate":"date","LastName":"varchar","OfficeLocation":"varchar","id":"int"},"data":[{"FirstName":"Bob","HireDate":"2015-10-08","LastName":"Smith","OfficeLocation":"Los Angeles","id":"101"}],"old":[{"FirstName":"Bob","HireDate":"2014-06-04","LastName":"Smith","OfficeLocation":"New York","id":"101"}]}
{"id":0,"database":"test","table":"employee","pkNames":["id"],"isDdl":false,"type":"DELETE","es":1668067230388,"ts":1668067231725,"sql":"","sqlType":{"FirstName":12,"HireDate":91,"LastName":12,"OfficeLocation":12,"id":4},"mysqlType":{"FirstName":"varchar","HireDate":"date","LastName":"varchar","OfficeLocation":"varchar","id":"int"},"data":[{"FirstName":"Bob","HireDate":"2015-10-08","LastName":"Smith","OfficeLocation":"Los Angeles","id":"101"}],"old":null}`
	decoder := NewBatchDecoder([]byte(encodedValue), false, "\n")
	cnt := 0
	for {
		tp, hasNext, err := decoder.HasNext()
		if !hasNext {
			break
		}
		require.Nil(t, err)
		require.Equal(t, model.MessageTypeRow, tp)
		cnt++
		event, err := decoder.NextRowChangedEvent()
		require.Nil(t, err)
		require.NotNil(t, event)
	}
	require.Equal(t, 3, cnt)
}
