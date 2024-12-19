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

	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/sink/codec/utils"
	"github.com/stretchr/testify/require"
)

func TestNewCanalJSONBatchDecoder4RowMessage(t *testing.T) {
	_, insertEvent, _, _ := utils.NewLargeEvent4Test(t, config.GetDefaultReplicaConfig())
	ctx := context.Background()

	for _, encodeEnable := range []bool{false, true} {
		encodeConfig := common.NewConfig(config.ProtocolCanalJSON)
		encodeConfig.EnableTiDBExtension = encodeEnable
		encodeConfig.Terminator = config.CRLF

		builder, err := NewJSONRowEventEncoderBuilder(ctx, encodeConfig)
		require.NoError(t, err)
		encoder := builder.Build()

		err = encoder.AppendRowChangedEvent(ctx, "", insertEvent, nil)
		require.NoError(t, err)

		messages := encoder.Build()
		require.Equal(t, 1, len(messages))
		msg := messages[0]

		for _, decodeEnable := range []bool{false, true} {
			decodeConfig := common.NewConfig(config.ProtocolCanalJSON)
			decodeConfig.EnableTiDBExtension = decodeEnable

			decoder := NewCanalJSONTxnEventDecoder(decodeConfig)
			err = decoder.AddKeyValue(msg.Key, msg.Value)
			require.NoError(t, err)

			ty, hasNext, err := decoder.HasNext()
			require.NoError(t, err)
			require.True(t, hasNext)
			require.Equal(t, model.MessageTypeRow, ty)

			decodedEvent, err := decoder.NextRowChangedEvent()
			require.NoError(t, err)

			if encodeEnable && decodeEnable {
				require.Equal(t, insertEvent.CommitTs, decodedEvent.CommitTs)
			}
			require.Equal(t, insertEvent.TableInfo.GetSchemaName(), decodedEvent.TableInfo.GetSchemaName())
			require.Equal(t, insertEvent.TableInfo.GetTableName(), decodedEvent.TableInfo.GetTableName())

			decodedColumns := make(map[string]*model.ColumnData, len(decodedEvent.Columns))
			for _, column := range decodedEvent.Columns {
				colName := decodedEvent.TableInfo.ForceGetColumnName(column.ColumnID)
				decodedColumns[colName] = column
			}
			for _, col := range insertEvent.Columns {
				colName := insertEvent.TableInfo.ForceGetColumnName(col.ColumnID)
				decoded, ok := decodedColumns[colName]
				require.True(t, ok)
				switch v := col.Value.(type) {
				case types.VectorFloat32:
					require.EqualValues(t, v.String(), decoded.Value)
				default:
					require.EqualValues(t, v, decoded.Value)
				}
			}

			_, hasNext, _ = decoder.HasNext()
			require.False(t, hasNext)

			decodedEvent, err = decoder.NextRowChangedEvent()
			require.Error(t, err)
			require.Nil(t, decodedEvent)
		}
	}
}

func TestCanalJSONBatchDecoderWithTerminator(t *testing.T) {
	encodedValue := `{"id":0,"database":"test","table":"employee","pkNames":["id"],"isDdl":false,"type":"INSERT","es":1668067205238,"ts":1668067206650,"sql":"","sqlType":{"FirstName":12,"HireDate":91,"LastName":12,"OfficeLocation":12,"id":4},"mysqlType":{"FirstName":"varchar","HireDate":"date","LastName":"varchar","OfficeLocation":"varchar","id":"int"},"data":[{"FirstName":"Bob","HireDate":"2014-06-04","LastName":"Smith","OfficeLocation":"New York","id":"101"}],"old":null}
{"id":0,"database":"test","table":"employee","pkNames":["id"],"isDdl":false,"type":"UPDATE","es":1668067229137,"ts":1668067230720,"sql":"","sqlType":{"FirstName":12,"HireDate":91,"LastName":12,"OfficeLocation":12,"id":4},"mysqlType":{"FirstName":"varchar","HireDate":"date","LastName":"varchar","OfficeLocation":"varchar","id":"int"},"data":[{"FirstName":"Bob","HireDate":"2015-10-08","LastName":"Smith","OfficeLocation":"Los Angeles","id":"101"}],"old":[{"FirstName":"Bob","HireDate":"2014-06-04","LastName":"Smith","OfficeLocation":"New York","id":"101"}]}
{"id":0,"database":"test","table":"employee","pkNames":["id"],"isDdl":false,"type":"DELETE","es":1668067230388,"ts":1668067231725,"sql":"","sqlType":{"FirstName":12,"HireDate":91,"LastName":12,"OfficeLocation":12,"id":4},"mysqlType":{"FirstName":"varchar","HireDate":"date","LastName":"varchar","OfficeLocation":"varchar","id":"int"},"data":[{"FirstName":"Bob","HireDate":"2015-10-08","LastName":"Smith","OfficeLocation":"Los Angeles","id":"101"}],"old":null}`
	codecConfig := common.NewConfig(config.ProtocolCanalJSON)
	codecConfig.Terminator = "\n"
	decoder := NewCanalJSONTxnEventDecoder(codecConfig)

	err := decoder.AddKeyValue(nil, []byte(encodedValue))
	require.NoError(t, err)

	cnt := 0
	for {
		tp, hasNext, err := decoder.HasNext()
		if !hasNext {
			break
		}
		require.NoError(t, err)
		require.Equal(t, model.MessageTypeRow, tp)
		cnt++
		event, err := decoder.NextRowChangedEvent()
		require.NoError(t, err)
		require.NotNil(t, event)
	}
	require.Equal(t, 3, cnt)
}
