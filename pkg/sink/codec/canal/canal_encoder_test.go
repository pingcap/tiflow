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

package canal

import (
	"context"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	canal "github.com/pingcap/tiflow/proto/canal"
	"github.com/stretchr/testify/require"
)

var (
	rowCases = [][]*model.RowChangedEvent{
		{{
			CommitTs: 1,
			TableInfo: &model.TableInfo{
				TableName: model.TableName{Schema: "test", Table: "t"},
			},
			Columns: []*model.Column{{
				Name:  "col1",
				Type:  mysql.TypeVarchar,
				Value: []byte("aa"),
			}},
		}},
		{
			{
				CommitTs: 1,
				TableInfo: &model.TableInfo{
					TableName: model.TableName{Schema: "test", Table: "t"},
				},
				Columns: []*model.Column{{
					Name:  "col1",
					Type:  mysql.TypeVarchar,
					Value: []byte("aa"),
				}},
			},
			{
				CommitTs: 2,
				TableInfo: &model.TableInfo{
					TableName: model.TableName{Schema: "test", Table: "t"},
				},
				Columns: []*model.Column{{Name: "col1", Type: 1, Value: "bb"}},
			},
		},
	}

	ddlCases = [][]*model.DDLEvent{
		{{
			CommitTs: 1,
			TableInfo: &model.TableInfo{
				TableName: model.TableName{
					Schema: "a", Table: "b",
				},
			},
			Query: "create table a",
			Type:  1,
		}},
		{
			{
				CommitTs: 2,
				TableInfo: &model.TableInfo{
					TableName: model.TableName{
						Schema: "a", Table: "b",
					},
				},
				Query: "create table b",
				Type:  3,
			},
			{
				CommitTs: 3,
				TableInfo: &model.TableInfo{
					TableName: model.TableName{
						Schema: "a", Table: "b",
					},
				},
				Query: "create table c",
				Type:  3,
			},
		},
	}
)

func TestCanalBatchEncoder(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	sql := `create table test.t(a varchar(10) primary key)`
	job := helper.DDL2Job(sql)
	tableInfo := model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	for _, cs := range rowCases {
		encoder := newBatchEncoder(common.NewConfig(config.ProtocolCanal))
		for _, row := range cs {
			_, _, colInfo := tableInfo.GetRowColInfos()
			row.TableInfo = tableInfo
			row.ColInfos = colInfo
			err := encoder.AppendRowChangedEvent(context.Background(), "", row, nil)
			require.NoError(t, err)
		}
		res := encoder.Build()

		if len(cs) == 0 {
			require.Nil(t, res)
			continue
		}
		require.Len(t, res, 1)
		require.Nil(t, res[0].Key)
		require.Equal(t, len(cs), res[0].GetRowsCount())

		packet := &canal.Packet{}
		err := proto.Unmarshal(res[0].Value, packet)
		require.Nil(t, err)
		require.Equal(t, canal.PacketType_MESSAGES, packet.GetType())
		messages := &canal.Messages{}
		err = proto.Unmarshal(packet.GetBody(), messages)
		require.Nil(t, err)
		require.Equal(t, len(cs), len(messages.GetMessages()))
	}

	for _, cs := range ddlCases {
		encoder := newBatchEncoder(common.NewConfig(config.ProtocolCanal))
		for _, ddl := range cs {
			msg, err := encoder.EncodeDDLEvent(ddl)
			require.NoError(t, err)
			require.NotNil(t, msg)
			require.Nil(t, msg.Key)

			packet := &canal.Packet{}
			err = proto.Unmarshal(msg.Value, packet)
			require.NoError(t, err)
			require.Equal(t, canal.PacketType_MESSAGES, packet.GetType())
			messages := &canal.Messages{}
			err = proto.Unmarshal(packet.GetBody(), messages)
			require.NoError(t, err)
			require.Equal(t, 1, len(messages.GetMessages()))
			require.NoError(t, err)
		}
	}
}

func TestCanalAppendRowChangedEventWithCallback(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	sql := `create table test.t(a varchar(10) primary key)`
	job := helper.DDL2Job(sql)
	tableInfo := model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	_, _, colInfo := tableInfo.GetRowColInfos()
	row := &model.RowChangedEvent{
		CommitTs: 1,
		Columns: []*model.Column{{
			Name:  "col1",
			Type:  mysql.TypeVarchar,
			Value: []byte("aa"),
		}},
		TableInfo: tableInfo,
		ColInfos:  colInfo,
	}

	encoder := newBatchEncoder(common.NewConfig(config.ProtocolCanal))
	require.NotNil(t, encoder)

	count := 0

	tests := []struct {
		row      *model.RowChangedEvent
		callback func()
	}{
		{
			row: row,
			callback: func() {
				count += 1
			},
		},
		{
			row: row,
			callback: func() {
				count += 2
			},
		},
		{
			row: row,
			callback: func() {
				count += 3
			},
		},
		{
			row: row,
			callback: func() {
				count += 4
			},
		},
		{
			row: row,
			callback: func() {
				count += 5
			},
		},
	}

	// Empty build makes sure that the callback build logic not broken.
	msgs := encoder.Build()
	require.Len(t, msgs, 0, "no message should be built and no panic")

	// Append the events.
	for _, test := range tests {
		err := encoder.AppendRowChangedEvent(context.Background(), "", test.row, test.callback)
		require.Nil(t, err)
	}
	require.Equal(t, 0, count, "nothing should be called")

	msgs = encoder.Build()
	require.Len(t, msgs, 1, "expected one message")
	msgs[0].Callback()
	require.Equal(t, 15, count, "expected all callbacks to be called")
}
