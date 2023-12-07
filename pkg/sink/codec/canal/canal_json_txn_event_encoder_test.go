// Copyright 2023 PingCAP, Inc.
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

//go:build intest
// +build intest

package canal

import (
	"testing"

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
)

func TestBuildCanalJSONTxnEventEncoder(t *testing.T) {
	t.Parallel()
	cfg := common.NewConfig(config.ProtocolCanalJSON)

	builder := NewJSONTxnEventEncoderBuilder(cfg)
	encoder, ok := builder.Build().(*JSONTxnEventEncoder)
	require.True(t, ok)
	require.NotNil(t, encoder.config)
}

func TestCanalJSONTxnEventEncoderMaxMessageBytes(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	sql := `create table test.t(a varchar(255) primary key)`
	job := helper.DDL2Job(sql)
	tableInfo := model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	_, _, colInfos := tableInfo.GetRowColInfos()

	// the size of `testEvent` after being encoded by canal-json is 200
	testEvent := &model.SingleTableTxn{
		Table: &model.TableName{Schema: "test", Table: "t"},
		Rows: []*model.RowChangedEvent{
			{
				CommitTs:  1,
				Table:     &model.TableName{Schema: "test", Table: "t"},
				TableInfo: tableInfo,
				Columns: []*model.Column{{
					Name:  "col1",
					Type:  mysql.TypeVarchar,
					Value: []byte("aa"),
				}},
				ColInfos: colInfos,
			},
		},
	}

	// the test message length is smaller than max-message-bytes
	maxMessageBytes := 300
	cfg := common.NewConfig(config.ProtocolCanalJSON).WithMaxMessageBytes(maxMessageBytes)
	encoder := NewJSONTxnEventEncoderBuilder(cfg).Build()
	err := encoder.AppendTxnEvent(testEvent, nil)
	require.Nil(t, err)

	// the test message length is larger than max-message-bytes
	cfg = cfg.WithMaxMessageBytes(100)
	encoder = NewJSONTxnEventEncoderBuilder(cfg).Build()
	err = encoder.AppendTxnEvent(testEvent, nil)
	require.NotNil(t, err)
}

func TestCanalJSONAppendTxnEventEncoderWithCallback(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	sql := `create table test.t(a varchar(255) primary key)`
	job := helper.DDL2Job(sql)
	tableInfo := model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	_, _, colInfos := tableInfo.GetRowColInfos()

	cfg := common.NewConfig(config.ProtocolCanalJSON)
	encoder := NewJSONTxnEventEncoderBuilder(cfg).Build()
	require.NotNil(t, encoder)

	count := 0

	txn := &model.SingleTableTxn{
		Table: &model.TableName{Schema: "test", Table: "t"},
		Rows: []*model.RowChangedEvent{
			{
				CommitTs:  1,
				Table:     &model.TableName{Schema: "test", Table: "t"},
				TableInfo: tableInfo,
				Columns: []*model.Column{{
					Name:  "a",
					Type:  mysql.TypeVarchar,
					Value: []byte("aa"),
				}},
				ColInfos: colInfos,
			},
			{
				CommitTs:  2,
				Table:     &model.TableName{Schema: "test", Table: "t"},
				TableInfo: tableInfo,
				Columns: []*model.Column{{
					Name:  "a",
					Type:  mysql.TypeVarchar,
					Value: []byte("bb"),
				}},
				ColInfos: colInfos,
			},
		},
	}

	// Empty build makes sure that the callback build logic not broken.
	msgs := encoder.Build()
	require.Len(t, msgs, 0, "no message should be built and no panic")

	// Append the events.
	callback := func() {
		count++
	}
	err := encoder.AppendTxnEvent(txn, callback)
	require.Nil(t, err)
	require.Equal(t, 0, count, "nothing should be called")

	msgs = encoder.Build()
	require.Len(t, msgs, 1, "expected one message")
	msgs[0].Callback()
	require.Equal(t, 1, count, "expected one callback be called")
	// Assert the build reset all the internal states.
	require.Nil(t, encoder.(*JSONTxnEventEncoder).txnSchema)
	require.Nil(t, encoder.(*JSONTxnEventEncoder).txnTable)
	require.Nil(t, encoder.(*JSONTxnEventEncoder).callback)
	require.Equal(t, 0, encoder.(*JSONTxnEventEncoder).batchSize)
	require.Equal(t, 0, encoder.(*JSONTxnEventEncoder).valueBuf.Len())
}
