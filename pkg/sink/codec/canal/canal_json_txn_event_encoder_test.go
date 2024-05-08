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

package canal

import (
	"testing"

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
	_ = helper.DDL2Event(sql)
	testEvent := helper.DML2Event(`insert into test.t values("aa")`, "test", "t")
	// the size of `testEvent` after being encoded by canal-json is 200
	txn := &model.SingleTableTxn{
		TableInfo: testEvent.TableInfo,
		Rows: []*model.RowChangedEvent{
			testEvent,
		},
	}

	// the test message length is smaller than max-message-bytes
	maxMessageBytes := 300
	cfg := common.NewConfig(config.ProtocolCanalJSON).WithMaxMessageBytes(maxMessageBytes)
	encoder := NewJSONTxnEventEncoderBuilder(cfg).Build()
	err := encoder.AppendTxnEvent(txn, nil)
	require.Nil(t, err)

	// the test message length is larger than max-message-bytes
	cfg = cfg.WithMaxMessageBytes(100)
	encoder = NewJSONTxnEventEncoderBuilder(cfg).Build()
	err = encoder.AppendTxnEvent(txn, nil)
	require.NotNil(t, err)
}

func TestCanalJSONAppendTxnEventEncoderWithCallback(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	sql := `create table test.t(a varchar(255) primary key)`
	_ = helper.DDL2Event(sql)

	cfg := common.NewConfig(config.ProtocolCanalJSON)
	encoder := NewJSONTxnEventEncoderBuilder(cfg).Build()
	require.NotNil(t, encoder)

	event1 := helper.DML2Event(`insert into test.t values("aa")`, "test", "t")
	event2 := helper.DML2Event(`insert into test.t values("bb")`, "test", "t")

	count := 0

	txn := &model.SingleTableTxn{
		TableInfo: event1.TableInfo,
		Rows:      []*model.RowChangedEvent{event1, event2},
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
