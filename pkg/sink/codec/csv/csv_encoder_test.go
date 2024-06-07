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

package csv

import (
	"testing"

	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
)

func TestCSVBatchCodec(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	ddl := helper.DDL2Event("create table test.table1(col1 int primary key)")
	event1 := helper.DML2Event("insert into test.table1 values (1)", "test", "table1")
	event2 := helper.DML2Event("insert into test.table1 values (2)", "test", "table1")

	testCases := []*model.SingleTableTxn{
		{
			Rows: []*model.RowChangedEvent{
				event1,
				event2,
			},
		},
		{
			TableInfo: ddl.TableInfo,
			Rows:      nil,
		},
	}

	for _, cs := range testCases {
		encoder := newBatchEncoder(&common.Config{
			Delimiter:       ",",
			Quote:           "\"",
			Terminator:      "\n",
			NullString:      "\\N",
			IncludeCommitTs: true,
		})
		err := encoder.AppendTxnEvent(cs, nil)
		require.Nil(t, err)
		messages := encoder.Build()
		if len(cs.Rows) == 0 {
			require.Nil(t, messages)
			continue
		}
		require.Len(t, messages, 1)
		require.Equal(t, len(cs.Rows), messages[0].GetRowsCount())
	}
}

func TestCSVAppendRowChangedEventWithCallback(t *testing.T) {
	encoder := newBatchEncoder(&common.Config{
		Delimiter:       ",",
		Quote:           "\"",
		Terminator:      "\n",
		NullString:      "\\N",
		IncludeCommitTs: true,
	})
	require.NotNil(t, encoder)

	count := 0

	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	_ = helper.DDL2Event("create table test.table1(col1 int primary key)")
	row := helper.DML2Event("insert into test.table1 values (1)", "test", "table1")
	txn := &model.SingleTableTxn{
		TableInfo: row.TableInfo,
		Rows:      []*model.RowChangedEvent{row},
	}
	callback := func() {
		count += 1
	}

	// Empty build makes sure that the callback build logic not broken.
	msgs := encoder.Build()
	require.Len(t, msgs, 0, "no message should be built and no panic")

	// Append the event.
	err := encoder.AppendTxnEvent(txn, callback)
	require.Nil(t, err)
	require.Equal(t, 0, count, "nothing should be called")

	msgs = encoder.Build()
	require.Len(t, msgs, 1, "expected one message")
	msgs[0].Callback()
	require.Equal(t, 1, count, "expected all callbacks to be called")
}
