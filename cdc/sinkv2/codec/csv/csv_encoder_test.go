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

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/rowcodec"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestCSVBatchCodec(t *testing.T) {
	testCases := [][]*model.RowChangedEvent{{
		{
			CommitTs: 1,
			Table:    &model.TableName{Schema: "test", Table: "table1"},
			Columns:  []*model.Column{{Name: "tiny", Value: int64(1), Type: mysql.TypeTiny}},
			ColInfos: []rowcodec.ColInfo{{
				ID:            1,
				IsPKHandle:    false,
				VirtualGenCol: false,
				Ft:            types.NewFieldType(mysql.TypeTiny),
			}},
		},
		{
			CommitTs: 2,
			Table:    &model.TableName{Schema: "test", Table: "table1"},
			Columns:  []*model.Column{{Name: "tiny", Value: int64(2), Type: mysql.TypeTiny}},
			ColInfos: []rowcodec.ColInfo{{
				ID:            1,
				IsPKHandle:    false,
				VirtualGenCol: false,
				Ft:            types.NewFieldType(mysql.TypeTiny),
			}},
		},
	}, {}}

	for _, cs := range testCases {
		encoder := newBatchEncoder(&config.CSVConfig{
			Delimiter:       ",",
			Quote:           "\"",
			Terminator:      "\n",
			NullString:      "\\N",
			IncludeCommitTs: true,
		})

		txn := &eventsink.TxnCallbackableEvent{
			Event: &model.SingleTableTxn{
				CommitTs:     100,
				Table:        &model.TableName{Schema: "test", Table: "table1"},
				TableVersion: 33,
				Rows:         cs,
			},
			Callback: func() {},
		}

		err := encoder.AppendTxnEvent(txn)
		require.NoError(t, err)

		messages := encoder.Build()
		if len(cs) == 0 {
			require.Nil(t, messages)
			continue
		}
		require.Len(t, messages, 1)
		require.Equal(t, len(cs), messages[0].GetRowsCount())
	}
}

func TestCSVAppendRowChangedEventWithCallback(t *testing.T) {
	encoder := newBatchEncoder(&config.CSVConfig{
		Delimiter:       ",",
		Quote:           "\"",
		Terminator:      "\n",
		NullString:      "\\N",
		IncludeCommitTs: true,
	})
	require.NotNil(t, encoder)

	count := 0
	row := &model.RowChangedEvent{
		CommitTs: 1,
		Table:    &model.TableName{Schema: "test", Table: "table1"},
		Columns:  []*model.Column{{Name: "tiny", Value: int64(1), Type: mysql.TypeTiny}},
		ColInfos: []rowcodec.ColInfo{{
			ID:            1,
			IsPKHandle:    false,
			VirtualGenCol: false,
			Ft:            types.NewFieldType(mysql.TypeTiny),
		}},
	}

	txn := &eventsink.TxnCallbackableEvent{
		Event: &model.SingleTableTxn{
			CommitTs:     100,
			Table:        &model.TableName{Schema: "test", Table: "table1"},
			TableVersion: 33,
		},
		Callback: func() {},
	}

	total := 0
	for i := 0; i < 5; i++ {
		txn.Event.Rows = append(txn.Event.Rows, row)
		total += i
	}

	txn.Callback = func() {
		count += total
	}

	// Empty build makes sure that the callback build logic not broken.
	msgs := encoder.Build()
	require.Len(t, msgs, 0, "no message should be built and no panic")

	err := encoder.AppendTxnEvent(txn)
	require.NoError(t, err)
	require.Equal(t, 0, count, "nothing should be called")

	msgs = encoder.Build()
	require.Len(t, msgs, 1, "expected one message")
	msgs[0].Callback()
	require.Equal(t, 10, count, "expected all callbacks to be called")
}
