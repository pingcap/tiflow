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
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
)

func TestCSVBatchCodec(t *testing.T) {
	testCases := []*model.SingleTableTxn{
		{
			Table: &model.TableName{Schema: "test", Table: "table1"},
			Rows: []*model.RowChangedEvent{
				{
					CommitTs: 1,
					Table:    &model.TableName{Schema: "test", Table: "table1"},
					Columns: []*model.Column{{
						Name:  "tiny",
						Value: int64(1), Type: mysql.TypeTiny,
					}},
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
					Columns: []*model.Column{{
						Name:  "tiny",
						Value: int64(2), Type: mysql.TypeTiny,
					}},
					ColInfos: []rowcodec.ColInfo{{
						ID:            1,
						IsPKHandle:    false,
						VirtualGenCol: false,
						Ft:            types.NewFieldType(mysql.TypeTiny),
					}},
				},
			},
		},
		{
			Table: &model.TableName{Schema: "test", Table: "table1"},
			Rows:  nil,
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

	txn := &model.SingleTableTxn{
		Table: row.Table,
		Rows:  []*model.RowChangedEvent{row},
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
