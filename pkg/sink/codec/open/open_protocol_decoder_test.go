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

package open

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
)

var (
	insertEvent = &model.RowChangedEvent{
		CommitTs: 1,
		Table:    &model.TableName{Schema: "test", Table: "t"},
		Columns: []*model.Column{
			{
				Name:  "a",
				Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
				Type:  mysql.TypeLong,
				Value: int64(1),
			},
			{
				Name:  "b",
				Type:  mysql.TypeLong,
				Value: int64(2),
			},
			{
				Name:  "c",
				Type:  mysql.TypeLong,
				Value: int64(3),
			},
		},
	}
)

func TestDecodeEvent(t *testing.T) {
	config := common.NewConfig(config.ProtocolOpen)
	encoder := NewBatchEncoderBuilder(config).Build()

	ctx := context.Background()
	topic := "test"
	err := encoder.AppendRowChangedEvent(ctx, topic, insertEvent, nil)
	require.NoError(t, err)

	message := encoder.Build()[0]

	decoder := NewBatchDecoder()
	err = decoder.AddKeyValue(message.Key, message.Value)
	require.NoError(t, err)
	tp, hasNext, err := decoder.HasNext()
	require.NoError(t, err)
	require.True(t, hasNext)
	require.Equal(t, model.MessageTypeRow, tp)

	obtained, onlyHandleKey, err := decoder.NextRowChangedEvent()
	require.NoError(t, err)
	require.False(t, onlyHandleKey)

	obtainedColumns := make(map[string]*model.Column)
	for _, col := range obtained.Columns {
		obtainedColumns[col.Name] = col
	}

	for _, col := range insertEvent.Columns {
		require.Contains(t, obtainedColumns, col.Name)
	}
}

func TestDecodeEventOnlyHandleKeyColumns(t *testing.T) {
	config := common.NewConfig(config.ProtocolOpen)
	config.LargeMessageOnlyHandleKeyColumns = true
	config.MaxMessageBytes = 185

	encoder := NewBatchEncoderBuilder(config).Build()

	ctx := context.Background()
	topic := "test"
	err := encoder.AppendRowChangedEvent(ctx, topic, insertEvent, nil)
	require.NoError(t, err)

	message := encoder.Build()[0]

	decoder := NewBatchDecoder()
	err = decoder.AddKeyValue(message.Key, message.Value)
	require.NoError(t, err)
	tp, hasNext, err := decoder.HasNext()
	require.NoError(t, err)
	require.True(t, hasNext)
	require.Equal(t, model.MessageTypeRow, tp)

	obtained, onlyHandleKey, err := decoder.NextRowChangedEvent()
	require.NoError(t, err)
	require.True(t, onlyHandleKey)
	require.Equal(t, insertEvent.CommitTs, obtained.CommitTs)

	obtainedColumns := make(map[string]*model.Column)
	for _, col := range obtained.Columns {
		obtainedColumns[col.Name] = col
		require.True(t, col.Flag.IsHandleKey())
	}

	for _, col := range insertEvent.Columns {
		if col.Flag.IsHandleKey() {
			require.Contains(t, obtainedColumns, col.Name)
		}
	}
}
