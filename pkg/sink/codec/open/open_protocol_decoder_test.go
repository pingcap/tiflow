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
	"database/sql"
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
	codecConfig := common.NewConfig(config.ProtocolOpen)
	encoder := NewBatchEncoderBuilder(codecConfig).Build()

	ctx := context.Background()
	topic := "test"
	err := encoder.AppendRowChangedEvent(ctx, topic, insertEvent, nil)
	require.NoError(t, err)

	message := encoder.Build()[0]

	decoder, err := NewBatchDecoder(ctx, codecConfig, nil)
	require.NoError(t, err)

	err = decoder.AddKeyValue(message.Key, message.Value)
	require.NoError(t, err)
	tp, hasNext, err := decoder.HasNext()
	require.NoError(t, err)
	require.True(t, hasNext)
	require.Equal(t, model.MessageTypeRow, tp)

	obtained, err := decoder.NextRowChangedEvent()
	require.NoError(t, err)

	obtainedColumns := make(map[string]*model.Column)
	for _, col := range obtained.Columns {
		obtainedColumns[col.Name] = col
	}

	for _, col := range insertEvent.Columns {
		require.Contains(t, obtainedColumns, col.Name)
	}
}

func TestDecodeEventOnlyHandleKeyColumns(t *testing.T) {
	codecConfig := common.NewConfig(config.ProtocolOpen)
	codecConfig.LargeMessageHandle = &config.LargeMessageHandleConfig{
		LargeMessageHandleOption: config.LargeMessageHandleOptionHandleKeyOnly,
	}

	//config.LargeMessageOnlyHandleKeyColumns = true
	codecConfig.MaxMessageBytes = 185

	encoder := NewBatchEncoderBuilder(codecConfig).Build()

	ctx := context.Background()
	topic := "test"
	err := encoder.AppendRowChangedEvent(ctx, topic, insertEvent, nil)
	require.NoError(t, err)

	message := encoder.Build()[0]

	decoder, err := NewBatchDecoder(ctx, codecConfig, &sql.DB{})
	require.NoError(t, err)
	err = decoder.AddKeyValue(message.Key, message.Value)
	require.NoError(t, err)
	tp, hasNext, err := decoder.HasNext()
	require.NoError(t, err)
	require.True(t, hasNext)
	require.Equal(t, model.MessageTypeRow, tp)

	nextEvent := decoder.(*BatchDecoder).nextEvent
	require.NotNil(t, nextEvent)

	obtainedColumns := make(map[string]*model.Column)
	for _, col := range nextEvent.Columns {
		obtainedColumns[col.Name] = col
		require.True(t, col.Flag.IsHandleKey())
	}

	for _, col := range insertEvent.Columns {
		if col.Flag.IsHandleKey() {
			require.Contains(t, obtainedColumns, col.Name)
		}
	}
}
