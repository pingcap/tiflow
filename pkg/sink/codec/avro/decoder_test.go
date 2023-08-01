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

package avro

import (
	"context"
	"math/rand"
	"testing"
	"time"

	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestDecodeEvent(t *testing.T) {
	config := &common.Config{
		MaxMessageBytes:                1024 * 1024,
		EnableTiDBExtension:            true,
		AvroDecimalHandlingMode:        "precise",
		AvroBigintUnsignedHandlingMode: "long",
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	encoder, err := SetupEncoderAndSchemaRegistry4Testing(ctx, config)
	defer TeardownEncoderAndSchemaRegistry4Testing()
	require.NoError(t, err)
	require.NotNil(t, encoder)

	event := newLargeEvent()
	input := &avroEncodeInput{
		columns:  event.Columns,
		colInfos: event.ColInfos,
	}

	rand.New(rand.NewSource(time.Now().Unix())).Shuffle(len(input.columns), func(i, j int) {
		input.columns[i], input.columns[j] = input.columns[j], input.columns[i]
		input.colInfos[i], input.colInfos[j] = input.colInfos[j], input.colInfos[i]
	})

	topic := "avro-test-topic"
	err = encoder.AppendRowChangedEvent(ctx, topic, event, func() {})
	require.NoError(t, err)

	messages := encoder.Build()
	require.Len(t, messages, 1)
	message := messages[0]

	schemaM, err := NewConfluentSchemaManager(ctx, "http://127.0.0.1:8081", nil)
	require.NoError(t, err)

	tz, err := util.GetLocalTimezone()
	require.NoError(t, err)
	decoder := NewDecoder(config, schemaM, topic, tz)
	err = decoder.AddKeyValue(message.Key, message.Value)
	require.NoError(t, err)

	messageType, exist, err := decoder.HasNext()
	require.NoError(t, err)
	require.True(t, exist)
	require.Equal(t, model.MessageTypeRow, messageType)

	decodedEvent, err := decoder.NextRowChangedEvent()
	require.NoError(t, err)
	require.NotNil(t, decodedEvent)
}

func TestDecodeDDLEvent(t *testing.T) {
	t.Parallel()

	config := &common.Config{
		EnableTiDBExtension: true,
		AvroEnableWatermark: true,
	}

	encoder := &BatchEncoder{
		namespace: model.DefaultNamespace,
		result:    make([]*common.Message, 0, 1),
		config:    config,
	}

	message, err := encoder.EncodeDDLEvent(&model.DDLEvent{
		StartTs:  1020,
		CommitTs: 1030,
		TableInfo: &model.TableInfo{
			TableName: model.TableName{
				Schema:      "test",
				Table:       "t1",
				TableID:     0,
				IsPartition: false,
			},
		},
		Type:  timodel.ActionAddColumn,
		Query: "ALTER TABLE test.t1 ADD COLUMN a int",
	})
	require.NoError(t, err)
	require.NotNil(t, message)

	topic := "test-topic"
	tz, err := util.GetLocalTimezone()
	require.NoError(t, err)
	decoder := NewDecoder(config, nil, topic, tz)
	err = decoder.AddKeyValue(message.Key, message.Value)
	require.NoError(t, err)

	messageType, exist, err := decoder.HasNext()
	require.NoError(t, err)
	require.True(t, exist)
	require.Equal(t, model.MessageTypeDDL, messageType)

	decodedEvent, err := decoder.NextDDLEvent()
	require.NoError(t, err)
	require.NotNil(t, decodedEvent)
	require.Equal(t, uint64(1030), decodedEvent.CommitTs)
	require.Equal(t, timodel.ActionAddColumn, decodedEvent.Type)
	require.Equal(t, "ALTER TABLE test.t1 ADD COLUMN a int", decodedEvent.Query)
	require.Equal(t, "test", decodedEvent.TableInfo.TableName.Schema)
	require.Equal(t, "t1", decodedEvent.TableInfo.TableName.Table)
	require.Equal(t, int64(0), decodedEvent.TableInfo.TableName.TableID)
	require.False(t, decodedEvent.TableInfo.TableName.IsPartition)
}

func TestDecodeResolvedEvent(t *testing.T) {
	t.Parallel()

	config := &common.Config{
		EnableTiDBExtension: true,
		AvroEnableWatermark: true,
	}

	encoder := &BatchEncoder{
		namespace: model.DefaultNamespace,
		config:    config,
		result:    make([]*common.Message, 0, 1),
	}

	resolvedTs := uint64(1591943372224)
	message, err := encoder.EncodeCheckpointEvent(resolvedTs)
	require.NoError(t, err)
	require.NotNil(t, message)

	topic := "test-topic"
	tz, err := util.GetLocalTimezone()
	require.NoError(t, err)
	decoder := NewDecoder(config, nil, topic, tz)
	err = decoder.AddKeyValue(message.Key, message.Value)
	require.NoError(t, err)

	messageType, exist, err := decoder.HasNext()
	require.NoError(t, err)
	require.True(t, exist)
	require.Equal(t, model.MessageTypeResolved, messageType)

	obtained, err := decoder.NextResolvedEvent()
	require.NoError(t, err)
	require.Equal(t, resolvedTs, obtained)
}
