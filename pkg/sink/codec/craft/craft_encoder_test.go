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

package craft

import (
	"context"
	"testing"

	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/sink/codec/internal"
	"github.com/stretchr/testify/require"
)

func TestCraftMaxMessageBytes(t *testing.T) {
	cfg := common.NewConfig(config.ProtocolCraft).WithMaxMessageBytes(256)
	encoder := NewBatchEncoderBuilder(cfg).Build()

	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	_ = helper.DDL2Event(`create table test.t(a varchar(10) primary key)`)
	testEvent := helper.DML2Event(`insert into test.t values ("aa")`, "test", "t")

	for i := 0; i < 10000; i++ {
		err := encoder.AppendRowChangedEvent(context.Background(), "", testEvent, nil)
		require.Nil(t, err)
	}

	messages := encoder.Build()
	for _, msg := range messages {
		require.LessOrEqual(t, msg.Length(), 256)
	}
}

func TestCraftMaxBatchSize(t *testing.T) {
	cfg := common.NewConfig(config.ProtocolCraft).WithMaxMessageBytes(10485760)
	cfg.MaxBatchSize = 64
	encoder := NewBatchEncoderBuilder(cfg).Build()

	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	_ = helper.DDL2Event(`create table test.t(a varchar(10) primary key)`)
	testEvent := helper.DML2Event(`insert into test.t values ("aa")`, "test", "t")

	for i := 0; i < 10000; i++ {
		err := encoder.AppendRowChangedEvent(context.Background(), "", testEvent, nil)
		require.Nil(t, err)
	}

	messages := encoder.Build()
	sum := 0
	for _, msg := range messages {
		decoder, err := newBatchDecoder(nil, msg.Value)
		require.Nil(t, err)
		count := 0
		for {
			v, hasNext, err := decoder.HasNext()
			require.Nil(t, err)
			if !hasNext {
				break
			}

			require.Equal(t, model.MessageTypeRow, v)
			_, err = decoder.NextRowChangedEvent()
			require.NoError(t, err)
			count++
		}
		require.LessOrEqual(t, count, 64)
		sum += count
	}
	require.Equal(t, 10000, sum)
}

func TestBuildCraftBatchEncoder(t *testing.T) {
	t.Parallel()
	cfg := common.NewConfig(config.ProtocolCraft)

	builder := &batchEncoderBuilder{config: cfg}
	encoder, ok := builder.Build().(*BatchEncoder)
	require.True(t, ok)
	require.NotNil(t, encoder.config)
}

func TestDefaultCraftBatchCodec(t *testing.T) {
	cfg := common.NewConfig(config.ProtocolCraft).WithMaxMessageBytes(8192)
	cfg.MaxBatchSize = 64
	builder := NewBatchEncoderBuilder(cfg)
	internal.TestBatchCodec(t, builder, newBatchDecoder)
}

func TestCraftAppendRowChangedEventWithCallback(t *testing.T) {
	cfg := common.NewConfig(config.ProtocolCraft).WithMaxMessageBytes(10485760)
	cfg.MaxBatchSize = 2
	encoder := NewBatchEncoderBuilder(cfg).Build()
	require.NotNil(t, encoder)

	count := 0

	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	_ = helper.DDL2Event(`create table test.t(a varchar(10) primary key)`)
	row := helper.DML2Event(`insert into test.t values ("aa")`, "test", "t")

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
	require.Len(t, msgs, 3, "expected 3 messages")
	msgs[0].Callback()
	require.Equal(t, 3, count, "expected 2 callbacks to be called")
	msgs[1].Callback()
	require.Equal(t, 10, count, "expected 2 callbacks to be called")
	msgs[2].Callback()
	require.Equal(t, 15, count, "expected one callback to be called")
}
