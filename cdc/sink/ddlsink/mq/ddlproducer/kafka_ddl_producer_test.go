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

package ddlproducer

import (
	"context"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/sink/kafka"
	"github.com/stretchr/testify/require"
)

func getOptions() *kafka.Options {
	options := kafka.NewOptions()
	options.Version = "0.9.0.0"
	options.ClientID = "test-client"
	options.PartitionNum = int32(kafka.DefaultMockPartitionNum)
	options.AutoCreate = false
	options.BrokerEndpoints = []string{"127.0.0.1:9092"}

	return options
}

func TestSyncBroadcastMessage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	options := getOptions()
	options.MaxMessages = 1

	ctx = context.WithValue(ctx, "testing.T", t)
	changefeed := model.DefaultChangeFeedID("changefeed-test")
	factory, err := kafka.NewMockFactory(options, changefeed)
	require.NoError(t, err)

	syncProducer, err := factory.SyncProducer(ctx)
	require.NoError(t, err)

	p := NewKafkaDDLProducer(ctx, changefeed, syncProducer)

	for i := 0; i < kafka.DefaultMockPartitionNum; i++ {
		syncProducer.(*kafka.MockSaramaSyncProducer).Producer.ExpectSendMessageAndSucceed()
	}
	err = p.SyncBroadcastMessage(ctx, kafka.DefaultMockTopicName,
		kafka.DefaultMockPartitionNum, &common.Message{Ts: 417318403368288260})
	require.NoError(t, err)

	p.Close()
	err = p.SyncBroadcastMessage(ctx, kafka.DefaultMockTopicName,
		kafka.DefaultMockPartitionNum, &common.Message{Ts: 417318403368288260})
	require.ErrorIs(t, err, cerror.ErrKafkaProducerClosed)
	cancel()
}

func TestSyncSendMessage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	options := getOptions()

	ctx = context.WithValue(ctx, "testing.T", t)
	changefeed := model.DefaultChangeFeedID("changefeed-test")
	factory, err := kafka.NewMockFactory(options, changefeed)
	require.NoError(t, err)

	syncProducer, err := factory.SyncProducer(ctx)
	require.NoError(t, err)

	p := NewKafkaDDLProducer(ctx, changefeed, syncProducer)

	syncProducer.(*kafka.MockSaramaSyncProducer).Producer.ExpectSendMessageAndSucceed()
	err = p.SyncSendMessage(ctx, kafka.DefaultMockTopicName, 0, &common.Message{Ts: 417318403368288260})
	require.NoError(t, err)

	p.Close()
	err = p.SyncSendMessage(ctx, kafka.DefaultMockTopicName, 0, &common.Message{Ts: 417318403368288260})
	require.ErrorIs(t, err, cerror.ErrKafkaProducerClosed)
	cancel()
}

func TestProducerSendMsgFailed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	options := getOptions()
	options.MaxMessages = 1
	options.MaxMessageBytes = 1

	ctx = context.WithValue(ctx, "testing.T", t)

	// This will make the first send failed.
	changefeed := model.DefaultChangeFeedID("changefeed-test")
	factory, err := kafka.NewMockFactory(options, changefeed)
	require.NoError(t, err)

	syncProducer, err := factory.SyncProducer(ctx)
	require.NoError(t, err)

	p := NewKafkaDDLProducer(ctx, changefeed, syncProducer)
	defer p.Close()

	syncProducer.(*kafka.MockSaramaSyncProducer).Producer.ExpectSendMessageAndFail(sarama.ErrMessageTooLarge)
	err = p.SyncSendMessage(ctx, kafka.DefaultMockTopicName, 0, &common.Message{Ts: 417318403368288260})
	require.ErrorIs(t, err, sarama.ErrMessageTooLarge)
}

func TestProducerDoubleClose(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	options := getOptions()

	ctx = context.WithValue(ctx, "testing.T", t)
	changefeed := model.DefaultChangeFeedID("changefeed-test")
	factory, err := kafka.NewMockFactory(options, changefeed)
	require.NoError(t, err)

	syncProducer, err := factory.SyncProducer(ctx)
	require.NoError(t, err)

	p := NewKafkaDDLProducer(ctx, changefeed, syncProducer)

	p.Close()
	p.Close()
}
