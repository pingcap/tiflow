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

	"github.com/Shopify/sarama"
	"github.com/pingcap/tiflow/cdc/sink/codec/common"
	kafkav1 "github.com/pingcap/tiflow/cdc/sink/mq/producer/kafka"
	cerror "github.com/pingcap/tiflow/pkg/errors"
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

	return config
}

func TestSyncBroadcastMessage(t *testing.T) {
	t.Skip("skip because of race introduced by #9026")
	t.Parallel()

	leader, topic := initBroker(t, kafka.DefaultMockPartitionNum)
	defer leader.Close()

	ctx, cancel := context.WithCancel(context.Background())
	config := getConfig(leader.Addr())
	saramaConfig, err := kafkav1.NewSaramaConfig(context.Background(), config)
	require.Nil(t, err)
	saramaConfig.Producer.Flush.MaxMessages = 1

	client, err := sarama.NewClient(config.BrokerEndpoints, saramaConfig)
	require.Nil(t, err)
	adminClient, err := kafka.NewMockAdminClient(config.BrokerEndpoints, saramaConfig)
	require.Nil(t, err)
	p, err := NewKafkaDDLProducer(ctx, client, adminClient)
	require.Nil(t, err)

	p, err := NewKafkaDDLProducer(ctx, changefeed, syncProducer)
	require.NoError(t, err)

	for i := 0; i < kafka.DefaultMockPartitionNum; i++ {
		syncProducer.(*kafka.MockSaramaSyncProducer).Producer.ExpectSendMessageAndSucceed()
	}
	err = p.SyncBroadcastMessage(ctx, kafka.DefaultMockTopicName,
		kafka.DefaultMockPartitionNum, &common.Message{Ts: 417318403368288260})
	require.Nil(t, err)

	p.Close()
	err = p.SyncBroadcastMessage(ctx, kafka.DefaultMockTopicName,
		kafka.DefaultMockPartitionNum, &common.Message{Ts: 417318403368288260})
	require.ErrorIs(t, err, cerror.ErrKafkaProducerClosed)
	cancel()
}

func TestSyncSendMessage(t *testing.T) {
	t.Skip("skip because of race introduced by #9026")
	t.Parallel()

	leader, topic := initBroker(t, 1)
	defer leader.Close()

	ctx, cancel := context.WithCancel(context.Background())
	config := getConfig(leader.Addr())
	saramaConfig, err := kafkav1.NewSaramaConfig(context.Background(), config)
	require.Nil(t, err)
	saramaConfig.Producer.Flush.MaxMessages = 1

	client, err := sarama.NewClient(config.BrokerEndpoints, saramaConfig)
	require.Nil(t, err)
	adminClient, err := kafka.NewMockAdminClient(config.BrokerEndpoints, saramaConfig)
	require.Nil(t, err)
	p, err := NewKafkaDDLProducer(ctx, client, adminClient)
	require.Nil(t, err)

	err = p.SyncSendMessage(ctx, topic, 0, &common.Message{Ts: 417318403368288260})
	require.Nil(t, err)

	p.Close()
	err = p.SyncSendMessage(ctx, kafka.DefaultMockTopicName, 0, &common.Message{Ts: 417318403368288260})
	require.ErrorIs(t, err, cerror.ErrKafkaProducerClosed)
	cancel()
}

func TestProducerSendMsgFailed(t *testing.T) {
	t.Skip("skip because of race introduced by #9026")
	t.Parallel()

	leader, topic := initBroker(t, 0)
	defer leader.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	config := getConfig(leader.Addr())
	saramaConfig, err := kafkav1.NewSaramaConfig(context.Background(), config)
	require.Nil(t, err)
	saramaConfig.Producer.Flush.MaxMessages = 1
	saramaConfig.Producer.Retry.Max = 1
	// This will make the first send failed.
	saramaConfig.Producer.MaxMessageBytes = 1
	client, err := sarama.NewClient(config.BrokerEndpoints, saramaConfig)
	require.Nil(t, err)

	adminClient, err := kafka.NewMockAdminClient(config.BrokerEndpoints, saramaConfig)
	require.Nil(t, err)
	p, err := NewKafkaDDLProducer(ctx, client, adminClient)
	require.Nil(t, err)
	defer p.Close()

	syncProducer.(*kafka.MockSaramaSyncProducer).Producer.ExpectSendMessageAndFail(sarama.ErrMessageTooLarge)
	err = p.SyncSendMessage(ctx, kafka.DefaultMockTopicName, 0, &common.Message{Ts: 417318403368288260})
	require.ErrorIs(t, err, sarama.ErrMessageTooLarge)
}

func TestProducerDoubleClose(t *testing.T) {
	t.Skip("skip because of race introduced by #9026")
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	config := getConfig(leader.Addr())
	saramaConfig, err := kafkav1.NewSaramaConfig(context.Background(), config)
	require.Nil(t, err)
	saramaConfig.Producer.Flush.MaxMessages = 1

	client, err := sarama.NewClient(config.BrokerEndpoints, saramaConfig)
	require.Nil(t, err)
	adminClient, err := kafka.NewMockAdminClient(config.BrokerEndpoints, saramaConfig)
	require.Nil(t, err)
	p, err := NewKafkaDDLProducer(ctx, client, adminClient)
	require.Nil(t, err)

	p.Close()
	p.Close()
}
