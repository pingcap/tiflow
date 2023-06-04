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
	"strings"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pingcap/tiflow/cdc/sink/codec/common"
	kafkav1 "github.com/pingcap/tiflow/cdc/sink/mq/producer/kafka"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/kafka"
	"github.com/stretchr/testify/require"
)

//nolint:unused
func initBroker(t *testing.T, withPartitionResponse int) (*sarama.MockBroker, string) {
	topic := kafka.DefaultMockTopicName
	leader := sarama.NewMockBroker(t, 2)
	metadataResponse := new(sarama.MetadataResponse)
	metadataResponse.AddBroker(leader.Addr(), leader.BrokerID())
	metadataResponse.AddTopicPartition(topic, 0,
		leader.BrokerID(), nil, nil, nil, sarama.ErrNoError)
	metadataResponse.AddTopicPartition(topic, 1,
		leader.BrokerID(), nil, nil, nil, sarama.ErrNoError)
	metadataResponse.AddTopicPartition(topic, 2,
		leader.BrokerID(), nil, nil, nil, sarama.ErrNoError)
	// Response for `sarama.NewClient`
	leader.Returns(metadataResponse)

	prodSuccess := new(sarama.ProduceResponse)
	for i := 0; i < withPartitionResponse; i++ {
		prodSuccess.AddTopicPartition(topic, int32(i), sarama.ErrNoError)
	}
	for i := 0; i < withPartitionResponse; i++ {
		leader.Returns(prodSuccess)
	}

	return leader, topic
}

//nolint:unused
func getConfig(addr string) *kafkav1.Config {
	config := kafkav1.NewConfig()
	// Because the sarama mock broker is not compatible with version larger than 1.0.0.
	// We use a smaller version in the following producer tests.
	// Ref: https://github.com/Shopify/sarama/blob/89707055369768913defac
	// 030c15cf08e9e57925/async_producer_test.go#L1445-L1447
	config.Version = "0.9.0.0"
	config.PartitionNum = int32(kafka.DefaultMockPartitionNum)
	config.AutoCreate = false
	config.BrokerEndpoints = strings.Split(addr, ",")

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

	err = p.SyncBroadcastMessage(ctx, topic,
		kafka.DefaultMockPartitionNum, &common.Message{Ts: 417318403368288260})
	require.Nil(t, err)

	p.Close()
	err = p.SyncBroadcastMessage(ctx, topic,
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
	err = p.SyncSendMessage(ctx, topic, 0, &common.Message{Ts: 417318403368288260})
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

	err = p.SyncSendMessage(ctx, topic, 0, &common.Message{Ts: 417318403368288260})
	require.Regexp(t, ".*too large.*", err)
}

func TestProducerDoubleClose(t *testing.T) {
	t.Skip("skip because of race introduced by #9026")
	t.Parallel()

	leader, _ := initBroker(t, 0)
	defer leader.Close()

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
