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

<<<<<<< HEAD:cdc/sinkv2/ddlsink/mq/ddlproducer/kafka_ddl_producer_test.go
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
=======
func getOptions() *kafka.Options {
	options := kafka.NewOptions()
	options.Version = "0.9.0.0"
	options.ClientID = "test-client"
	options.PartitionNum = int32(kafka.DefaultMockPartitionNum)
	options.AutoCreate = false
	options.BrokerEndpoints = []string{"127.0.0.1:9092"}
>>>>>>> 4bc1e73180 (kafka(ticdc): use sarama mock producer in the unit test to workaround the data race (#9356)):cdc/sink/ddlsink/mq/ddlproducer/kafka_ddl_producer_test.go

	return config
}

func TestSyncBroadcastMessage(t *testing.T) {
<<<<<<< HEAD:cdc/sinkv2/ddlsink/mq/ddlproducer/kafka_ddl_producer_test.go
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
=======
	ctx, cancel := context.WithCancel(context.Background())
	options := getOptions()
	options.MaxMessages = 1

	ctx = context.WithValue(ctx, "testing.T", t)
	changefeed := model.DefaultChangeFeedID("changefeed-test")
	factory, err := kafka.NewMockFactory(options, changefeed)
	require.NoError(t, err)

	syncProducer, err := factory.SyncProducer(ctx)
	require.NoError(t, err)
>>>>>>> 4bc1e73180 (kafka(ticdc): use sarama mock producer in the unit test to workaround the data race (#9356)):cdc/sink/ddlsink/mq/ddlproducer/kafka_ddl_producer_test.go

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
<<<<<<< HEAD:cdc/sinkv2/ddlsink/mq/ddlproducer/kafka_ddl_producer_test.go
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
=======
	ctx, cancel := context.WithCancel(context.Background())
	options := getOptions()

	ctx = context.WithValue(ctx, "testing.T", t)
	changefeed := model.DefaultChangeFeedID("changefeed-test")
	factory, err := kafka.NewMockFactory(options, changefeed)
	require.NoError(t, err)

	syncProducer, err := factory.SyncProducer(ctx)
	require.NoError(t, err)

	p, err := NewKafkaDDLProducer(ctx, changefeed, syncProducer)
	require.NoError(t, err)

	syncProducer.(*kafka.MockSaramaSyncProducer).Producer.ExpectSendMessageAndSucceed()
	err = p.SyncSendMessage(ctx, kafka.DefaultMockTopicName, 0, &common.Message{Ts: 417318403368288260})
	require.NoError(t, err)
>>>>>>> 4bc1e73180 (kafka(ticdc): use sarama mock producer in the unit test to workaround the data race (#9356)):cdc/sink/ddlsink/mq/ddlproducer/kafka_ddl_producer_test.go

	p.Close()
	err = p.SyncSendMessage(ctx, kafka.DefaultMockTopicName, 0, &common.Message{Ts: 417318403368288260})
	require.ErrorIs(t, err, cerror.ErrKafkaProducerClosed)
	cancel()
}

func TestProducerSendMsgFailed(t *testing.T) {
<<<<<<< HEAD:cdc/sinkv2/ddlsink/mq/ddlproducer/kafka_ddl_producer_test.go
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
=======
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	options := getOptions()
	options.MaxMessages = 1
	options.MaxMessageBytes = 1

	ctx = context.WithValue(ctx, "testing.T", t)

>>>>>>> 4bc1e73180 (kafka(ticdc): use sarama mock producer in the unit test to workaround the data race (#9356)):cdc/sink/ddlsink/mq/ddlproducer/kafka_ddl_producer_test.go
	// This will make the first send failed.
	saramaConfig.Producer.MaxMessageBytes = 1
	client, err := sarama.NewClient(config.BrokerEndpoints, saramaConfig)
	require.Nil(t, err)

<<<<<<< HEAD:cdc/sinkv2/ddlsink/mq/ddlproducer/kafka_ddl_producer_test.go
	adminClient, err := kafka.NewMockAdminClient(config.BrokerEndpoints, saramaConfig)
	require.Nil(t, err)
	p, err := NewKafkaDDLProducer(ctx, client, adminClient)
	require.Nil(t, err)
=======
	syncProducer, err := factory.SyncProducer(ctx)
	require.NoError(t, err)

	p, err := NewKafkaDDLProducer(ctx, changefeed, syncProducer)
	require.NoError(t, err)

>>>>>>> 4bc1e73180 (kafka(ticdc): use sarama mock producer in the unit test to workaround the data race (#9356)):cdc/sink/ddlsink/mq/ddlproducer/kafka_ddl_producer_test.go
	defer p.Close()

	syncProducer.(*kafka.MockSaramaSyncProducer).Producer.ExpectSendMessageAndFail(sarama.ErrMessageTooLarge)
	err = p.SyncSendMessage(ctx, kafka.DefaultMockTopicName, 0, &common.Message{Ts: 417318403368288260})
	require.ErrorIs(t, err, sarama.ErrMessageTooLarge)
}

func TestProducerDoubleClose(t *testing.T) {
<<<<<<< HEAD:cdc/sinkv2/ddlsink/mq/ddlproducer/kafka_ddl_producer_test.go
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
=======
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	options := getOptions()

	ctx = context.WithValue(ctx, "testing.T", t)
	changefeed := model.DefaultChangeFeedID("changefeed-test")
	factory, err := kafka.NewMockFactory(options, changefeed)
	require.NoError(t, err)

	syncProducer, err := factory.SyncProducer(ctx)
	require.NoError(t, err)

	p, err := NewKafkaDDLProducer(ctx, changefeed, syncProducer)
	require.NoError(t, err)
>>>>>>> 4bc1e73180 (kafka(ticdc): use sarama mock producer in the unit test to workaround the data race (#9356)):cdc/sink/ddlsink/mq/ddlproducer/kafka_ddl_producer_test.go

	p.Close()
	p.Close()
}
