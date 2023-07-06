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
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/sink/kafka"
	"github.com/stretchr/testify/require"
)

func initBroker(t *testing.T) (*sarama.MockBroker, string) {
	topic := kafka.DefaultMockTopicName
	leader := sarama.NewMockBroker(t, 2)
	return leader, topic
}

func getOptions(addr string) *kafka.Options {
	options := kafka.NewOptions()
	// Because the sarama mock broker is not compatible with version larger than 1.0.0.
	// We use a smaller version in the following producer tests.
	// Ref: https://github.com/Shopify/sarama/blob/89707055369768913defac
	// 030c15cf08e9e57925/async_producer_test.go#L1445-L1447
	options.Version = "0.9.0.0"
	options.ClientID = "test-client"
	options.PartitionNum = int32(kafka.DefaultMockPartitionNum)
	options.AutoCreate = false
	options.BrokerEndpoints = strings.Split(addr, ",")

	return options
}

func TestSyncBroadcastMessage(t *testing.T) {
	leader, topic := initBroker(t)
	defer leader.Close()

	ctx, cancel := context.WithCancel(context.Background())
	options := getOptions(leader.Addr())
	options.MaxMessages = 1

	changefeed := model.DefaultChangeFeedID("changefeed-test")
	factory, err := kafka.NewMockFactory(t, options, changefeed)
	require.NoError(t, err)

	syncProducer, err := factory.SyncProducer(ctx)
	require.NoError(t, err)

	p, err := NewKafkaDDLProducer(ctx, changefeed, syncProducer)
	require.NoError(t, err)

	for i := 0; i < kafka.DefaultMockPartitionNum; i++ {
		syncProducer.(*kafka.MockSaramaSyncProducer).Producer.ExpectSendMessageAndSucceed()
	}
	err = p.SyncBroadcastMessage(ctx, topic,
		kafka.DefaultMockPartitionNum, &common.Message{Ts: 417318403368288260})
	require.NoError(t, err)

	p.Close()
	err = p.SyncBroadcastMessage(ctx, topic,
		kafka.DefaultMockPartitionNum, &common.Message{Ts: 417318403368288260})
	require.ErrorIs(t, err, cerror.ErrKafkaProducerClosed)
	cancel()
}

func TestSyncSendMessage(t *testing.T) {
	leader, topic := initBroker(t)
	defer leader.Close()

	ctx, cancel := context.WithCancel(context.Background())
	options := getOptions(leader.Addr())

	changefeed := model.DefaultChangeFeedID("changefeed-test")
	factory, err := kafka.NewMockFactory(t, options, changefeed)
	require.NoError(t, err)

	syncProducer, err := factory.SyncProducer(ctx)
	require.NoError(t, err)

	p, err := NewKafkaDDLProducer(ctx, changefeed, syncProducer)
	require.NoError(t, err)

	syncProducer.(*kafka.MockSaramaSyncProducer).Producer.ExpectSendMessageAndSucceed()
	err = p.SyncSendMessage(ctx, topic, 0, &common.Message{Ts: 417318403368288260})
	require.NoError(t, err)

	p.Close()
	err = p.SyncSendMessage(ctx, topic, 0, &common.Message{Ts: 417318403368288260})
	require.ErrorIs(t, err, cerror.ErrKafkaProducerClosed)
	cancel()
}

func TestProducerSendMsgFailed(t *testing.T) {
	leader, topic := initBroker(t)
	defer leader.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	options := getOptions(leader.Addr())
	options.MaxMessages = 1
	options.MaxMessageBytes = 1

	// This will make the first send failed.
	changefeed := model.DefaultChangeFeedID("changefeed-test")
	factory, err := kafka.NewMockFactory(t, options, changefeed)
	require.NoError(t, err)

	syncProducer, err := factory.SyncProducer(ctx)
	require.NoError(t, err)

	p, err := NewKafkaDDLProducer(ctx, changefeed, syncProducer)
	require.NoError(t, err)

	defer p.Close()

	syncProducer.(*kafka.MockSaramaSyncProducer).Producer.ExpectSendMessageAndFail(sarama.ErrMessageTooLarge)
	err = p.SyncSendMessage(ctx, topic, 0, &common.Message{Ts: 417318403368288260})
	require.ErrorIs(t, err, sarama.ErrMessageTooLarge)
}

func TestProducerDoubleClose(t *testing.T) {
	leader, _ := initBroker(t)
	defer leader.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	options := getOptions(leader.Addr())

	changefeed := model.DefaultChangeFeedID("changefeed-test")
	factory, err := kafka.NewMockFactory(t, options, changefeed)
	require.NoError(t, err)

	syncProducer, err := factory.SyncProducer(ctx)
	require.NoError(t, err)

	p, err := NewKafkaDDLProducer(ctx, changefeed, syncProducer)
	require.NoError(t, err)

	p.Close()
	p.Close()
}
