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

package mq

import (
	"context"
	"fmt"
	"net/url"
	"testing"

	"github.com/Shopify/sarama"
	kafkav1 "github.com/pingcap/tiflow/cdc/sink/mq/producer/kafka"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/kafka"
	"github.com/stretchr/testify/require"
)

func initBroker(t *testing.T, partitionNum int) (*sarama.MockBroker, string) {
	topic := kafka.DefaultMockTopicName
	leader := sarama.NewMockBroker(t, 1)

	metadataResponse := sarama.NewMockMetadataResponse(t)
	metadataResponse.SetBroker(leader.Addr(), leader.BrokerID())
	for i := 0; i < partitionNum; i++ {
		metadataResponse.SetLeader(topic, int32(i), leader.BrokerID())
	}

	prodSuccess := sarama.NewMockProduceResponse(t)
	handlerMap := make(map[string]sarama.MockResponse)
	handlerMap["MetadataRequest"] = metadataResponse
	handlerMap["ProduceRequest"] = prodSuccess
	leader.SetHandlerByMap(handlerMap)

	return leader, topic
}

func TestWriteEvents(t *testing.T) {
	kafkav1.NewAdminClientImpl = kafka.NewMockAdminClient
	defer func() {
		kafkav1.NewAdminClientImpl = kafka.NewSaramaAdminClient
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	leader, topic := initBroker(t, kafka.DefaultMockPartitionNum)
	defer leader.Close()
	uriTemplate := "kafka://%s/%s?kafka-version=0.9.0.0&max-batch-size=1" +
		"&max-message-bytes=1048576&partition-num=1" +
		"&kafka-client-id=unit-test&auto-create-topic=false&compression=gzip&protocol=open-protocol"
	uri := fmt.Sprintf(uriTemplate, leader.Addr(), topic)

	sinkURI, err := url.Parse(uri)
	require.Nil(t, err)
	replicaConfig := config.GetDefaultReplicaConfig()
	require.Nil(t, replicaConfig.ValidateAndAdjust(sinkURI))
	errCh := make(chan error, 1)

	s, err := NewKafkaSink(ctx, sinkURI, replicaConfig, errCh)
	require.Nil(t, err)
	require.NotNil(t, s)
}
