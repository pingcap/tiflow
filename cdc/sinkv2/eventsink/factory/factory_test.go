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
// limitations under the License

package factory

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink/mq"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink/mq/dmlproducer"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink"
	"github.com/pingcap/tiflow/pkg/sink/kafka"
	"github.com/prometheus/client_golang/prometheus"
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

func newForTest(ctx context.Context,
	sinkURIStr string,
	cfg *config.ReplicaConfig,
	errCh chan error,
) (*SinkFactory, error) {
	sinkURI, err := config.GetSinkURIAndAdjustConfigWithSinkURI(sinkURIStr, cfg)
	if err != nil {
		return nil, err
	}

	s := &SinkFactory{}
	schema := strings.ToLower(sinkURI.Scheme)
	switch schema {
	case "kafka", "kafka+ssl":
		mqs, err := mq.NewKafkaDMLSink(ctx, sinkURI, cfg, errCh,
			// Use mock kafka clients for test.
			kafka.NewMockAdminClient, dmlproducer.NewDMLMockProducer)
		if err != nil {
			return nil, err
		}
		s.txnSink = mqs
		s.sinkType = sink.TxnSink
	default:
		return nil,
			cerror.ErrSinkURIInvalid.GenWithStack("the sink scheme (%s) is not supported", schema)
	}
	return s, nil
}

func TestSinkFactory(t *testing.T) {
	t.Parallel()

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

	sinkFactory, err := newForTest(ctx, uri, replicaConfig, errCh)
	require.NotNil(t, sinkFactory)
	require.Nil(t, err)
	require.Equal(t, sink.TxnSink, sinkFactory.sinkType)
	require.NotNil(t, sinkFactory.txnSink)

	tableSink := sinkFactory.CreateTableSink(model.DefaultChangeFeedID("1"),
		1, 0, prometheus.NewCounter(prometheus.CounterOpts{}))
	require.NotNil(t, tableSink, "table sink can be created")

	sinkFactory.Close()
}
