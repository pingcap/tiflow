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
	"net/url"

	"github.com/Shopify/sarama"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/sink/mq/dispatcher"
	"github.com/pingcap/tiflow/cdc/sink/mq/producer/kafka"
	"github.com/pingcap/tiflow/cdc/sinkv2/ddlsink/mq/producer"
	mqutil "github.com/pingcap/tiflow/cdc/sinkv2/util/mq"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	pkafka "github.com/pingcap/tiflow/pkg/kafka"
	"go.uber.org/zap"
)

// NewKafkaDDLSink will verify the config and create a Kafka DDL Sink.
func NewKafkaDDLSink(
	ctx context.Context,
	sinkURI *url.URL,
	replicaConfig *config.ReplicaConfig,
	adminClientCreator pkafka.ClusterAdminClientCreator,
	producerCreator producer.Factory,
) (*ddlSink, error) {
	topic, err := mqutil.GetTopic(sinkURI)
	if err != nil {
		return nil, errors.Trace(err)
	}

	baseConfig := kafka.NewConfig()
	if err := baseConfig.Apply(sinkURI); err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaInvalidConfig, err)
	}
	saramaConfig, err := kafka.NewSaramaConfig(ctx, baseConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}

	adminClient, err := adminClientCreator(baseConfig.BrokerEndpoints, saramaConfig)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaNewSaramaProducer, err)
	}
	// we must close adminClient when this func return cause by an error
	// otherwise the adminClient will never be closed and lead to a goroutine leak
	// TODO: we should pass this adminClient to the producer to get the metrics.
	defer func() {
		_ = adminClient.Close()
	}()
	if err := kafka.AdjustConfig(adminClient, baseConfig, saramaConfig, topic); err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaNewSaramaProducer, err)
	}

	protocol, err := mqutil.GetProtocol(replicaConfig.Sink.Protocol)
	if err != nil {
		return nil, errors.Trace(err)
	}

	client, err := sarama.NewClient(baseConfig.BrokerEndpoints, saramaConfig)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaNewSaramaProducer, err)
	}

	log.Info("Try to create a DDL sink producer",
		zap.Any("baseConfig", baseConfig))
	p, err := producerCreator(ctx, client)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaNewSaramaProducer, err)
	}
	// Preventing leaks when error occurs.
	// This also closes the client in p.Close().
	defer func() {
		if err != nil {
			p.Close()
		}
	}()

	topicManager, err := mqutil.GetTopicManagerAndTryCreateTopic(
		topic,
		baseConfig.DeriveTopicConfig(),
		client,
		adminClient,
	)
	if err != nil {
		return nil, errors.Trace(err)
	}

	eventRouter, err := dispatcher.NewEventRouter(replicaConfig, topic)
	if err != nil {
		return nil, errors.Trace(err)
	}

	encoderConfig, err := mqutil.GetEncoderConfig(sinkURI, protocol, replicaConfig,
		saramaConfig.Producer.MaxMessageBytes)
	if err != nil {
		return nil, errors.Trace(err)
	}

	s, err := newDDLSink(ctx, p, topicManager, eventRouter, encoderConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return s, nil
}
