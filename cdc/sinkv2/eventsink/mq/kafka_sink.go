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
	"strings"

	"github.com/Shopify/sarama"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/sink/mq/codec"
	"github.com/pingcap/tiflow/cdc/sink/mq/dispatcher"
	"github.com/pingcap/tiflow/cdc/sink/mq/manager"
	"github.com/pingcap/tiflow/cdc/sink/mq/producer/kafka"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink/mq/producer"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	pkafka "github.com/pingcap/tiflow/pkg/kafka"
	"go.uber.org/zap"
)

// NewKafkaSink will verify the config and create a KafkaSink.
func NewKafkaSink(
	ctx context.Context,
	sinkURI *url.URL,
	replicaConfig *config.ReplicaConfig,
	errCh chan error,
	adminClientCreator pkafka.ClusterAdminClientCreator,
	producerCreator producer.Factory,
) (*sink, error) {
	topic, err := getTopic(sinkURI)
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
	defer func() {
		if err != nil {
			_ = adminClient.Close()
		}
	}()
	if err := kafka.AdjustConfig(adminClient, baseConfig, saramaConfig, topic); err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaNewSaramaProducer, err)
	}

	protocol, err := getProtocol(replicaConfig.Sink.Protocol)
	if err != nil {
		return nil, errors.Trace(err)
	}

	client, err := sarama.NewClient(baseConfig.BrokerEndpoints, saramaConfig)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaNewSaramaProducer, err)
	}

	log.Info("Try to create a producer",
		zap.Any("baseConfig", baseConfig))
	p, err := producerCreator(ctx, client, errCh)
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

	topicManager, err := getTopicManagerAndTryCreateTopic(
		baseConfig.BrokerEndpoints, topic,
		baseConfig.DeriveTopicConfig(),
		adminClient,
		saramaConfig,
	)
	if err != nil {
		return nil, errors.Trace(err)
	}

	eventRouter, err := dispatcher.NewEventRouter(replicaConfig, topic)
	if err != nil {
		return nil, errors.Trace(err)
	}

	encoderConfig, err := getEncoderConfig(sinkURI, protocol, replicaConfig,
		saramaConfig.Producer.MaxMessageBytes)
	if err != nil {
		return nil, errors.Trace(err)
	}

	s, err := newSink(ctx, p, topicManager, eventRouter, encoderConfig, errCh)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return s, nil
}

func getTopic(sinkURI *url.URL) (string, error) {
	topic := strings.TrimFunc(sinkURI.Path, func(r rune) bool {
		return r == '/'
	})
	if topic == "" {
		return "", cerror.ErrKafkaInvalidConfig.GenWithStack("no topic is specified in sink-uri")
	}
	return topic, nil
}

func getProtocol(protocolStr string) (config.Protocol, error) {
	var protocol config.Protocol
	if err := protocol.FromString(protocolStr); err != nil {
		return protocol, cerror.WrapError(cerror.ErrKafkaInvalidConfig, err)
	}

	return protocol, nil
}

func getEncoderConfig(
	sinkURI *url.URL,
	protocol config.Protocol,
	replicaConfig *config.ReplicaConfig,
	maxMsgBytes int,
) (*codec.Config, error) {
	encoderConfig := codec.NewConfig(protocol)
	if err := encoderConfig.Apply(sinkURI, replicaConfig); err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaInvalidConfig, err)
	}
	// Always set encoder's `MaxMessageBytes` equal to producer's `MaxMessageBytes`
	// to prevent that the encoder generate batched message too large
	// then cause producer meet `message too large`.
	encoderConfig = encoderConfig.WithMaxMessageBytes(maxMsgBytes)

	if err := encoderConfig.Validate(); err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaInvalidConfig, err)
	}

	return encoderConfig, nil
}

func getTopicManagerAndTryCreateTopic(
	endpoints []string,
	topic string,
	topicCfg *kafka.AutoCreateTopicConfig,
	adminClient pkafka.ClusterAdminClient,
	saramaConfig *sarama.Config,
) (manager.TopicManager, error) {
	client, err := sarama.NewClient(endpoints, saramaConfig)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaNewSaramaProducer, err)
	}

	topicManager, err := manager.NewKafkaTopicManager(
		client,
		adminClient,
		topicCfg,
	)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaNewSaramaProducer, err)
	}

	if _, err := topicManager.CreateTopicAndWaitUntilVisible(topic); err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaCreateTopic, err)
	}

	return topicManager, nil
}
