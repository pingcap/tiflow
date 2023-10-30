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

package util

import (
	"context"
	"net/url"
	"strings"

	"github.com/pingcap/tiflow/cdc/sink/dmlsink/mq/manager"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/sink/kafka"
)

// GetTopic returns the topic name from the sink URI.
func GetTopic(sinkURI *url.URL) (string, error) {
	topic := strings.TrimFunc(sinkURI.Path, func(r rune) bool {
		return r == '/'
	})
	if topic == "" {
		return "", cerror.ErrKafkaInvalidConfig.GenWithStack("no topic is specified in sink-uri")
	}
	return topic, nil
}

// GetProtocol returns the protocol from the sink URI.
func GetProtocol(protocolStr string) (config.Protocol, error) {
	protocol, err := config.ParseSinkProtocolFromString(protocolStr)
	if err != nil {
		return protocol, cerror.WrapError(cerror.ErrKafkaInvalidConfig, err)
	}

	return protocol, nil
}

// GetFileExtension returns the extension for specific protocol
func GetFileExtension(protocol config.Protocol) string {
	switch protocol {
	case config.ProtocolAvro, config.ProtocolCanalJSON, config.ProtocolMaxwell,
		config.ProtocolOpen:
		return ".json"
	case config.ProtocolCraft:
		return ".craft"
	case config.ProtocolCanal:
		return ".canal"
	case config.ProtocolCsv:
		return ".csv"
	default:
		return ".unknown"
	}
}

// GetEncoderConfig returns the encoder config and validates the config.
func GetEncoderConfig(
	sinkURI *url.URL,
	protocol config.Protocol,
	replicaConfig *config.ReplicaConfig,
	maxMsgBytes int,
) (*common.Config, error) {
	encoderConfig := common.NewConfig(protocol)
	if err := encoderConfig.Apply(sinkURI, replicaConfig); err != nil {
		return nil, cerror.WrapError(cerror.ErrSinkInvalidConfig, err)
	}
	// Always set encoder's `MaxMessageBytes` equal to producer's `MaxMessageBytes`
	// to prevent that the encoder generate batched message too large
	// then cause producer meet `message too large`.
	encoderConfig = encoderConfig.WithMaxMessageBytes(maxMsgBytes)

	if err := encoderConfig.Validate(); err != nil {
		return nil, cerror.WrapError(cerror.ErrSinkInvalidConfig, err)
	}

	return encoderConfig, nil
}

// GetTopicManagerAndTryCreateTopic returns the topic manager and try to create the topic.
func GetTopicManagerAndTryCreateTopic(
	ctx context.Context,
	topic string,
	topicCfg *kafka.AutoCreateTopicConfig,
	adminClient kafka.ClusterAdminClient,
) (manager.TopicManager, error) {
	topicManager := manager.NewKafkaTopicManager(
		ctx, topic, adminClient, topicCfg,
	)

	if _, err := topicManager.CreateTopicAndWaitUntilVisible(ctx, topic); err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaCreateTopic, err)
	}

	return topicManager, nil
}
