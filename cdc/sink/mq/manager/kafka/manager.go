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

package kafka

import (
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	kafkaconfig "github.com/pingcap/tiflow/cdc/sink/mq/producer/kafka"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/kafka"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// TopicManager is a manager for kafka topics.
type TopicManager struct {
	client kafka.Client
	admin  kafka.ClusterAdminClient

	cfg *kafkaconfig.AutoCreateTopicConfig

	topics sync.Map

	lastMetadataRefresh atomic.Int64
}

// NewTopicManager creates a new topic manager.
func NewTopicManager(
	client kafka.Client,
	admin kafka.ClusterAdminClient,
	cfg *kafkaconfig.AutoCreateTopicConfig,
) *TopicManager {
	return &TopicManager{
		client: client,
		admin:  admin,
		cfg:    cfg,
	}
}

// GetPartitionNum returns the number of partitions of the topic.
// It may also try to update the topics' information maintained by manager.
func (m *TopicManager) GetPartitionNum(topic string) (int32, error) {
	err := m.tryRefreshMeta()
	if err != nil {
		return 0, errors.Trace(err)
	}

	if partitions, ok := m.topics.Load(topic); ok {
		return partitions.(int32), nil
	}

	return m.CreateTopic(topic)
}

// tryRefreshMeta try to refresh the topics' information maintained by manager.
func (m *TopicManager) tryRefreshMeta() error {
	if time.Since(time.Unix(m.lastMetadataRefresh.Load(), 0)) > time.Minute {
		topics, err := m.client.Topics()
		if err != nil {
			return err
		}

		for _, topic := range topics {
			partitions, err := m.client.Partitions(topic)
			if err != nil {
				return err
			}
			m.tryUpdatePartitionsAndLogging(topic, int32(len(partitions)))
		}
		m.lastMetadataRefresh.Store(time.Now().Unix())
	}

	return nil
}

// tryUpdatePartitionsAndLogging try to update the partitions of the topic.
func (m *TopicManager) tryUpdatePartitionsAndLogging(topic string, partitions int32) {
	oldPartitions, ok := m.topics.Load(topic)
	if ok {
		if oldPartitions.(int32) != partitions {
			m.topics.Store(topic, partitions)
			log.Info(
				"update topic partition number",
				zap.String("topic", topic),
				zap.Int32("oldPartitionNumber", oldPartitions.(int32)),
				zap.Int32("newPartitionNumber", partitions),
			)
		}
	} else {
		m.topics.Store(topic, partitions)
		log.Info(
			"store topic partition number",
			zap.String("topic", topic),
			zap.Int32("partitionNumber", partitions),
		)
	}
}

// CreateTopic creates a topic with the given name
// and returns the number of partitions.
func (m *TopicManager) CreateTopic(topicName string) (int32, error) {
	start := time.Now()
	topics, err := m.admin.ListTopics()
	if err != nil {
		log.Error(
			"Kafka admin client list topics failed",
			zap.Error(err),
			zap.Duration("duration", time.Since(start)),
		)
		return 0, errors.Trace(err)
	}
	log.Info(
		"Kafka admin client list topics success",
		zap.Duration("duration", time.Since(start)),
	)

	// Now that we have access to the latest topics' information,
	// we need to update it here immediately.
	for topic, detail := range topics {
		m.tryUpdatePartitionsAndLogging(topic, detail.NumPartitions)
	}
	m.lastMetadataRefresh.Store(time.Now().Unix())

	// Maybe our cache has expired information, so we just return it.
	if t, ok := topics[topicName]; ok {
		log.Info(
			"topic already exists and the cached information has expired",
			zap.String("topic", topicName),
		)
		return t.NumPartitions, nil
	}

	if !m.cfg.AutoCreate {
		return 0, cerror.ErrKafkaInvalidConfig.GenWithStack(
			fmt.Sprintf("`auto-create-topic` is false, "+
				"and %s not found", topicName))
	}

	start = time.Now()
	err = m.admin.CreateTopic(topicName, &sarama.TopicDetail{
		NumPartitions:     m.cfg.PartitionNum,
		ReplicationFactor: m.cfg.ReplicationFactor,
	}, false)
	// Ignore topic already exists error.
	if err != nil && errors.Cause(err) != sarama.ErrTopicAlreadyExists {
		log.Error(
			"Kafka admin client create the topic failed",
			zap.String("topic", topicName),
			zap.Int32("partitionNumber", m.cfg.PartitionNum),
			zap.Int16("replicationFactor", m.cfg.ReplicationFactor),
			zap.Error(err),
			zap.Duration("duration", time.Since(start)),
		)
		return 0, cerror.WrapError(cerror.ErrKafkaNewSaramaProducer, err)
	}
	log.Info(
		"Kafka admin client create the topic success",
		zap.String("topic", topicName),
		zap.Int32("partitionNumber", m.cfg.PartitionNum),
		zap.Int16("replicationFactor", m.cfg.ReplicationFactor),
		zap.Duration("duration", time.Since(start)),
	)
	m.tryUpdatePartitionsAndLogging(topicName, m.cfg.PartitionNum)

	return m.cfg.PartitionNum, nil
}
