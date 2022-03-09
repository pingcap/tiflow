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
	kafkaconfig "github.com/pingcap/tiflow/cdc/sink/producer/kafka"
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
func NewTopicManager(client kafka.Client, admin kafka.ClusterAdminClient, cfg *kafkaconfig.AutoCreateTopicConfig) *TopicManager {
	return &TopicManager{
		client: client,
		admin:  admin,
		cfg:    cfg,
	}
}

// Partitions returns the number of partitions of the topic.
// It may also try to update the topics' information maintained by manager.
func (m *TopicManager) Partitions(topic string) (int32, error) {
	err := m.tryRefreshMeta()
	if err != nil {
		return 0, errors.Trace(err)
	}

	if partitions, ok := m.topics.Load(topic); ok {
		return partitions.(int32), nil
	}

	return m.cfg.PartitionNum, m.CreateTopic(topic)
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
			m.topics.Store(topic, int32(len(partitions)))
		}
		m.lastMetadataRefresh.Store(time.Now().Unix())
	}

	return nil
}

// CreateTopic creates a topic with the given name.
func (m *TopicManager) CreateTopic(topicName string) error {
	topics, err := m.admin.ListTopics()
	if err != nil {
		return errors.Trace(err)
	}

	// Now that we have access to the latest topics' information,
	// we need to update it here immediately.
	for topic, detail := range topics {
		m.topics.Store(topic, detail.NumPartitions)
	}
	m.lastMetadataRefresh.Store(time.Now().Unix())

	// Maybe our cache has expired information, so we just return it.
	if _, ok := topics[topicName]; ok {
		return nil
	}

	if !m.cfg.AutoCreate {
		return cerror.ErrKafkaInvalidConfig.GenWithStack(
			fmt.Sprintf("`auto-create-topic` is false, and %s not found", topicName))
	}

	err = m.admin.CreateTopic(topicName, &sarama.TopicDetail{
		NumPartitions:     m.cfg.PartitionNum,
		ReplicationFactor: m.cfg.ReplicationFactor,
	}, false)
	// Ignore topic already exists error.
	if err != nil && errors.Cause(err) != sarama.ErrTopicAlreadyExists {
		return cerror.WrapError(cerror.ErrKafkaNewSaramaProducer, err)
	}
	m.topics.Store(topicName, m.cfg.PartitionNum)

	log.Info("TiCDC create the topic",
		zap.Int32("partition-num", m.cfg.PartitionNum),
		zap.Int16("replication-factor", m.cfg.ReplicationFactor))

	return nil
}
