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

package manager

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/retry"
	"github.com/pingcap/tiflow/pkg/sink/kafka"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// kafkaTopicManager is a manager for kafka topics.
type kafkaTopicManager struct {
	client kafka.Client
	admin  kafka.ClusterAdminClient

	cfg *kafka.AutoCreateTopicConfig

	topics sync.Map

	lastMetadataRefresh atomic.Int64
}

// NewKafkaTopicManager creates a new topic manager.
func NewKafkaTopicManager(
	client kafka.Client,
	admin kafka.ClusterAdminClient,
	cfg *kafka.AutoCreateTopicConfig,
) (*kafkaTopicManager, error) {
	mgr := &kafkaTopicManager{
		client: client,
		admin:  admin,
		cfg:    cfg,
	}

	// do an initial metadata fetching using ListTopics
	err := mgr.listTopics()
	if err != nil {
		return nil, err
	}

	return mgr, nil
}

// GetPartitionNum returns the number of partitions of the topic.
// It may also try to update the topics' information maintained by manager.
func (m *kafkaTopicManager) GetPartitionNum(topic string) (int32, error) {
	err := m.tryRefreshMeta()
	if err != nil {
		return 0, errors.Trace(err)
	}

	if partitions, ok := m.topics.Load(topic); ok {
		return partitions.(int32), nil
	}

	partitionNum, err := m.CreateTopicAndWaitUntilVisible(topic)
	if err != nil {
		return 0, errors.Trace(err)
	}

	return partitionNum, nil
}

// tryRefreshMeta try to refresh the topics' information maintained by manager.
func (m *kafkaTopicManager) tryRefreshMeta() error {
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
func (m *kafkaTopicManager) tryUpdatePartitionsAndLogging(topic string, partitions int32) {
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

func (m *kafkaTopicManager) getMetadataOfTopics() (map[string]kafka.TopicDetail, error) {
	var topicList []string

	m.topics.Range(func(key, value any) bool {
		topic := key.(string)
		topicList = append(topicList, topic)

		return true
	})

	start := time.Now()
	// ignore the topic with error, return a subset of all topics.
	topicMetaList, err := m.admin.GetTopicsMeta(context.Background(), topicList, true)
	if err != nil {
		log.Warn(
			"Kafka admin client describe topics failed",
			zap.Error(err),
			zap.Duration("duration", time.Since(start)),
		)
		return nil, err
	}

	log.Info(
		"Kafka admin client describe topics success",
		zap.Duration("duration", time.Since(start)))

	return topicMetaList, nil
}

// waitUntilTopicVisible is called after CreateTopic to make sure the topic
// can be safely written to. The reason is that it may take several seconds after
// CreateTopic returns success for all the brokers to become aware that the
// topics have been created.
// See https://kafka.apache.org/23/javadoc/org/apache/kafka/clients/admin/AdminClient.html
func (m *kafkaTopicManager) waitUntilTopicVisible(topicName string) error {
	topics := []string{topicName}
	err := retry.Do(context.Background(), func() error {
		start := time.Now()
		meta, err := m.admin.GetTopicsMeta(context.Background(), topics, false)
		if err != nil {
			log.Warn(" topic not found, retry it",
				zap.Error(err),
				zap.Duration("duration", time.Since(start)),
			)
			return err
		}
		log.Info("topic found",
			zap.String("topic", topicName),
			zap.Int32("partitionNumber", meta[topicName].NumPartitions),
			zap.Duration("duration", time.Since(start)))
		return nil
	}, retry.WithBackoffBaseDelay(500),
		retry.WithBackoffMaxDelay(1000),
		retry.WithMaxTries(6),
	)

	return err
}

// listTopics is used to do an initial metadata fetching.
func (m *kafkaTopicManager) listTopics() error {
	start := time.Now()
	topics, err := m.admin.GetAllTopicsMeta(context.Background())
	if err != nil {
		log.Error(
			"Kafka admin client list topics failed",
			zap.Error(err),
			zap.Duration("duration", time.Since(start)),
		)
		return errors.Trace(err)
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

	return nil
}

// createTopic creates a topic with the given name
// and returns the number of partitions.
func (m *kafkaTopicManager) createTopic(topicName string) (int32, error) {
	topicMetaList, err := m.getMetadataOfTopics()
	if err != nil {
		return 0, errors.Trace(err)
	}

	// Now that we have access to the latest topics' information,
	// we need to update it here immediately.
	for topic, detail := range topicMetaList {
		m.tryUpdatePartitionsAndLogging(topic, detail.NumPartitions)
	}

	detail, targetTopicFound := topicMetaList[topicName]
	if targetTopicFound {
		log.Info(
			"topic already exists and the cached information has expired",
			zap.String("topic", topicName),
		)
		return detail.NumPartitions, nil
	}

	if !m.cfg.AutoCreate {
		return 0, cerror.ErrKafkaInvalidConfig.GenWithStack(
			fmt.Sprintf("`auto-create-topic` is false, "+
				"and %s not found", topicName))
	}

	start := time.Now()
	err = m.admin.CreateTopic(context.Background(), topicName, &kafka.TopicDetail{
		NumPartitions:     m.cfg.PartitionNum,
		ReplicationFactor: m.cfg.ReplicationFactor,
	}, false)
	if err != nil {
		log.Error(
			"Kafka admin client create the topic failed",
			zap.String("topic", topicName),
			zap.Int32("partitionNumber", m.cfg.PartitionNum),
			zap.Int16("replicationFactor", m.cfg.ReplicationFactor),
			zap.Error(err),
			zap.Duration("duration", time.Since(start)),
		)
		return 0, cerror.WrapError(cerror.ErrKafkaCreateTopic, err)
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

// CreateTopicAndWaitUntilVisible wraps createTopic and waitUntilTopicVisible together.
func (m *kafkaTopicManager) CreateTopicAndWaitUntilVisible(topicName string) (int32, error) {
	partitionNum, err := m.createTopic(topicName)
	if err != nil {
		return 0, errors.Trace(err)
	}

	err = m.waitUntilTopicVisible(topicName)
	if err != nil {
		return 0, errors.Trace(err)
	}

	return partitionNum, nil
}
