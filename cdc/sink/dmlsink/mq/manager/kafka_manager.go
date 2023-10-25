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
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/retry"
	"github.com/pingcap/tiflow/pkg/sink/kafka"
	"github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

// kafkaTopicManager is a manager for kafka topics.
type kafkaTopicManager struct {
	changefeedID model.ChangeFeedID
	role         util.Role

	admin kafka.ClusterAdminClient

	cfg *kafka.AutoCreateTopicConfig

	topics sync.Map
}

// NewKafkaTopicManager creates a new topic manager.
func NewKafkaTopicManager(
	changefeedID model.ChangeFeedID,
	admin kafka.ClusterAdminClient,
	cfg *kafka.AutoCreateTopicConfig,
	role util.Role,
) *kafkaTopicManager {
	return &kafkaTopicManager{
		changefeedID: changefeedID,
		role:         role,
		admin:        admin,
		cfg:          cfg,
	}
}

// GetPartitionNum returns the number of partitions of the topic.
// It may also try to update the topics' information maintained by manager.
func (m *kafkaTopicManager) GetPartitionNum(
	ctx context.Context,
	topic string,
) (int32, error) {
	if partitions, ok := m.topics.Load(topic); ok {
		return partitions.(int32), nil
	}

	// If the topic is not in the metadata, we try to create the topic.
	partitionNum, err := m.CreateTopicAndWaitUntilVisible(ctx, topic)
	if err != nil {
		return 0, errors.Trace(err)
	}

	return partitionNum, nil
}

// tryUpdatePartitionsAndLogging try to update the partitions of the topic.
func (m *kafkaTopicManager) tryUpdatePartitionsAndLogging(topic string, partitions int32) {
	oldPartitions, ok := m.topics.Load(topic)
	if ok {
		if oldPartitions.(int32) != partitions {
			m.topics.Store(topic, partitions)
			log.Info(
				"update topic partition number",
				zap.String("namespace", m.changefeedID.Namespace),
				zap.String("changefeed", m.changefeedID.ID),
				zap.String("topic", topic),
				zap.Int32("oldPartitionNumber", oldPartitions.(int32)),
				zap.Int32("newPartitionNumber", partitions),
			)
		}
	} else {
		m.topics.Store(topic, partitions)
		log.Info(
			"store topic partition number",
			zap.String("namespace", m.changefeedID.Namespace),
			zap.String("changefeed", m.changefeedID.ID),
			zap.String("topic", topic),
			zap.Int32("partitionNumber", partitions),
		)
	}
}

// waitUntilTopicVisible is called after CreateTopic to make sure the topic
// can be safely written to. The reason is that it may take several seconds after
// CreateTopic returns success for all the brokers to become aware that the
// topics have been created.
// See https://kafka.apache.org/23/javadoc/org/apache/kafka/clients/admin/AdminClient.html
func (m *kafkaTopicManager) waitUntilTopicVisible(
	ctx context.Context,
	topicName string,
) error {
	topics := []string{topicName}
	err := retry.Do(ctx, func() error {
		start := time.Now()
		// ignoreTopicError is set to false since we just create the topic,
		// make sure the topic is visible.
		meta, err := m.admin.GetTopicsMeta(ctx, topics, false)
		if err != nil {
			log.Warn(" topic not found, retry it",
				zap.String("namespace", m.changefeedID.Namespace),
				zap.String("changefeed", m.changefeedID.ID),
				zap.Error(err),
				zap.Duration("duration", time.Since(start)),
			)
			return err
		}
		log.Info("topic found",
			zap.String("namespace", m.changefeedID.Namespace),
			zap.String("changefeed", m.changefeedID.ID),
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

// createTopic creates a topic with the given name
// and returns the number of partitions.
func (m *kafkaTopicManager) createTopic(
	ctx context.Context,
	topicName string,
) (int32, error) {
	if !m.cfg.AutoCreate {
		return 0, cerror.ErrKafkaInvalidConfig.GenWithStack(
			fmt.Sprintf("`auto-create-topic` is false, "+
				"and %s not found", topicName))
	}

	start := time.Now()
	err := m.admin.CreateTopic(ctx, &kafka.TopicDetail{
		Name:              topicName,
		NumPartitions:     m.cfg.PartitionNum,
		ReplicationFactor: m.cfg.ReplicationFactor,
	}, false)
	if err != nil {
		log.Error(
			"Kafka admin client create the topic failed",
			zap.String("namespace", m.changefeedID.Namespace),
			zap.String("changefeed", m.changefeedID.ID),
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
		zap.String("namespace", m.changefeedID.Namespace),
		zap.String("changefeed", m.changefeedID.ID),
		zap.String("topic", topicName),
		zap.Int32("partitionNumber", m.cfg.PartitionNum),
		zap.Int16("replicationFactor", m.cfg.ReplicationFactor),
		zap.Duration("duration", time.Since(start)),
	)
	return m.cfg.PartitionNum, nil
}

// CreateTopicAndWaitUntilVisible wraps createTopic and waitUntilTopicVisible together.
func (m *kafkaTopicManager) CreateTopicAndWaitUntilVisible(
	ctx context.Context, topicName string,
) (int32, error) {
	var (
		partitionNum int32
		err          error
	)
	// If the topic is not in the cache, we try to get the metadata of the topic.
	// ignoreTopicErr is set to true to ignore the error if the topic is not found,
	// which means we should create the topic later.
	topicDetails, err := m.admin.GetTopicsMeta(ctx, []string{topicName}, true)
	if err != nil {
		return 0, errors.Trace(err)
	}

	details, ok := topicDetails[topicName]
	partitionNum = details.NumPartitions
	if !ok {
		partitionNum, err = m.createTopic(ctx, topicName)
		if err != nil {
			return 0, errors.Trace(err)
		}

		err = m.waitUntilTopicVisible(ctx, topicName)
		if err != nil {
			return 0, errors.Trace(err)
		}
	}

	// store the partition number specified in the sink-uri which is adjusted.
	m.tryUpdatePartitionsAndLogging(topicName, m.cfg.PartitionNum)
	return partitionNum, nil
}
