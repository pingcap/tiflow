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
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/model"
	kafkaconfig "github.com/pingcap/tiflow/cdc/sink/mq/producer/kafka"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/retry"
	"github.com/pingcap/tiflow/pkg/sink/kafka"
	"go.uber.org/zap"
)

const (
	// metaRefreshInterval is the interval of refreshing metadata.
	// We can't get the metadata too frequently, because it may cause
	// the kafka cluster to be overloaded. Especially when there are
	// many topics in the cluster or there are many TiCDC changefeeds.
	metaRefreshInterval = 10 * time.Minute
)

// kafkaTopicManager is a manager for kafka topics.
type kafkaTopicManager struct {
	changefeedID model.ChangeFeedID

	admin kafka.ClusterAdminClient

	cfg *kafkaconfig.AutoCreateTopicConfig

	topics sync.Map

	metaRefreshTicker *time.Ticker

	// cancel is used to cancel the background goroutine.
	cancel context.CancelFunc
}

// NewKafkaTopicManager creates a new topic manager.
func NewKafkaTopicManager(
	ctx context.Context,
	admin kafka.ClusterAdminClient,
<<<<<<< HEAD:cdc/sink/mq/manager/kafka_manager.go
	cfg *kafkaconfig.AutoCreateTopicConfig,
) (*kafkaTopicManager, error) {
	changefeedID := contextutil.ChangefeedIDFromCtx(ctx)
=======
	cfg *kafka.AutoCreateTopicConfig,
) *kafkaTopicManager {
>>>>>>> 447e5126cb (kafka(ticdc): sarama admin client fetch metadata by cache (#9511)):cdc/sink/dmlsink/mq/manager/kafka_manager.go
	mgr := &kafkaTopicManager{
		changefeedID:      changefeedID,
		admin:             admin,
		cfg:               cfg,
		metaRefreshTicker: time.NewTicker(metaRefreshInterval),
	}

	ctx, mgr.cancel = context.WithCancel(ctx)
	// Background refresh metadata.
	go mgr.backgroundRefreshMeta(ctx)

	return mgr
}

// GetPartitionNum returns the number of partitions of the topic.
// It may also try to update the topics' information maintained by manager.
func (m *kafkaTopicManager) GetPartitionNum(topic string) (int32, error) {
	if partitions, ok := m.topics.Load(topic); ok {
		return partitions.(int32), nil
	}

	// If the topic is not in the metadata, we try to create the topic.
	partitionNum, err := m.CreateTopicAndWaitUntilVisible(topic)
	if err != nil {
		return 0, errors.Trace(err)
	}

	return partitionNum, nil
}

func (m *kafkaTopicManager) backgroundRefreshMeta(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Info("Background refresh Kafka metadata goroutine exit.",
				zap.String("namespace", m.changefeedID.Namespace),
				zap.String("changefeed", m.changefeedID.ID),
			)
			return
		case <-m.metaRefreshTicker.C:
<<<<<<< HEAD:cdc/sink/mq/manager/kafka_manager.go
			topicMetaList, err := m.getMetadataOfTopics()
=======
>>>>>>> 447e5126cb (kafka(ticdc): sarama admin client fetch metadata by cache (#9511)):cdc/sink/dmlsink/mq/manager/kafka_manager.go
			// We ignore the error here, because the error may be caused by the
			// network problem, and we can try to get the metadata next time.
			topicPartitionNums, _ := m.fetchAllTopicsPartitionsNum(ctx)
			for topic, partitionNum := range topicPartitionNums {
				m.tryUpdatePartitionsAndLogging(topic, partitionNum)
			}
<<<<<<< HEAD:cdc/sink/mq/manager/kafka_manager.go

			for _, detail := range topicMetaList {
				partitionNum := int32(len(detail.Partitions))
				m.tryUpdatePartitionsAndLogging(detail.Name, partitionNum)
			}

=======
>>>>>>> 447e5126cb (kafka(ticdc): sarama admin client fetch metadata by cache (#9511)):cdc/sink/dmlsink/mq/manager/kafka_manager.go
		}
	}
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

<<<<<<< HEAD:cdc/sink/mq/manager/kafka_manager.go
func (m *kafkaTopicManager) getMetadataOfTopics() ([]*sarama.TopicMetadata, error) {
	var topicList []string

=======
// fetchAllTopicsPartitionsNum fetches all topics' partitions number.
// The error returned by this method could be a transient error that is fixable by the underlying logic.
// When handling this error, please be cautious.
// If you simply throw the error to the caller, it may impact the robustness of your program.
func (m *kafkaTopicManager) fetchAllTopicsPartitionsNum(
	ctx context.Context,
) (map[string]int32, error) {
	var topics []string
>>>>>>> 447e5126cb (kafka(ticdc): sarama admin client fetch metadata by cache (#9511)):cdc/sink/dmlsink/mq/manager/kafka_manager.go
	m.topics.Range(func(key, value any) bool {
		topics = append(topics, key.(string))
		return true
	})

	start := time.Now()
<<<<<<< HEAD:cdc/sink/mq/manager/kafka_manager.go
	topicMetaList, err := m.admin.DescribeTopics(topicList)
=======
	numPartitions, err := m.admin.GetTopicsPartitionsNum(ctx, topics)
>>>>>>> 447e5126cb (kafka(ticdc): sarama admin client fetch metadata by cache (#9511)):cdc/sink/dmlsink/mq/manager/kafka_manager.go
	if err != nil {
		log.Warn(
			"Kafka admin client describe topics failed",
			zap.String("namespace", m.changefeedID.Namespace),
			zap.String("changefeed", m.changefeedID.ID),
			zap.Error(err),
			zap.Duration("duration", time.Since(start)),
		)
		return nil, err
	}

	log.Info(
		"Kafka admin client describe topics success",
		zap.String("namespace", m.changefeedID.Namespace),
		zap.String("changefeed", m.changefeedID.ID),
		zap.Duration("duration", time.Since(start)))

	return numPartitions, nil
}

// waitUntilTopicVisible is called after CreateTopic to make sure the topic
// can be safely written to. The reason is that it may take several seconds after
// CreateTopic returns success for all the brokers to become aware that the
// topics have been created.
// See https://kafka.apache.org/23/javadoc/org/apache/kafka/clients/admin/AdminClient.html
func (m *kafkaTopicManager) waitUntilTopicVisible(topicName string) error {
	err := retry.Do(context.Background(), func() error {
		start := time.Now()
<<<<<<< HEAD:cdc/sink/mq/manager/kafka_manager.go
		topicMetaList, err := m.admin.DescribeTopics([]string{topicName})
=======
		// ignoreTopicError is set to false since we just create the topic,
		// make sure the topic is visible.
		meta, err := m.admin.GetTopicsMeta(ctx, topics, false)
>>>>>>> 447e5126cb (kafka(ticdc): sarama admin client fetch metadata by cache (#9511)):cdc/sink/dmlsink/mq/manager/kafka_manager.go
		if err != nil {
			log.Error("Kafka admin client describe topic failed",
				zap.String("namespace", m.changefeedID.Namespace),
				zap.String("changefeed", m.changefeedID.ID),
				zap.String("topic", topicName),
				zap.Error(err),
				zap.Duration("duration", time.Since(start)))
			return err
		}

		if len(topicMetaList) != 1 {
			log.Error("topic metadata length is wrong.",
				zap.String("namespace", m.changefeedID.Namespace),
				zap.String("changefeed", m.changefeedID.ID),
				zap.String("topic", topicName),
				zap.Int("expected", 1),
				zap.Int("actual", len(topicMetaList)))
			return cerror.ErrKafkaTopicNotExists.GenWithStack(
				fmt.Sprintf("metadata length of topic %s is not equal to 1", topicName))
		}

		meta := topicMetaList[0]
		if meta.Err != sarama.ErrNoError {
			log.Error("topic metadata is fetched with error",
				zap.String("namespace", m.changefeedID.Namespace),
				zap.String("changefeed", m.changefeedID.ID),
				zap.String("topic", topicName),
				zap.Error(meta.Err))
			return meta.Err
		}

		log.Info("Kafka admin client describe topic success",
			zap.String("namespace", m.changefeedID.Namespace),
			zap.String("changefeed", m.changefeedID.ID),
			zap.String("topic", topicName),
			zap.Int("partitionNumber", len(meta.Partitions)),
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
func (m *kafkaTopicManager) createTopic(topicName string) (int32, error) {
	if !m.cfg.AutoCreate {
		return 0, cerror.ErrKafkaInvalidConfig.GenWithStack(
			fmt.Sprintf("`auto-create-topic` is false, "+
				"and %s not found", topicName))
	}

	start := time.Now()
	err := m.admin.CreateTopic(topicName, &sarama.TopicDetail{
		NumPartitions:     m.cfg.PartitionNum,
		ReplicationFactor: m.cfg.ReplicationFactor,
	}, false)
	// Ignore the already exists error because it's not harmful.
	if err != nil && !strings.Contains(err.Error(), sarama.ErrTopicAlreadyExists.Error()) {
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
		return 0, cerror.WrapError(cerror.ErrKafkaNewSaramaProducer, err)
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
	m.tryUpdatePartitionsAndLogging(topicName, m.cfg.PartitionNum)

	return m.cfg.PartitionNum, nil
}

// CreateTopicAndWaitUntilVisible wraps createTopic and waitUntilTopicVisible together.
<<<<<<< HEAD:cdc/sink/mq/manager/kafka_manager.go
func (m *kafkaTopicManager) CreateTopicAndWaitUntilVisible(topicName string) (int32, error) {
	// If the topic is not in the cache, we try to get the metadata of the topic.
	topicDetails, err := m.admin.DescribeTopics([]string{topicName})
=======
func (m *kafkaTopicManager) CreateTopicAndWaitUntilVisible(
	ctx context.Context, topicName string,
) (int32, error) {
	// If the topic is not in the cache, we try to get the metadata of the topic.
	// ignoreTopicErr is set to true to ignore the error if the topic is not found,
	// which means we should create the topic later.
	topicDetails, err := m.admin.GetTopicsMeta(ctx, []string{topicName}, true)
>>>>>>> 447e5126cb (kafka(ticdc): sarama admin client fetch metadata by cache (#9511)):cdc/sink/dmlsink/mq/manager/kafka_manager.go
	if err != nil {
		return 0, errors.Trace(err)
	}
	for _, detail := range topicDetails {
		if detail.Err == sarama.ErrNoError {
			if detail.Name == topicName {
				log.Info("Kafka topic already exists",
					zap.String("namespace", m.changefeedID.Namespace),
					zap.String("changefeed", m.changefeedID.ID),
					zap.String("topic", topicName),
					zap.Int32("partitionNumber", int32(len(detail.Partitions))))
				partitionNum := int32(len(detail.Partitions))
				m.tryUpdatePartitionsAndLogging(topicName, partitionNum)
				return partitionNum, nil
			}
		} else if detail.Err != sarama.ErrUnknownTopicOrPartition {
			log.Error("Kafka admin client describe topic failed",
				zap.String("namespace", m.changefeedID.Namespace),
				zap.String("changefeed", m.changefeedID.ID),
				zap.String("topic", topicName),
				zap.Error(detail.Err))
			return 0, errors.Trace(detail.Err)
		}
	}

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

// Close exits the background goroutine.
func (m *kafkaTopicManager) Close() {
	m.cancel()
}
