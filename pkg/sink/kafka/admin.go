// Copyright 2023 PingCAP, Inc.
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
	"context"
	"strconv"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

type saramaAdminClient struct {
	changefeed model.ChangeFeedID

	client sarama.Client
	admin  sarama.ClusterAdmin
}

func (a *saramaAdminClient) GetAllBrokers(_ context.Context) ([]Broker, error) {
	brokers := a.client.Brokers()
	result := make([]Broker, 0, len(brokers))
	for _, broker := range brokers {
		result = append(result, Broker{
			ID: broker.ID(),
		})
	}

	return result, nil
}

func (a *saramaAdminClient) GetBrokerConfig(
	_ context.Context,
	configName string,
) (string, error) {
	_, controller, err := a.admin.DescribeCluster()
	if err != nil {
		return "", errors.Trace(err)
	}

	configEntries, err := a.admin.DescribeConfig(sarama.ConfigResource{
		Type:        sarama.BrokerResource,
		Name:        strconv.Itoa(int(controller)),
		ConfigNames: []string{configName},
	})
	if err != nil {
		return "", errors.Trace(err)
	}

	// For compatibility with KOP, we checked all return values.
	// 1. Kafka only returns requested configs.
	// 2. Kop returns all configs.
	for _, entry := range configEntries {
		if entry.Name == configName {
			return entry.Value, nil
		}
	}

	log.Warn("Kafka config item not found",
		zap.String("namespace", a.changefeed.Namespace),
		zap.String("changefeed", a.changefeed.ID),
		zap.String("configName", configName))
	return "", cerror.ErrKafkaConfigNotFound.GenWithStack(
		"cannot find the `%s` from the broker's configuration", configName)
}

func (a *saramaAdminClient) GetTopicConfig(
	_ context.Context, topicName string, configName string,
) (string, error) {
	configEntries, err := a.admin.DescribeConfig(sarama.ConfigResource{
		Type:        sarama.TopicResource,
		Name:        topicName,
		ConfigNames: []string{configName},
	})
	if err != nil {
		return "", errors.Trace(err)
	}

	// For compatibility with KOP, we checked all return values.
	// 1. Kafka only returns requested configs.
	// 2. Kop returns all configs.
	for _, entry := range configEntries {
		if entry.Name == configName {
			log.Info("Kafka config item found",
				zap.String("namespace", a.changefeed.Namespace),
				zap.String("changefeed", a.changefeed.ID),
				zap.String("configName", configName),
				zap.String("configValue", entry.Value))
			return entry.Value, nil
		}
	}

	log.Warn("Kafka config item not found",
		zap.String("namespace", a.changefeed.Namespace),
		zap.String("changefeed", a.changefeed.ID),
		zap.String("configName", configName))
	return "", cerror.ErrKafkaConfigNotFound.GenWithStack(
		"cannot find the `%s` from the topic's configuration", configName)
}

func (a *saramaAdminClient) GetTopicsMeta(
	_ context.Context, topics []string, ignoreTopicError bool,
) (map[string]TopicDetail, error) {
	result := make(map[string]TopicDetail, len(topics))

	metaList, err := a.admin.DescribeTopics(topics)
	if err != nil {
		return nil, errors.Trace(err)
	}

	for _, meta := range metaList {
		if meta.Err != sarama.ErrNoError {
			if !ignoreTopicError {
				return nil, meta.Err
			}
			log.Warn("fetch topic meta failed",
				zap.String("namespace", a.changefeed.Namespace),
				zap.String("changefeed", a.changefeed.ID),
				zap.String("topic", meta.Name),
				zap.Error(meta.Err))
			continue
		}
		result[meta.Name] = TopicDetail{
			Name:          meta.Name,
			NumPartitions: int32(len(meta.Partitions)),
		}
	}
	return result, nil
}

func (a *saramaAdminClient) GetTopicsPartitionsNum(
	_ context.Context, topics []string,
) (map[string]int32, error) {
	result := make(map[string]int32, len(topics))
	for _, topic := range topics {
		partition, err := a.client.Partitions(topic)
		if err != nil {
			return nil, errors.Trace(err)
		}
		result[topic] = int32(len(partition))
	}

	return result, nil
}

func (a *saramaAdminClient) CreateTopic(
	_ context.Context, detail *TopicDetail, validateOnly bool,
) error {
	request := &sarama.TopicDetail{
		NumPartitions:     detail.NumPartitions,
		ReplicationFactor: detail.ReplicationFactor,
	}

	err := a.admin.CreateTopic(detail.Name, request, validateOnly)
	// Ignore the already exists error because it's not harmful.
	if err != nil && !strings.Contains(err.Error(), sarama.ErrTopicAlreadyExists.Error()) {
		return err
	}
	return nil
}

func (a *saramaAdminClient) Close() {
	if err := a.admin.Close(); err != nil {
		log.Warn("close admin client meet error",
			zap.String("namespace", a.changefeed.Namespace),
			zap.String("changefeed", a.changefeed.ID),
			zap.Error(err))
	}
}
