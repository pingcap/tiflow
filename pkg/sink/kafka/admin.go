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
	"github.com/pingcap/log"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

type admin struct {
	client sarama.ClusterAdmin
}

// NewSaramaAdminClient constructs a ClusterAdminClient with sarama.
func NewSaramaAdminClient(ctx context.Context, config *Options) (ClusterAdminClient, error) {
	saramaConfig, err := NewSaramaConfig(ctx, config)
	if err != nil {
		return nil, err
	}

	client, err := sarama.NewClusterAdmin(config.BrokerEndpoints, saramaConfig)
	if err != nil {
		return nil, err
	}
	return &admin{client: client}, nil
}

func (a *admin) ListTopics() (map[string]TopicDetail, error) {
	topics, err := a.client.ListTopics()
	if err != nil {
		return nil, err
	}

	result := make(map[string]TopicDetail, len(topics))
	for topic, detail := range topics {
		configEntries := make(map[string]string, len(detail.ConfigEntries))
		for name, value := range detail.ConfigEntries {
			if value != nil {
				configEntries[name] = *value
			}
		}
		result[topic] = TopicDetail{
			NumPartitions:     detail.NumPartitions,
			ReplicationFactor: detail.ReplicationFactor,
			ConfigEntries:     configEntries,
		}
	}

	return result, nil
}

func (a *admin) CreateTopic(topic string, detail *TopicDetail, validateOnly bool) error {
	err := a.client.CreateTopic(topic, &sarama.TopicDetail{
		NumPartitions:     detail.NumPartitions,
		ReplicationFactor: detail.ReplicationFactor,
	}, validateOnly)
	// Ignore the already exists error because it's not harmful.
	if err != nil && !strings.Contains(err.Error(), sarama.ErrTopicAlreadyExists.Error()) {
		return err
	}
	return nil
}

func (a *admin) GetAllBrokers() ([]Broker, error) {
	brokers, _, err := a.client.DescribeCluster()
	if err != nil {
		return nil, err
	}

	result := make([]Broker, 0, len(brokers))
	for _, broker := range brokers {
		result = append(result, Broker{
			ID: broker.ID(),
		})
	}

	return result, nil
}

func (a *admin) GetCoordinator() (int32, error) {
	_, controllerID, err := a.client.DescribeCluster()
	if err != nil {
		return 0, err
	}
	return controllerID, nil
}

func (a *admin) GetBrokerConfig(configName string) (string, error) {
	_, controller, err := a.client.DescribeCluster()
	if err != nil {
		return "", err
	}

	configEntries, err := a.client.DescribeConfig(sarama.ConfigResource{
		Type:        sarama.BrokerResource,
		Name:        strconv.Itoa(int(controller)),
		ConfigNames: []string{configName},
	})
	if err != nil {
		return "", err
	}

	if len(configEntries) == 0 || configEntries[0].Name != configName {
		log.Warn("Kafka config item not found", zap.String("configName", configName))
		return "", cerror.ErrKafkaBrokerConfigNotFound.GenWithStack(
			"cannot find the `%s` from the broker's configuration", configName)
	}

	return configEntries[0].Value, nil
}

func (a *admin) GetTopicMeta(topics []string) ([]*TopicMetadata, error) {
	metaList, err := a.client.DescribeTopics(topics)
	if err != nil {
		return nil, err
	}

	result := make([]*TopicMetadata, 0, len(metaList))
	for _, meta := range metaList {
		if meta.Err != sarama.ErrNoError {
			return nil, meta.Err
		}
		result = append(result, &TopicMetadata{
			Name:         meta.Name,
			Err:          meta.Err,
			NumPartition: int32(len(meta.Partitions)),
		})
	}

	return result, nil
}

func (a *admin) Close() error {
	return a.client.Close()
}

func configResourceType4Sarama(resourceType ConfigResourceType) sarama.ConfigResourceType {
	switch resourceType {
	case TopicResource:
		return sarama.TopicResource
	case BrokerResource:
		return sarama.BrokerResource
	case BrokerLoggerResource:
		return sarama.BrokerLoggerResource
	default:
		return sarama.UnknownResource
	}
}
