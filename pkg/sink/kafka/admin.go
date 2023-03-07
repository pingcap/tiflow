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
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/Shopify/sarama"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/retry"
	"go.uber.org/zap"
)

type saramaAdminClient struct {
	brokerEndpoints []string
	config          *sarama.Config
	changefeed      model.ChangeFeedID

	mu     sync.Mutex
	client sarama.Client
	admin  sarama.ClusterAdmin
}

const (
	defaultRetryBackoff  = 20
	defaultRetryMaxTries = 3
)

func newAdminClient(
	brokerEndpoints []string,
	config *sarama.Config,
	changefeed model.ChangeFeedID,
) (ClusterAdminClient, error) {
	client, err := sarama.NewClient(brokerEndpoints, config)
	if err != nil {
		return nil, cerror.Trace(err)
	}

	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		return nil, err
	}
	return &saramaAdminClient{
		client:          client,
		admin:           admin,
		brokerEndpoints: brokerEndpoints,
		config:          config,
		changefeed:      changefeed,
	}, nil
}

func (a *saramaAdminClient) reset() error {
	newClient, err := sarama.NewClient(a.brokerEndpoints, a.config)
	if err != nil {
		return cerror.Trace(err)
	}
	newAdmin, err := sarama.NewClusterAdminFromClient(newClient)
	if err != nil {
		return cerror.Trace(err)
	}

	_ = a.admin.Close()
	a.client = newClient
	a.admin = newAdmin
	log.Info("kafka admin client is reset",
		zap.String("namespace", a.changefeed.Namespace),
		zap.String("changefeed", a.changefeed.ID))
	return errors.New("retry after reset")
}

func (a *saramaAdminClient) queryClusterWithRetry(ctx context.Context, query func() error) error {
	err := retry.Do(ctx, func() error {
		a.mu.Lock()
		defer a.mu.Unlock()
		err := query()
		if err == nil {
			return nil
		}

		log.Warn("query kafka cluster meta failed, retry it",
			zap.String("namespace", a.changefeed.Namespace),
			zap.String("changefeed", a.changefeed.ID),
			zap.Error(err))

		if !errors.Is(err, syscall.EPIPE) {
			return err
		}
		if !errors.Is(err, net.ErrClosed) {
			return err
		}
		if !errors.Is(err, io.EOF) {
			return err
		}

		return a.reset()
	}, retry.WithBackoffBaseDelay(defaultRetryBackoff), retry.WithMaxTries(defaultRetryMaxTries))
	return err
}

func (a *saramaAdminClient) GetAllBrokers(ctx context.Context) ([]Broker, error) {
	var (
		brokers []*sarama.Broker
		err     error
	)
	query := func() error {
		brokers, _, err = a.admin.DescribeCluster()
		return err
	}

	err = a.queryClusterWithRetry(ctx, query)
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

func (a *saramaAdminClient) GetCoordinator(ctx context.Context) (int, error) {
	var (
		controllerID int32
		err          error
	)

	query := func() error {
		_, controllerID, err = a.admin.DescribeCluster()
		return err
	}
	err = a.queryClusterWithRetry(ctx, query)
	return int(controllerID), err
}

func (a *saramaAdminClient) GetBrokerConfig(
	ctx context.Context,
	configName string,
) (string, error) {
	controller, err := a.GetCoordinator(ctx)
	if err != nil {
		return "", err
	}

	var configEntries []sarama.ConfigEntry
	query := func() error {
		configEntries, err = a.admin.DescribeConfig(sarama.ConfigResource{
			Type:        sarama.BrokerResource,
			Name:        strconv.Itoa(controller),
			ConfigNames: []string{configName},
		})
		return err
	}
	err = a.queryClusterWithRetry(ctx, query)
	if err != nil {
		return "", err
	}

	if len(configEntries) == 0 || configEntries[0].Name != configName {
		log.Warn("Kafka config item not found",
			zap.String("namespace", a.changefeed.Namespace),
			zap.String("changefeed", a.changefeed.ID),
			zap.String("configName", configName))
		return "", cerror.ErrKafkaBrokerConfigNotFound.GenWithStack(
			"cannot find the `%s` from the broker's configuration", configName)
	}

	return configEntries[0].Value, nil
}

func (a *saramaAdminClient) GetTopicsPartitions(_ context.Context) (map[string]int32, error) {
	topics, err := a.client.Topics()
	if err != nil {
		return nil, cerror.Trace(err)
	}

	result := make(map[string]int32, len(topics))
	for _, topic := range topics {
		partitions, err := a.client.Partitions(topic)
		if err != nil {
			return nil, cerror.Trace(err)
		}
		result[topic] = int32(len(partitions))
	}

	return result, nil
}

func (a *saramaAdminClient) GetAllTopicsMeta(ctx context.Context) (map[string]TopicDetail, error) {
	var (
		topics map[string]sarama.TopicDetail
		err    error
	)

	query := func() error {
		topics, err = a.admin.ListTopics()
		return err
	}
	err = a.queryClusterWithRetry(ctx, query)
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
			Name:              topic,
			NumPartitions:     detail.NumPartitions,
			ReplicationFactor: detail.ReplicationFactor,
			ConfigEntries:     configEntries,
		}
	}

	return result, nil
}

func (a *saramaAdminClient) GetTopicsMeta(
	ctx context.Context,
	topics []string,
	ignoreTopicError bool,
) (map[string]TopicDetail, error) {
	var (
		metaList []*sarama.TopicMetadata
		err      error
	)
	query := func() error {
		metaList, err = a.admin.DescribeTopics(topics)
		return err
	}

	err = a.queryClusterWithRetry(ctx, query)
	if err != nil {
		return nil, err
	}

	result := make(map[string]TopicDetail, len(metaList))
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

func (a *saramaAdminClient) CreateTopic(
	ctx context.Context,
	detail *TopicDetail,
	validateOnly bool,
) error {
	request := &sarama.TopicDetail{
		NumPartitions:     detail.NumPartitions,
		ReplicationFactor: detail.ReplicationFactor,
	}
	query := func() error {
		err := a.admin.CreateTopic(detail.Name, request, validateOnly)
		// Ignore the already exists error because it's not harmful.
		if err != nil && !strings.Contains(err.Error(), sarama.ErrTopicAlreadyExists.Error()) {
			return err
		}
		return nil
	}
	return a.queryClusterWithRetry(ctx, query)
}

func (a *saramaAdminClient) Close() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.admin.Close(); err != nil {
		log.Warn("close admin client meet error",
			zap.String("namespace", a.changefeed.Namespace),
			zap.String("changefeed", a.changefeed.ID),
			zap.Error(err))
	}
}
