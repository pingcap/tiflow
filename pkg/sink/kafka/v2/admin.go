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

package v2

import (
	"context"
	"strconv"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/errors"
	pkafka "github.com/pingcap/tiflow/pkg/sink/kafka"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

type admin struct {
	client       Client
	changefeedID model.ChangeFeedID
}

func newClusterAdminClient(
	endpoints []string,
	transport *kafka.Transport,
	changefeedID model.ChangeFeedID,
) pkafka.ClusterAdminClient {
	client := newClient(endpoints, transport)
	return &admin{
		client:       client,
		changefeedID: changefeedID,
	}
}

func (a *admin) clusterMetadata(ctx context.Context) (*kafka.MetadataResponse, error) {
	// request is not set, so it will return all metadata
	result, err := a.client.Metadata(ctx, &kafka.MetadataRequest{})
	if err != nil {
		return nil, errors.Trace(err)
	}
	return result, nil
}

func (a *admin) GetAllBrokers(ctx context.Context) ([]pkafka.Broker, error) {
	response, err := a.clusterMetadata(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}

	result := make([]pkafka.Broker, 0, len(response.Brokers))
	for _, broker := range response.Brokers {
		result = append(result, pkafka.Broker{
			ID: int32(broker.ID),
		})
	}
	return result, nil
}

func (a *admin) GetCoordinator(ctx context.Context) (int, error) {
	response, err := a.clusterMetadata(ctx)
	if err != nil {
		return 0, errors.Trace(err)
	}

	return response.Controller.ID, nil
}

func (a *admin) GetBrokerConfig(ctx context.Context, configName string) (string, error) {
	controllerID, err := a.GetCoordinator(ctx)
	if err != nil {
		return "", err
	}
	request := &kafka.DescribeConfigsRequest{
		Resources: []kafka.DescribeConfigRequestResource{
			{
				ResourceType: kafka.ResourceTypeBroker,
				ResourceName: strconv.Itoa(controllerID),
				ConfigNames:  []string{configName},
			},
		},
	}

	resp, err := a.client.DescribeConfigs(ctx, request)
	if err != nil {
		return "", errors.Trace(err)
	}

	if len(resp.Resources) == 0 || len(resp.Resources[0].ConfigEntries) == 0 {
		log.Warn("kafka config item not found",
			zap.String("configName", configName))
		return "", errors.ErrKafkaBrokerConfigNotFound.GenWithStack(
			"cannot find the `%s` from the broker's configuration", configName)
	}

	entry := resp.Resources[0].ConfigEntries[0]
	if entry.ConfigName != configName {
		log.Warn("kafka config item not found",
			zap.String("configName", configName))
		return "", errors.ErrKafkaBrokerConfigNotFound.GenWithStack(
			"cannot find the `%s` from the broker's configuration", configName)
	}

	return entry.ConfigValue, nil
}

func (a *admin) GetTopicsPartitions(ctx context.Context) (map[string]int32, error) {
	response, err := a.clusterMetadata(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	result := make(map[string]int32, len(response.Topics))
	for _, topic := range response.Topics {
		result[topic.Name] = int32(len(topic.Partitions))
	}
	return result, nil
}

func (a *admin) GetAllTopicsMeta(ctx context.Context) (map[string]pkafka.TopicDetail, error) {
	response, err := a.clusterMetadata(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}

	describeTopicConfigsRequest := &kafka.DescribeConfigsRequest{
		Resources: []kafka.DescribeConfigRequestResource{},
	}
	result := make(map[string]pkafka.TopicDetail, len(response.Topics))
	for _, topic := range response.Topics {
		result[topic.Name] = pkafka.TopicDetail{
			Name:              topic.Name,
			NumPartitions:     int32(len(topic.Partitions)),
			ReplicationFactor: int16(len(topic.Partitions[0].Replicas)),
		}
		describeTopicConfigsRequest.Resources = append(describeTopicConfigsRequest.Resources,
			kafka.DescribeConfigRequestResource{
				ResourceType: kafka.ResourceTypeTopic,
				ResourceName: topic.Name,
			})
	}

	describeTopicConfigsResponse, err := a.client.DescribeConfigs(ctx, describeTopicConfigsRequest)
	if err != nil {
		return nil, errors.Trace(err)
	}
	for _, resource := range describeTopicConfigsResponse.Resources {
		topicDetails, ok := result[resource.ResourceName]
		if !ok {
			return nil, errors.New("undesired topic found from the response")
		}
		topicDetails.ConfigEntries = make(map[string]string, len(resource.ConfigEntries))
		for _, entry := range resource.ConfigEntries {
			if entry.IsDefault || entry.IsSensitive {
				continue
			}
			topicDetails.ConfigEntries[entry.ConfigName] = entry.ConfigValue
		}
		result[resource.ResourceName] = topicDetails
	}

	return result, nil
}

func (a *admin) GetTopicsMeta(
	ctx context.Context,
	topics []string,
	ignoreTopicError bool,
) (map[string]pkafka.TopicDetail, error) {
	resp, err := a.client.Metadata(ctx, &kafka.MetadataRequest{
		Topics: topics,
	})
	if err != nil {
		return nil, errors.Trace(err)
	}

	result := make(map[string]pkafka.TopicDetail, len(resp.Topics))
	for _, topic := range resp.Topics {
		if topic.Error != nil {
			if !ignoreTopicError {
				return nil, topic.Error
			}
			log.Warn("fetch topic meta failed",
				zap.String("topic", topic.Name), zap.Error(topic.Error))
		}
		result[topic.Name] = pkafka.TopicDetail{
			Name:          topic.Name,
			NumPartitions: int32(len(topic.Partitions)),
		}
	}
	return result, nil
}

func (a *admin) CreateTopic(
	ctx context.Context,
	detail *pkafka.TopicDetail,
	validateOnly bool,
) error {
	request := &kafka.CreateTopicsRequest{
		Topics: []kafka.TopicConfig{
			{
				Topic:             detail.Name,
				NumPartitions:     int(detail.NumPartitions),
				ReplicationFactor: int(detail.ReplicationFactor),
			},
		},
		ValidateOnly: validateOnly,
	}

	response, err := a.client.CreateTopics(ctx, request)
	if err != nil {
		return errors.Trace(err)
	}

	for _, err := range response.Errors {
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func (a *admin) Close() {
	client, ok := a.client.(*kafka.Client)
	if !ok {
		return
	}

	if client.Transport == nil {
		return
	}

	transport, ok := client.Transport.(*kafka.Transport)
	if !ok {
		return
	}

	transport.CloseIdleConnections()
	log.Info("admin client close idle connections",
		zap.String("namespace", a.changefeedID.Namespace),
		zap.String("changefeed", a.changefeedID.ID))
}
