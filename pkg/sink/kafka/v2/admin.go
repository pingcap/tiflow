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

	"github.com/pingcap/log"
	pkafka "github.com/pingcap/tiflow/pkg/sink/kafka"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

type admin struct {
	client *kafka.Client
}

func NewClusterAdminClient(ctx context.Context, endpoints []string) *admin {
	client := &kafka.Client{
		Addr: kafka.TCP(endpoints...),
		// todo: set transport
		Transport: nil,
	}
	return &admin{
		client: client,
	}
}

func (a *admin) GetAllBrokers() ([]pkafka.Broker, error) {
	return nil, nil
}

func (a *admin) GetCoordinator(context.Context) (int32, error) {
	resp, err := a.client.FindCoordinator(ctx, &kafka.FindCoordinatorRequest{})

	return int32(resp.Coordinator.NodeID), nil
}

func (a *admin) GetBrokerConfig(configName string) (string, error) {
	//var resources []kafka.DescribeConfigRequestResource
	//resources = append(resources, resource)
	//request := &kafka.DescribeConfigsRequest{
	//	Addr:      a.client.Addr,
	//	Resources: resources,
	//}
	//
	//response, err := a.client.DescribeConfigs(ctx, request)
	//if err != nil {
	//	return nil, err
	//}
	//
	//var entries []configEntry
	//for _, respResource := range response.Resources {
	//	if respResource.ResourceName == resource.ResourceName {
	//		if respResource.Error != nil {
	//			// todo: wrap this into a cdc rfc error.
	//			return nil, respResource.Error
	//		}
	//		for _, entry := range respResource.ConfigEntries {
	//			entries = append(entries, configEntry{
	//				Name:  entry.ConfigName,
	//				Value: entry.ConfigValue,
	//			})
	//		}
	//	}
	//}
	//
	//return entries, nil
	return "", nil
}

func (a *admin) GetAllTopicsMeta() (map[string]pkafka.TopicDetail, error) {
	request := &kafka.DescribeConfigsRequest{
		Addr:      a.client.Addr,
		Resources: nil,
	}
	//resp, err := a.client.DescribeConfigs(ctx,
	//	&kafka.DescribeConfigsRequest{
	//		Addr:      a.client.Addr,
	//		Resources: []kafka.DescribeConfigRequestResource{{}},
	//	})
	//describeResp, err := client.DescribeConfigs(context.Background(), &DescribeConfigsRequest{
	//	Resources: []DescribeConfigRequestResource{{
	//		ResourceType: ResourceTypeTopic,
	//		ResourceName: topic,
	//		ConfigNames:  []string{MaxMessageBytes},
	//	}},
	//})
	//
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//maxMessageBytesValue := "0"
	//for _, resource := range describeResp.Resources {
	//	if resource.ResourceType == int8(ResourceTypeTopic) && resource.ResourceName == topic {
	//		for _, entry := range resource.ConfigEntries {
	//			if entry.ConfigName == MaxMessageBytes {
	//				maxMessageBytesValue = entry.ConfigValue
	//			}
	//		}
	//	}
	//}
	//assert.Equal(t, maxMessageBytesValue, MaxMessageBytesValue)
	return nil, nil
}

func (a *admin) GetTopicsMeta(topics []string, ignoreTopicError bool) (map[string]pkafka.TopicDetail, error) {
	var resources []kafka.DescribeConfigRequestResource
	for _, topic := range topics {
		resources = append(resources, kafka.DescribeConfigRequestResource{
			ResourceType: kafka.ResourceTypeTopic,
			ResourceName: topic,
		})
	}

	request := &kafka.DescribeConfigsRequest{
		Resources: resources,
	}

	response, err := a.client.DescribeConfigs(ctx, request)
	if err != nil {
		return nil, err
	}

	var result []string
	for _, resp := range response.Resources {
		if resp.Error != nil {
			log.Warn("describe topic failed",
				zap.Error(err))
			return nil, err
		}
		result = append(result, resp.ResourceName)
	}
	return result, nil
}

func (a *admin) CreateTopic(ctx context.Context, topic string, detail *pkafka.TopicDetail, validateOnly bool) error {
	request := &kafka.CreateTopicsRequest{
		Addr: a.client.Addr,
		Topics: []kafka.TopicConfig{{
			Topic:             topic,
			NumPartitions:     int(detail.NumPartitions),
			ReplicationFactor: int(detail.ReplicationFactor),
		}},
		ValidateOnly: validateOnly,
	}

	response, err := a.client.CreateTopics(ctx, request)
	if err != nil {
		return err
	}

	for _, err := range response.Errors {
		if err != nil {
			return err
		}
	}

	return nil
}

//func (a *admin) DescribeCluster(ctx context.Context) (brokers []pkafka.Broker, controllerID int32, err error) {
//	return nil, 0, nil
//	//controller, err := a.client.Controller(ctx)
//	//return nil, 0, nil
//
//	// to create topics when auto.create.topics.enable='false'
//	//topic := "my-topic"
//	//
//	//
//	//controller, err := conn.Controller()
//	//if err != nil {
//	//	panic(err.Error())
//	//}
//	//var controllerConn *kafka.Conn
//	//controllerConn, err = kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
//	//if err != nil {
//	//	panic(err.Error())
//	//}
//	//defer controllerConn.Close()
//	//
//	//
//	//topicConfigs := []kafka.TopicConfig{
//	//	{
//	//		Topic:             topic,
//	//		NumPartitions:     1,
//	//		ReplicationFactor: 1,
//	//	},
//	//}
//	//
//	//err = controllerConn.CreateTopics(topicConfigs...)
//	//if err != nil {
//	//	panic(err.Error())
//	//}
//}

func (a *admin) Close() error {
	return nil
}
