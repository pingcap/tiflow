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
	"fmt"
	"net"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/errors"
	pkafka "github.com/pingcap/tiflow/pkg/sink/kafka"
	mock "github.com/pingcap/tiflow/pkg/sink/kafka/v2/mock"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/stretchr/testify/require"
)

func newClusterAdminClientWithMock(t *testing.T) (pkafka.ClusterAdminClient, *mock.MockClient) {
	transport, err := newTransport(pkafka.NewOptions())
	require.NoError(t, err)

	changefeedID := model.DefaultChangeFeedID("changefeed-test")
	adminClient := newClusterAdminClient([]string{"127.0.0.1:9092"}, transport, changefeedID)

	ctrl := gomock.NewController(t)
	client := mock.NewMockClient(ctrl)
	adminClient.(*admin).client = client
	return adminClient, client
}

func TestNewClusterAdminClient(t *testing.T) {
	t.Parallel()

	transport, err := newTransport(pkafka.NewOptions())
	require.NoError(t, err)

	changefeedID := model.DefaultChangeFeedID("changefeed-test")
	adminClient := newClusterAdminClient([]string{"127.0.0.1:9092"}, transport, changefeedID)
	require.NotNil(t, adminClient)
	require.NotNil(t, adminClient.(*admin).client)
}

func TestGetAllBrokers(t *testing.T) {
	t.Parallel()

	admin, client := newClusterAdminClientWithMock(t)
	client.EXPECT().Metadata(gomock.Any(), gomock.Any()).
		Return(nil, fmt.Errorf("kafka.(*Client).Metadata"))

	ctx := context.Background()
	brokers, err := admin.GetAllBrokers(ctx)
	require.Error(t, err)
	require.Nil(t, brokers)

	client.EXPECT().Metadata(gomock.Any(), gomock.Any()).
		Return(&kafka.MetadataResponse{}, nil)
	brokers, err = admin.GetAllBrokers(ctx)
	require.NoError(t, err)
	require.NotNil(t, brokers)
	require.Len(t, brokers, 0)

	client.EXPECT().Metadata(gomock.Any(), gomock.Any()).
		Return(&kafka.MetadataResponse{
			Brokers: []kafka.Broker{
				{ID: 1},
			},
		}, nil)
	brokers, err = admin.GetAllBrokers(ctx)
	require.NoError(t, err)
	require.Len(t, brokers, 1)
	require.Equal(t, int32(1), brokers[0].ID)
}

func TestGetBrokerConfig(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	admin, client := newClusterAdminClientWithMock(t)

	// cannot find the coordinator
	client.EXPECT().Metadata(gomock.Any(), gomock.Any()).
		Return(nil, fmt.Errorf("kafka.(*Client).Metadata"))
	result, err := admin.GetBrokerConfig(ctx, "test-config-name")
	require.Error(t, err)
	require.Equal(t, "", result)

	client.EXPECT().Metadata(gomock.Any(), gomock.Any()).
		Return(&kafka.MetadataResponse{
			Controller: kafka.Broker{ID: 1},
		}, nil).AnyTimes()

	// cannot get kafka broker's config
	client.EXPECT().DescribeConfigs(gomock.Any(), gomock.Any()).
		Return(nil, fmt.Errorf("kafka.(*Client).DescribeConfigs"))
	result, err = admin.GetBrokerConfig(ctx, "test-config-name")
	require.Error(t, err)
	require.Equal(t, "", result)

	// config is not found
	client.EXPECT().DescribeConfigs(gomock.Any(), gomock.Any()).
		Return(&kafka.DescribeConfigsResponse{
			Resources: []kafka.DescribeConfigResponseResource{},
		}, nil)
	result, err = admin.GetBrokerConfig(ctx, "test-config-name")
	require.Error(t, err, errors.ErrKafkaConfigNotFound)
	require.Equal(t, "", result)

	// config is found
	client.EXPECT().DescribeConfigs(gomock.Any(), gomock.Any()).
		Return(&kafka.DescribeConfigsResponse{
			Resources: []kafka.DescribeConfigResponseResource{
				{
					ConfigEntries: []kafka.DescribeConfigResponseConfigEntry{
						{
							ConfigName:  "test-config-name",
							ConfigValue: "test-config-value",
						},
					},
				},
			},
		}, nil)

	result, err = admin.GetBrokerConfig(ctx, "test-config-name")
	require.NoError(t, err)
	require.Equal(t, "test-config-value", result)

	// config is found, but the key does not match,
	// this should not happen, but we still prevent such case.
	client.EXPECT().DescribeConfigs(gomock.Any(), gomock.Any()).
		Return(&kafka.DescribeConfigsResponse{
			Resources: []kafka.DescribeConfigResponseResource{
				{
					ConfigEntries: []kafka.DescribeConfigResponseConfigEntry{
						{
							ConfigName:  "undesired-config-name",
							ConfigValue: "undesired-config-value",
						},
					},
				},
			},
		}, nil)
	result, err = admin.GetBrokerConfig(ctx, "test-config-name")
	require.Error(t, err, errors.ErrKafkaConfigNotFound)
	require.Len(t, result, 0)
}

func TestGetTopicMeta(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	admin, client := newClusterAdminClientWithMock(t)

	// cannot get topics meta from kafka
	client.EXPECT().Metadata(gomock.Any(), gomock.Any()).
		Return(nil, fmt.Errorf("kafka.(*Client).Metadata"))
	result, err := admin.GetTopicsMeta(ctx, []string{}, true)
	require.Error(t, err)
	require.Nil(t, result)

	client.EXPECT().Metadata(gomock.Any(), gomock.Any()).
		Return(&kafka.MetadataResponse{}, nil)
	result, err = admin.GetTopicsMeta(ctx, []string{}, true)
	require.NoError(t, err)
	require.Len(t, result, 0)

	targetTopic := "topic-1"
	client.EXPECT().Metadata(gomock.Any(), gomock.Any()).
		Return(&kafka.MetadataResponse{
			Topics: []kafka.Topic{
				{
					Name: targetTopic,
					Partitions: []kafka.Partition{
						{}, {}, // 2 partitions
					},
				},
				{
					Name: "topic-2",
					Partitions: []kafka.Partition{
						{}, {}, // 2 partitions
					},
					Error: errors.New("topic error found"),
				},
			},
		}, nil).Times(2)
	// ignore topic error
	result, err = admin.GetTopicsMeta(ctx, []string{targetTopic}, true)
	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Equal(t, "topic-1", result[targetTopic].Name)
	require.Equal(t, int32(2), result[targetTopic].NumPartitions)

	// not ignore topic error
	result, err = admin.GetTopicsMeta(ctx, []string{targetTopic}, false)
	require.Error(t, err)
	require.Nil(t, result)
}

func TestGetTopicsPartitions(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	admin, client := newClusterAdminClientWithMock(t)

	// cannot get topics meta from kafka
	client.EXPECT().Metadata(gomock.Any(), gomock.Any()).
		Return(nil, fmt.Errorf("kafka.(*Client).Metadata"))
	result, err := admin.GetTopicsMeta(ctx, []string{"test"}, false)
	require.Error(t, err)
	require.Nil(t, result)

	client.EXPECT().Metadata(gomock.Any(), gomock.Any()).
		Return(&kafka.MetadataResponse{}, nil)
	result, err = admin.GetTopicsMeta(ctx, []string{"test"}, false)
	require.NoError(t, err)
	require.Len(t, result, 0)

	client.EXPECT().Metadata(gomock.Any(), gomock.Any()).
		Return(&kafka.MetadataResponse{
			Topics: []kafka.Topic{
				{
					Name: "topic-1",
					Partitions: []kafka.Partition{
						{}, {}, // 2 partitions
					},
					Error: errors.New("topic error found"),
				},
				{
					Name: "topic-2",
					Partitions: []kafka.Partition{
						{}, {}, {}, // 3 partitions
					},
					Error: nil,
				},
				{
					Name:  "topic-3",
					Error: nil,
				},
			},
		}, nil)
	// ignore topic error
	result, err = admin.GetTopicsMeta(ctx, []string{"topic-1", "topic-2", "topic-3"}, true)
	require.NoError(t, err)
	require.Len(t, result, 2)
	require.Equal(t, int32(3), result["topic-2"].NumPartitions)
	require.Equal(t, int32(0), result["topic-3"].NumPartitions)
}

func TestCreateTopic(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	admin, client := newClusterAdminClientWithMock(t)

	client.EXPECT().CreateTopics(gomock.Any(), gomock.Any()).
		Return(nil, fmt.Errorf("kafka.(*Client).CreateTopics"))
	err := admin.CreateTopic(ctx, &pkafka.TopicDetail{
		Name:              "topic-1",
		NumPartitions:     1,
		ReplicationFactor: 1,
	}, false)
	require.Error(t, err)

	client.EXPECT().CreateTopics(gomock.Any(), gomock.Any()).
		Return(&kafka.CreateTopicsResponse{
			Errors: map[string]error{
				"topic-1": errors.New("topic-1 error"),
			},
		}, nil)

	err = admin.CreateTopic(ctx, &pkafka.TopicDetail{
		Name:              "topic-1",
		NumPartitions:     1,
		ReplicationFactor: 1,
	}, false)
	require.Error(t, err, "topic-1 error")

	client.EXPECT().CreateTopics(gomock.Any(), gomock.Any()).
		Return(&kafka.CreateTopicsResponse{}, nil)
	err = admin.CreateTopic(ctx, &pkafka.TopicDetail{
		Name:              "topic-1",
		NumPartitions:     1,
		ReplicationFactor: 1,
	}, false)
	require.NoError(t, err)
}

type fr struct{}

func (f fr) RoundTrip(context.Context, net.Addr, kafka.Request) (kafka.Response, error) {
	return nil, nil
}

func TestAdminClose(t *testing.T) {
	client := mock.NewMockGokrb5v8Client(gomock.NewController(t))
	ad := &admin{}
	ad.Close()
	ad = &admin{
		client: &kafka.Client{},
	}
	ad.Close()
	ad = &admin{
		client: &kafka.Client{
			Transport: fr{},
		},
	}
	ad.Close()
	ad = &admin{
		client: &kafka.Client{
			Transport: &kafka.Transport{
				SASL: plain.Mechanism{},
			},
		},
	}
	ad.Close()
	ad = &admin{
		client: &kafka.Client{
			Transport: &kafka.Transport{
				SASL: mechanism{client: client},
			},
		},
	}
	client.EXPECT().Destroy()
	ad.Close()
}
