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
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/tiflow/pkg/errors"
	mock "github.com/pingcap/tiflow/pkg/sink/kafka/v2/mock"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
)

func newClusterAdminClientWithMock(t *testing.T) (*admin, *mock.MockClient) {
	admin := NewClusterAdminClient([]string{"127.0.0.1:9092"})

	ctrl := gomock.NewController(t)
	client := mock.NewMockClient(ctrl)
	admin.client = client
	return admin, client
}

func TestNewClusterAdminClient(t *testing.T) {
	t.Parallel()

	admin := NewClusterAdminClient([]string{"127.0.0.1:9092"})
	require.NotNil(t, admin)
	require.NotNil(t, admin.client)
}

func TestGetAllBrokers(t *testing.T) {
	t.Parallel()

	admin, client := newClusterAdminClientWithMock(t)
	client.EXPECT().Metadata(gomock.Any(), gomock.Any()).Return(nil,
		fmt.Errorf("kafka.(*Client).Metadata"))

	ctx := context.Background()
	brokers, err := admin.GetAllBrokers(ctx)
	require.Error(t, err)
	require.Nil(t, brokers)

	client.EXPECT().Metadata(gomock.Any(), gomock.Any()).Return(&kafka.MetadataResponse{}, nil)
	brokers, err = admin.GetAllBrokers(ctx)
	require.NoError(t, err)
	require.NotNil(t, brokers)
	require.Len(t, brokers, 0)

	client.EXPECT().Metadata(gomock.Any(), gomock.Any()).Return(&kafka.MetadataResponse{
		Brokers: []kafka.Broker{
			{ID: 1},
		},
	}, nil)
	brokers, err = admin.GetAllBrokers(ctx)
	require.Len(t, brokers, 1)
	require.Equal(t, int32(1), brokers[0].ID)
}

func TestGetCoordinator(t *testing.T) {
	t.Parallel()

	admin, client := newClusterAdminClientWithMock(t)
	client.EXPECT().Metadata(gomock.Any(), gomock.Any()).Return(nil,
		fmt.Errorf("kafka.(*Client).Metadata"))

	ctx := context.Background()
	coordinatorID, err := admin.GetCoordinator(ctx)
	require.Error(t, err)
	require.Equal(t, 0, coordinatorID)

	client.EXPECT().Metadata(gomock.Any(), gomock.Any()).Return(&kafka.MetadataResponse{
		Controller: kafka.Broker{ID: 1},
	}, nil)

	coordinatorID, err = admin.GetCoordinator(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, coordinatorID)
}

func TestGetBrokerConfig(t *testing.T) {
	t.Parallel()

	admin, client := newClusterAdminClientWithMock(t)
	client.EXPECT().Metadata(gomock.Any(), gomock.Any()).Return(nil,
		fmt.Errorf("kafka.(*Client).Metadata"))

	ctx := context.Background()
	result, err := admin.GetBrokerConfig(ctx, "test-config-name")
	require.Error(t, err)
	require.Equal(t, "", result)

	client.EXPECT().Metadata(gomock.Any(), gomock.Any()).Return(&kafka.MetadataResponse{
		Controller: kafka.Broker{ID: 1},
	}, nil).AnyTimes()

	client.EXPECT().DescribeConfigs(gomock.Any(), gomock.Any()).Return(nil,
		fmt.Errorf("kafka.(*Client).DescribeConfigs"))
	result, err = admin.GetBrokerConfig(ctx, "test-config-name")
	require.Error(t, err)
	require.Equal(t, "", result)

	client.EXPECT().DescribeConfigs(gomock.Any(), gomock.Any()).Return(&kafka.DescribeConfigsResponse{
		Resources: []kafka.DescribeConfigResponseResource{},
	}, nil)
	result, err = admin.GetBrokerConfig(ctx, "test-config-name")
	require.Error(t, err, errors.ErrKafkaBrokerConfigNotFound)
	require.Equal(t, "", result)

	client.EXPECT().DescribeConfigs(gomock.Any(), gomock.Any()).Return(&kafka.DescribeConfigsResponse{
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
}

func TestGetAllTopicsMeta(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	admin, client := newClusterAdminClientWithMock(t)
	client.EXPECT().Metadata(gomock.Any(), gomock.Any()).Return(nil,
		fmt.Errorf("kafka.(*Client).Metadata"))

	result, err := admin.GetAllTopicsMeta(ctx)
	require.Error(t, err)
	require.Nil(t, result)

	client.EXPECT().Metadata(gomock.Any(), gomock.Any()).Return(&kafka.MetadataResponse{}, nil)
	client.EXPECT().DescribeConfigs(gomock.Any(), gomock.Any()).Return(nil,
		fmt.Errorf("kafka.(*Client).DescribeConfigs"))

	result, err = admin.GetAllTopicsMeta(ctx)
	require.Error(t, err)
	require.Nil(t, result)

	client.EXPECT().Metadata(gomock.Any(), gomock.Any()).Return(&kafka.MetadataResponse{
		Topics: []kafka.Topic{
			{
				Name: "topic-1",
				Partitions: []kafka.Partition{
					{
						Replicas: []kafka.Broker{
							{ID: 1},
						},
					},
				},
			},
			{
				Name: "topic-2",
				Partitions: []kafka.Partition{
					{
						Replicas: []kafka.Broker{
							{ID: 1},
							{ID: 2},
						},
					},
					{
						Replicas: []kafka.Broker{
							{ID: 1},
							{ID: 2},
						},
					},
				},
			},
			{
				Name: "topic-3",
				Partitions: []kafka.Partition{
					{
						Replicas: []kafka.Broker{
							{ID: 1},
							{ID: 2},
						},
					},
					{
						Replicas: []kafka.Broker{
							{ID: 1},
							{ID: 3},
						},
					},
					{
						Replicas: []kafka.Broker{
							{ID: 2},
							{ID: 3},
						},
					},
				},
			},
		},
	}, nil).AnyTimes()

	client.EXPECT().DescribeConfigs(gomock.Any(), gomock.Any()).Return(nil,
		fmt.Errorf("kafka.(*Client).DescribeConfigs"))

	result, err = admin.GetAllTopicsMeta(ctx)
	require.Error(t, err)
	require.Nil(t, result)

	client.EXPECT().DescribeConfigs(gomock.Any(), gomock.Any()).Return(&kafka.DescribeConfigsResponse{}, nil)

	result, err = admin.GetAllTopicsMeta(ctx)
	require.NoError(t, err)
	require.Equal(t, 3, len(result))

	require.Equal(t, "topic-1", result["topic-1"].Name)
	require.Equal(t, int32(1), result["topic-1"].NumPartitions)
	require.Equal(t, int16(1), result["topic-1"].ReplicationFactor)

	require.Equal(t, "topic-2", result["topic-2"].Name)
	require.Equal(t, int32(2), result["topic-2"].NumPartitions)
	require.Equal(t, int16(2), result["topic-2"].ReplicationFactor)

	require.Equal(t, "topic-3", result["topic-3"].Name)
	require.Equal(t, int32(3), result["topic-3"].NumPartitions)
	require.Equal(t, int16(2), result["topic-3"].ReplicationFactor)

	client.EXPECT().DescribeConfigs(gomock.Any(), gomock.Any()).Return(&kafka.DescribeConfigsResponse{
		Resources: []kafka.DescribeConfigResponseResource{
			{
				ResourceName: "undesired-resource",
			},
		},
	}, nil)

	result, err = admin.GetAllTopicsMeta(ctx)
	require.Error(t, err, "undesired topic found from the response")

	client.EXPECT().DescribeConfigs(gomock.Any(), gomock.Any()).Return(&kafka.DescribeConfigsResponse{
		Resources: []kafka.DescribeConfigResponseResource{
			{
				ResourceName: "topic-1",
				ConfigEntries: []kafka.DescribeConfigResponseConfigEntry{
					{
						IsDefault: true,
					},
					{
						IsSensitive: true,
					},
					{
						ConfigName:  "config-1",
						ConfigValue: "config-1-value",
					},
				},
			},
		},
	}, nil)

	result, err = admin.GetAllTopicsMeta(ctx)
	require.NoError(t, err)
	require.Equal(t, 3, len(result))

	require.Equal(t, "topic-1", result["topic-1"].Name)
	require.Equal(t, int32(1), result["topic-1"].NumPartitions)
	require.Equal(t, int16(1), result["topic-1"].ReplicationFactor)
	require.Len(t, result["topic-1"].ConfigEntries, 1)
	require.Contains(t, result["topic-1"].ConfigEntries, "config-1")
	require.Equal(t, "config-1-value", result["topic-1"].ConfigEntries["config-1"])
}

//func TestGetTopicMeta(t *testing.T) {
//	t.Parallel()
//
//	admin, client := newClusterAdminClientWithMock(t)
//}
//
//func TestCreateTopic(t *testing.T) {
//	t.Parallel()
//
//	admin, client := newClusterAdminClientWithMock(t)
//}
