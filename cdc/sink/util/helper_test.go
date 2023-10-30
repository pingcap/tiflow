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

package util

import (
	"context"
	"net/url"
	"testing"

	"github.com/pingcap/tiflow/pkg/sink/kafka"
	"github.com/stretchr/testify/require"
)

func TestPartition(t *testing.T) {
	t.Parallel()

	adminClient := kafka.NewClusterAdminClientMockImpl()
	defer adminClient.Close()

	cfg := &kafka.AutoCreateTopicConfig{
		AutoCreate:        true,
		PartitionNum:      2,
		ReplicationFactor: 1,
	}

	ctx := context.Background()
	manager, err := GetTopicManagerAndTryCreateTopic(ctx, kafka.DefaultMockTopicName, cfg, adminClient)
	require.NoError(t, err)
	defer manager.Close()

	// default topic, real partition is 3, but 2 is set in the sink-uri, so return 2.
	partitionsNum, err := manager.GetPartitionNum(ctx, kafka.DefaultMockTopicName)
	require.NoError(t, err)
	require.Equal(t, int32(2), partitionsNum)

	// new topic, create it with partition number as 2.
	partitionsNum, err = manager.GetPartitionNum(ctx, "new-topic")
	require.NoError(t, err)
	require.Equal(t, int32(2), partitionsNum)

	// assume a topic already exist, the not default topic won't be affected by the default topic's partition number.
	err = adminClient.CreateTopic(ctx, &kafka.TopicDetail{
		Name:          "new-topic-2",
		NumPartitions: 3,
	}, false)
	require.NoError(t, err)

	partitionsNum, err = manager.GetPartitionNum(ctx, "new-topic-2")
	require.NoError(t, err)
	require.Equal(t, int32(3), partitionsNum)
}

func TestGetTopic(t *testing.T) {
	t.Parallel()

	testCases := map[string]struct {
		sinkURI   string
		wantTopic string
		wantErr   string
	}{
		"no topic": {
			sinkURI:   "kafka://localhost:9092/",
			wantTopic: "",
			wantErr:   "no topic is specified in sink-uri",
		},
		"valid topic": {
			sinkURI:   "kafka://localhost:9092/test",
			wantTopic: "test",
			wantErr:   "",
		},
		"topic with query": {
			sinkURI:   "kafka://localhost:9092/test?version=1.0.0",
			wantTopic: "test",
			wantErr:   "",
		},
	}

	for name, tc := range testCases {
		tc := tc
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			sinkURI, err := url.Parse(tc.sinkURI)
			require.NoError(t, err)
			topic, err := GetTopic(sinkURI)
			if tc.wantErr != "" {
				require.Regexp(t, tc.wantErr, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.wantTopic, topic)
			}
		})
	}
}
