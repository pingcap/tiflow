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

package manager

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/pingcap/tiflow/pkg/config"
)

// pulsarTopicManager is a manager for pulsar topics.
type pulsarTopicManagerMock struct {
	*pulsarTopicManager
}

// NewMockPulsarTopicManager creates a new topic manager.
func NewMockPulsarTopicManager(
	cfg *config.PulsarConfig,
	client pulsar.Client,
) (TopicManager, error) {
	mgr := &pulsarTopicManagerMock{}
	return mgr, nil
}

// GetPartitionNum spend more time,but no use.
// mock 3 partitions
func (m *pulsarTopicManagerMock) GetPartitionNum(ctx context.Context, topic string) (int32, error) {
	return 3, nil
}
