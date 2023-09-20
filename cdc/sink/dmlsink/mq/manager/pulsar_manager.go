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
	"sync"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/pingcap/tiflow/pkg/config"
)

// PulsarTopicManager is a manager for pulsar topics.
type PulsarTopicManager func(
	cfg *config.PulsarConfig,
	client pulsar.Client,
) (TopicManager, error)

// pulsarTopicManager is a manager for pulsar topics.
type pulsarTopicManager struct {
	client     pulsar.Client
	partitions sync.Map // key : topic, value : partition-name
	cfg        *config.PulsarConfig
}

// NewPulsarTopicManager creates a new topic manager.
func NewPulsarTopicManager(
	cfg *config.PulsarConfig,
	client pulsar.Client,
) (TopicManager, error) {
	mgr := &pulsarTopicManager{
		client:     client,
		cfg:        cfg,
		partitions: sync.Map{},
	}

	return mgr, nil
}

// GetPartitionNum  always return 1 because we pass a message key to pulsar producer,
// and pulsar producer will hash the key to a partition.
// This method is only used to meet the requirement of mq sink's interface.
func (m *pulsarTopicManager) GetPartitionNum(ctx context.Context, topic string) (int32, error) {
	return 1, nil
}

// CreateTopicAndWaitUntilVisible no need to create first
func (m *pulsarTopicManager) CreateTopicAndWaitUntilVisible(ctx context.Context, topicName string) (int32, error) {
	return 0, nil
}

// Close
func (m *pulsarTopicManager) Close() {
}

// str2Pointer returns the pointer of the string.
func str2Pointer(str string) *string {
	return &str
}
