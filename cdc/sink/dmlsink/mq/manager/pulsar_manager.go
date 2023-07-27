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
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/pingcap/log"
	pulsarConfig "github.com/pingcap/tiflow/pkg/sink/pulsar"
	"go.uber.org/zap"
)

type partition struct {
	partitions []string
	since      time.Time
}

// PulsarTopicManager is a manager for pulsar topics.
type PulsarTopicManager func(
	cfg *pulsarConfig.Config,
	client pulsar.Client,
) (TopicManager, error)

// pulsarTopicManager is a manager for pulsar topics.
type pulsarTopicManager struct {
	client     pulsar.Client
	partitions sync.Map // key : topic, value : partition-name
	cfg        *pulsarConfig.Config
}

// NewPulsarTopicManager creates a new topic manager.
func NewPulsarTopicManager(
	cfg *pulsarConfig.Config,
	client pulsar.Client,
) (TopicManager, error) {
	mgr := &pulsarTopicManager{
		client:     client,
		cfg:        cfg,
		partitions: sync.Map{},
	}

	return mgr, nil
}

// GetPartitionNum spend more time,but no use.
func (m *pulsarTopicManager) GetPartitionNum(ctx context.Context, topic string) (int32, error) {
	if v, ok := m.partitions.Load(topic); ok {
		pt, ok := v.(*partition)
		if ok {
			if time.Since(pt.since) > time.Minute {
				m.partitions.Delete(topic)
			}
			return int32(len(pt.partitions)), nil
		}
	}

	partitions, err := m.client.TopicPartitions(topic)
	if err != nil {
		log.L().Error("pulsar GetPartitions fail", zap.Error(err))
		return 0, err
	}
	log.L().Debug("pulsar GetPartitions", zap.Strings("partitions", partitions))
	pt := &partition{
		partitions: partitions,
		since:      time.Now(),
	}
	m.partitions.Store(topic, pt)

	return int32(len(pt.partitions)), nil
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
