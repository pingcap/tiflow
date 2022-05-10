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

package pulsar

// TopicManager is the interface
// that wraps the basic Pulsar topic management operations.
// Right now it doesn't have any implementation,
// Pulsar doesn't support multiple topics yet.
// So it now just returns a fixed number of partitions for a fixed topic.
type TopicManager struct {
	partitionNum int32
}

// NewTopicManager creates a new TopicManager.
func NewTopicManager(partitionNum int32) *TopicManager {
	return &TopicManager{
		partitionNum: partitionNum,
	}
}

// GetPartitionNum returns the number of partitions of the topic.
func (m *TopicManager) GetPartitionNum(_ string) (int32, error) {
	return m.partitionNum, nil
}

// CreateTopic do nothing.
func (m *TopicManager) CreateTopic(_ string) (int32, error) {
	return m.partitionNum, nil
}
