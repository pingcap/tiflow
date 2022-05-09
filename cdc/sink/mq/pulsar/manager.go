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

// topicManager is the interface
// that wraps the basic Pulsar topic management operations.
// Right now it doesn't have any implementation,
// Pulsar doesn't support multiple topics yet.
// So it now just returns a fixed number of partitions for a fixed topic.
type topicManager struct {
	partitionNum int32
}

// NewTopicManager creates a new TopicManager.
func NewTopicManager(partitionNum int32) *topicManager {
	return &topicManager{
		partitionNum: partitionNum,
	}
}

// Partitions returns the number of partitions of the topic.
func (m *topicManager) Partitions(_ string) (int32, error) {
	return m.partitionNum, nil
}

// CreateTopic do nothing.
func (m *topicManager) CreateTopic(_ string) (int32, error) {
	return m.partitionNum, nil
}
