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

package kafka

// ClientMockImpl is a mock implementation of Client interface.
type ClientMockImpl struct {
	topics map[string][]int32
}

// NewClientMockImpl creates a new ClientMockImpl instance.
func NewClientMockImpl() *ClientMockImpl {
	topics := make(map[string][]int32)
	topics[DefaultMockTopicName] = []int32{0, 1, 2}
	return &ClientMockImpl{
		topics: topics,
	}
}

// Partitions returns the partitions of the given topic.
func (c *ClientMockImpl) Partitions(topic string) ([]int32, error) {
	return c.topics[topic], nil
}

// Topics returns the all topics.
func (c *ClientMockImpl) Topics() ([]string, error) {
	var topics []string
	for topic := range c.topics {
		topics = append(topics, topic)
	}
	return topics, nil
}

// AddTopic adds a topic.
func (c *ClientMockImpl) AddTopic(topicName string, partitions int32) {
	p := make([]int32, partitions)
	for i := int32(0); i < partitions; i++ {
		p[i] = i
	}
	c.topics[topicName] = p
}

// DeleteTopic deletes a topic.
func (c *ClientMockImpl) DeleteTopic(topicName string) {
	delete(c.topics, topicName)
}
