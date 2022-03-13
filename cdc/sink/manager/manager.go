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

package manager

// TopicManager is the interface of topic manager.
// It will be responsible for creating and
// updating the information of the topic.
type TopicManager interface {
	// Partitions returns the partitions of the topic.
	Partitions(topic string) (int32, error)
	// CreateTopic creates the topic.
	CreateTopic(topicName string) error
}
