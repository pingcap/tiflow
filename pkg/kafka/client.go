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

// Client is a generic Kafka client.
type Client interface {
	// Topics returns the set of available
	// topics as retrieved from cluster metadata.
	Topics() ([]string, error)
	// Partitions returns the sorted list of
	// all partition IDs for the given topic.
	Partitions(topic string) ([]int32, error)
}
