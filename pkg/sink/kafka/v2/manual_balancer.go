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

import "github.com/segmentio/kafka-go"

// todo: after implement the producer, remove the `unused lint`.
//
//nolint:unused
type manualPartitioner struct{}

//nolint:unused
func newManualPartitioner() kafka.Balancer {
	return &manualPartitioner{}
}

//nolint:unused
func (m *manualPartitioner) Balance(msg kafka.Message, partitions ...int) (partition int) {
	return msg.Partition
}
