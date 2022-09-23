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

package dmlproducer

import (
	"context"

	"github.com/Shopify/sarama"
	"github.com/pingcap/tiflow/cdc/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/sink/kafka"
)

// DMLProducer is the interface for message producer.
type DMLProducer interface {
	// AsyncSendMessage sends a message asynchronously.
	AsyncSendMessage(
		ctx context.Context, topic string, partition int32, message *common.Message,
	) error

	// Close closes the producer and client(s).
	Close()
}

// Factory is a function to create a producer.
// errCh is used to report error to the caller(i.e. processor,owner).
// Because the caller passes errCh to many goroutines,
// there is no way to safely close errCh by the sender.
// So we let the GC close errCh.
// It's usually a buffered channel.
type Factory func(ctx context.Context, client sarama.Client,
	adminClient kafka.ClusterAdminClient, errCh chan error) (DMLProducer, error)
