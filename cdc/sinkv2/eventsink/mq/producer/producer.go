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

package producer

import (
	"context"

	"github.com/Shopify/sarama"
	"github.com/pingcap/tiflow/cdc/sink/mq/codec"
)

// Producer is the interface for message producer.
type Producer interface {
	// AsyncSendMessage sends a message asynchronously.
	AsyncSendMessage(
		ctx context.Context, topic string, partition int32, message *codec.MQMessage,
	) error

	// Close closes the producer and client(s).
	Close() error
}

// NewProducerFunc is a function to create a producer.
type NewProducerFunc func(ctx context.Context, client sarama.Client,
	errCh chan error) (Producer, error)
