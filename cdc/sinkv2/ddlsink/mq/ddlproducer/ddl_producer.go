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

package ddlproducer

import (
	"context"

	"github.com/Shopify/sarama"
	"github.com/pingcap/tiflow/cdc/sink/mq/codec"
)

// DDLProducer is the interface for DDL message producer.
type DDLProducer interface {
	// SyncBroadcastMessage broadcasts a message synchronously.
	SyncBroadcastMessage(
		ctx context.Context, topic string, totalPartitionsNum int32, message *codec.MQMessage,
	) error
	// SyncSendMessage sends a message for a partition synchronously.
	SyncSendMessage(
		ctx context.Context, topic string, partitionNum int32, message *codec.MQMessage,
	) error
	// Close closes the producer.
	Close()
}

// Factory is a function to create a producer.
type Factory func(ctx context.Context, client sarama.Client) (DDLProducer, error)
