// Copyright 2020 PingCAP, Inc.
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

	"github.com/pingcap/tiflow/cdc/sink/codec/common"
)

// Producer is an interface of mq producer
type Producer interface {
	// AsyncSendMessage sends a message asynchronously.
	AsyncSendMessage(
		ctx context.Context, topic string, partition int32, message *common.Message,
	) error
	// SyncBroadcastMessage broadcasts a message synchronously.
	SyncBroadcastMessage(
		ctx context.Context, topic string, partitionsNum int32, message *common.Message,
	) error
	// Flush all the messages buffered in the client and wait until all messages have been successfully
	// persisted.
	Flush(ctx context.Context) error
	// Close closes the producer and client(s).
	Close() error
}
