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
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/sink/kafka"
	"go.uber.org/zap"
)

// Assert DDLEventSink implementation
var _ DDLProducer = (*kafkaDDLProducer)(nil)

// kafkaDDLProducer is used to send messages to kafka synchronously.
type kafkaDDLProducer struct {
	// id indicates this sink belongs to which processor(changefeed).
	id model.ChangeFeedID
	// syncProducer is used to send messages to kafka synchronously.
	syncProducer kafka.SyncProducer
	// closedMu is used to protect `closed`.
	// We need to ensure that closed producers are never written to.
	closedMu sync.RWMutex
	// closed is used to indicate whether the producer is closed.
	// We also use it to guard against double closes.
	closed bool
}

// NewKafkaDDLProducer creates a new kafka producer for replicating DDL.
func NewKafkaDDLProducer(ctx context.Context,
	changefeedID model.ChangeFeedID,
	syncProducer kafka.SyncProducer,
) DDLProducer {
	return &kafkaDDLProducer{
		id:           changefeedID,
		syncProducer: syncProducer,
		closed:       false,
	}
}

func (k *kafkaDDLProducer) SyncBroadcastMessage(ctx context.Context, topic string,
	totalPartitionsNum int32, message *common.Message,
) error {
	k.closedMu.RLock()
	defer k.closedMu.RUnlock()

	if k.closed {
		return cerror.ErrKafkaProducerClosed.GenWithStackByArgs()
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		err := k.syncProducer.SendMessages(ctx, topic,
			totalPartitionsNum, message.Key, message.Value)
		return cerror.WrapError(cerror.ErrKafkaSendMessage, err)
	}
}

func (k *kafkaDDLProducer) SyncSendMessage(ctx context.Context, topic string,
	partitionNum int32, message *common.Message,
) error {
	k.closedMu.RLock()
	defer k.closedMu.RUnlock()

	if k.closed {
		return cerror.ErrKafkaProducerClosed.GenWithStackByArgs()
	}

	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	default:
		err := k.syncProducer.SendMessage(ctx, topic,
			partitionNum, message.Key, message.Value)
		return cerror.WrapError(cerror.ErrKafkaSendMessage, err)
	}
}

func (k *kafkaDDLProducer) Close() {
	// We have to hold the lock to prevent write to closed producer.
	k.closedMu.Lock()
	defer k.closedMu.Unlock()
	// If the producer was already closed, we should skip the close operation.
	if k.closed {
		// We need to guard against double closed the clients,
		// which could lead to panic.
		log.Warn("Kafka DDL producer already closed",
			zap.String("namespace", k.id.Namespace),
			zap.String("changefeed", k.id.ID))
		return
	}
	k.closed = true

	if k.syncProducer != nil {
		k.syncProducer.Close()
	}
}
