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

package codec

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"go.uber.org/zap"
)

const (
	bootstrapWorkerTickerInterval = 5 * time.Second
	bootstrapWorkerGCInterval     = 30 * time.Second

	defaultMaxInactiveDuration = 30 * time.Minute
)

// bootstrapWorker is used to send bootstrap message to the MQ sink worker.
// It will be only used in simple protocol.
type bootstrapWorker struct {
	changefeedID                model.ChangeFeedID
	activeTables                sync.Map
	encoder                     RowEventEncoder
	sendBootstrapInterval       time.Duration
	sendBootstrapInMsgCount     int32
	sendBootstrapToAllPartition bool
	// maxInactiveDuration is the max duration that a table can be inactive
	maxInactiveDuration time.Duration
	outCh               chan<- *future
}

// newBootstrapWorker creates a new bootstrapWorker instance
func newBootstrapWorker(
	changefeedID model.ChangeFeedID,
	outCh chan<- *future,
	encoder RowEventEncoder,
	sendBootstrapInterval int64,
	sendBootstrapInMsgCount int32,
	sendBootstrapToAllPartition bool,
	maxInactiveDuration time.Duration,
) *bootstrapWorker {
	log.Info("Sending bootstrap event is enabled for simple protocol. "+
		"Both send-bootstrap-interval-in-sec and send-bootstrap-in-msg-count are > 0.",
		zap.Stringer("changefeed", changefeedID),
		zap.Int64("sendBootstrapIntervalInSec", sendBootstrapInterval),
		zap.Int32("sendBootstrapInMsgCount", sendBootstrapInMsgCount))
	return &bootstrapWorker{
		changefeedID:                changefeedID,
		outCh:                       outCh,
		encoder:                     encoder,
		activeTables:                sync.Map{},
		sendBootstrapInterval:       time.Duration(sendBootstrapInterval) * time.Second,
		sendBootstrapInMsgCount:     sendBootstrapInMsgCount,
		sendBootstrapToAllPartition: sendBootstrapToAllPartition,
		maxInactiveDuration:         maxInactiveDuration,
	}
}

func (b *bootstrapWorker) run(ctx context.Context) error {
	sendTicker := time.NewTicker(bootstrapWorkerTickerInterval)
	gcTicker := time.NewTicker(bootstrapWorkerGCInterval)
	defer func() {
		gcTicker.Stop()
		sendTicker.Stop()
	}()

	var err error
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-sendTicker.C:
			b.activeTables.Range(func(key, value interface{}) bool {
				table := value.(*tableStatistic)
				err = b.sendBootstrapMsg(ctx, table)
				return err == nil
			})
			if err != nil {
				return errors.Trace(err)
			}
		case <-gcTicker.C:
			b.gcInactiveTables()
		}
	}
}

func (b *bootstrapWorker) addEvent(
	ctx context.Context,
	key model.TopicPartitionKey,
	row *model.RowChangedEvent,
) error {
	table, ok := b.activeTables.Load(row.PhysicalTableID)
	if !ok {
		tb := newTableStatistic(key, row)
		b.activeTables.Store(tb.id, tb)
		// Send bootstrap message immediately when a new table is added
		err := b.sendBootstrapMsg(ctx, tb)
		if err != nil {
			return errors.Trace(err)
		}
	} else {
		// If the table is already in the activeTables, update its status.
		table.(*tableStatistic).update(row, key.TotalPartition)
	}
	return nil
}

// sendBootstrapMsg sends a bootstrap message if the table meets the condition
// 1. The time since last bootstrap message sent is larger than sendBootstrapInterval
// 2. The received row event count since last bootstrap message sent is larger than sendBootstrapInMsgCount
// Note: It is a blocking method, it will block if the outCh is full.
func (b *bootstrapWorker) sendBootstrapMsg(ctx context.Context, table *tableStatistic) error {
	if !table.shouldSendBootstrapMsg(
		b.sendBootstrapInterval,
		b.sendBootstrapInMsgCount) {
		return nil
	}
	table.reset()
	tableInfo := table.tableInfo.Load().(*model.TableInfo)
	events, err := b.generateEvents(table.topic, table.totalPartition.Load(), tableInfo)
	if err != nil {
		return errors.Trace(err)
	}
	for _, e := range events {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case b.outCh <- e:
		}
	}
	return nil
}

func (b *bootstrapWorker) generateEvents(
	topic string,
	totalPartition int32,
	tableInfo *model.TableInfo,
) ([]*future, error) {
	res := make([]*future, 0, totalPartition)
	msg, err := b.encoder.EncodeDDLEvent(model.NewBootstrapDDLEvent(tableInfo))
	if err != nil {
		return nil, errors.Trace(err)
	}
	// If sendBootstrapToAllPartition is true, send bootstrap message to all partition
	// Otherwise, send bootstrap message to partition 0.
	if !b.sendBootstrapToAllPartition {
		totalPartition = 1
	}
	for i := int32(0); i < totalPartition; i++ {
		f := &future{
			Key: model.TopicPartitionKey{
				Topic:     topic,
				Partition: i,
			},
			done:     make(chan struct{}),
			Messages: []*common.Message{msg},
		}
		close(f.done)
		res = append(res, f)
	}
	return res, nil
}

func (b *bootstrapWorker) gcInactiveTables() {
	b.activeTables.Range(func(key, value interface{}) bool {
		table := value.(*tableStatistic)
		if table.isInactive(b.maxInactiveDuration) {
			log.Info("A table is removed from the bootstrap worker",
				zap.Int64("tableID", table.id),
				zap.String("topic", table.topic),
				zap.Stringer("changefeed", b.changefeedID))
			b.activeTables.Delete(key)
		}
		return true
	})
}

// tableStatistic is used to record the statistics of a table
type tableStatistic struct {
	// id is the table's ID, it will not change
	id int64
	// topic is the table's topic, it will not change
	topic string
	// totalPartition is the partition number of the corresponding topic
	totalPartition atomic.Int32
	// counter is the number of row event sent since last bootstrap message sent
	// It is used to check if the bootstrap message should be sent
	counter atomic.Int32
	// lastMsgReceivedTime is the last time the row event is received
	// It is used to check if the table is inactive
	lastMsgReceivedTime atomic.Value
	// lastSendTime is the last time the bootstrap message is sent
	// It is used to check if the bootstrap message should be sent
	lastSendTime atomic.Value
	// version is the table version
	// It is used to check if the table schema is changed since last bootstrap message sent
	version atomic.Uint64
	// tableInfo is the tableInfo of the table
	// It is used to generate bootstrap message
	tableInfo atomic.Value
}

func newTableStatistic(key model.TopicPartitionKey, row *model.RowChangedEvent) *tableStatistic {
	res := &tableStatistic{
		id:    row.PhysicalTableID,
		topic: key.Topic,
	}
	res.totalPartition.Store(key.TotalPartition)
	res.counter.Add(1)
	res.lastMsgReceivedTime.Store(time.Now())
	res.lastSendTime.Store(time.Unix(0, 0))
	res.version.Store(row.TableInfo.UpdateTS)
	res.tableInfo.Store(row.TableInfo)
	return res
}

func (t *tableStatistic) shouldSendBootstrapMsg(
	sendBootstrapInterval time.Duration,
	sendBootstrapMsgCountInterval int32,
) bool {
	lastSendTime := t.lastSendTime.Load().(time.Time)
	return time.Since(lastSendTime) >= sendBootstrapInterval ||
		t.counter.Load() >= sendBootstrapMsgCountInterval
}

func (t *tableStatistic) update(row *model.RowChangedEvent, totalPartition int32) {
	t.counter.Add(1)
	t.lastMsgReceivedTime.Store(time.Now())

	// Note(dongmen): Rename Table DDL is a special case,
	// the TableInfo.Name is changed but the TableInfo.UpdateTs is not changed.
	if t.version.Load() != row.TableInfo.UpdateTS ||
		t.tableInfo.Load().(*model.TableInfo).Name != row.TableInfo.Name {
		t.version.Store(row.TableInfo.UpdateTS)
		t.tableInfo.Store(row.TableInfo)
	}
	if t.totalPartition.Load() != totalPartition {
		t.totalPartition.Store(totalPartition)
	}
}

func (t *tableStatistic) isInactive(maxInactiveDuration time.Duration) bool {
	lastMsgReceivedTime := t.lastMsgReceivedTime.Load().(time.Time)
	return time.Since(lastMsgReceivedTime) > maxInactiveDuration
}

func (t *tableStatistic) reset() {
	t.lastSendTime.Store(time.Now())
	t.counter.Store(0)
}
