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

package codec

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/zap"
)

const (
	bootstrapWorkerTickerInterval = 10 * time.Second
	bootstrapWorkerGCInterval     = 30 * time.Second

	defaultSendBootstrapInterval   = 1 * time.Minute
	defaultSendBootstrapInMsgCount = 1000
	defaultMaxInactiveDuration     = 5 * time.Minute
)

type bootstrapWorker struct {
	activeTables            sync.Map
	builder                 RowEventEncoderBuilder
	sendBootstrapInterval   time.Duration
	sendBootstrapInMsgCount int32
	// maxInactiveDuration is the max duration that a table can be inactive
	maxInactiveDuration time.Duration
	outCh               chan<- *future
}

// newBootstrapWorker creates a new bootstrapGenerator instance
func newBootstrapWorker(
	outCh chan<- *future,
	builder RowEventEncoderBuilder,
	sendBootstrapInterval time.Duration,
	sendBootstrapInMsgCount int32,
	maxInactiveDuration time.Duration,
) *bootstrapWorker {
	return &bootstrapWorker{
		outCh:                   outCh,
		builder:                 builder,
		activeTables:            sync.Map{},
		sendBootstrapInterval:   sendBootstrapInterval,
		sendBootstrapInMsgCount: sendBootstrapInMsgCount,
		maxInactiveDuration:     maxInactiveDuration,
	}
}

func (b *bootstrapWorker) run(ctx context.Context) error {
	log.Info("fizz: bootstrap worker is started")
	sendTicker := time.NewTicker(bootstrapWorkerTickerInterval)
	defer sendTicker.Stop()
	gcTicker := time.NewTicker(bootstrapWorkerGCInterval)
	defer gcTicker.Stop()
	errCh := make(chan error, 1)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-sendTicker.C:
			b.activeTables.Range(func(key, value interface{}) bool {
				table := value.(*tableStatus)
				err := b.sendBootstrapMsg(ctx, table)
				if err != nil {
					errCh <- err
					return false
				}
				return true
			})
		case <-gcTicker.C:
			b.gcInactiveTables()
		case err := <-errCh:
			return err
		}
	}
}

func (b *bootstrapWorker) addEvent(
	ctx context.Context,
	key TopicPartitionKey,
	row *model.RowChangedEvent,
) error {
	table, ok := b.activeTables.Load(row.Table.TableID)
	if !ok {
		tb := newTableStatus(key, row)
		b.activeTables.Store(tb.id, tb)
		// Send bootstrap message immediately when a new table is added
		err := b.sendBootstrapMsg(ctx, tb)
		if err != nil {
			return errors.Trace(err)
		}
		log.Info("fizz: a new table is added to the bootstrap worker, send bootstrap message immediately",
			zap.String("topic", tb.topic),
			zap.Int64("tableID", int64(tb.id)))
	} else {
		// If the table is already in the activeTables, update its status.
		table.(*tableStatus).update(key, row)
	}
	return nil
}

// sendBootstrapMsg sends a bootstrap message if the table meets the condition
// 1. The time since last bootstrap message sent is larger than sendBootstrapInterval
// 2. The received row event count since last bootstrap message sent is larger than sendBootstrapInMsgCount
// Note: It is a blocking method, it will block if the outCh is full.
func (b *bootstrapWorker) sendBootstrapMsg(ctx context.Context, table *tableStatus) error {
	if !table.shouldSendBootstrapMsg(
		b.sendBootstrapInterval,
		b.sendBootstrapInMsgCount) {
		return nil
	}
	table.reset()
	log.Info("fizz: bootstrap worker is sending bootstrap message", zap.Any("table", table.id))
	tableInfo := table.tableInfo.Load().(*model.TableInfo)
	events, err := b.generateEvents(ctx, table.topic, table.totalPartition.Load(), tableInfo)
	if err != nil {
		return errors.Trace(err)
	}
	for _, event := range events {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case b.outCh <- event:
			log.Info("fizz: bootstrap message is sent",
				zap.Any("msgKey", event.Key),
				zap.Any("table", table.id))
		}
	}
	log.Info("fizz: bootstrap message are sent", zap.Any("table", table.id))
	return nil
}

func (b *bootstrapWorker) generateEvents(
	ctx context.Context,
	topic string,
	totalPartition int32,
	tableInfo *model.TableInfo,
) ([]*future, error) {
	res := make([]*future, 0, int(totalPartition))
	// Bootstrap messages of a table should be sent to all partitions.
	for i := 0; i < int(totalPartition); i++ {
		encoder := b.builder.Build()
		msg, err := encoder.EncodeDDLEvent(&model.DDLEvent{
			StartTs:     0,
			CommitTs:    0,
			TableInfo:   tableInfo,
			IsBootstrap: true,
		})
		if err != nil {
			return nil, errors.Trace(err)
		}

		key := TopicPartitionKey{
			Topic:          topic,
			Partition:      int32(i),
			TotalPartition: totalPartition,
		}

		future := &future{
			Key:  key,
			done: make(chan struct{}),
		}
		future.Messages = append(future.Messages, msg)
		close(future.done)
		res = append(res, future)
	}
	return res, nil
}

func (b *bootstrapWorker) gcInactiveTables() {
	b.activeTables.Range(func(key, value interface{}) bool {
		table := value.(*tableStatus)
		if !table.isActive(b.maxInactiveDuration) {
			log.Info("fizz: a table is removed from the bootstrap worker", zap.Any("table", table.id))
			b.activeTables.Delete(key)
		}
		return true
	})
}

// tableStatus is used to record the status of a table
type tableStatus struct {
	// id is the table's ID, it will not change
	id int64
	// topic is the table's topic, it will not change
	topic string
	// All fields below are concurrently accessed, please use atomic operations.
	// totalPartition is the total number of partitions of the table's topic
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

func newTableStatus(key TopicPartitionKey, row *model.RowChangedEvent) *tableStatus {
	res := &tableStatus{
		id:    row.Table.TableID,
		topic: key.Topic,
	}
	res.counter.Add(1)
	res.totalPartition.Store(key.TotalPartition)
	res.lastMsgReceivedTime.Store(time.Now())
	res.lastSendTime.Store(time.Unix(0, 0))
	res.version.Store(row.TableInfo.UpdateTS)
	res.tableInfo.Store(row.TableInfo)
	return res
}

func (t *tableStatus) shouldSendBootstrapMsg(
	sendBootstrapInterval time.Duration,
	sendBootstrapMsgCountInterval int32,
) bool {
	lastSendTime := t.lastSendTime.Load().(time.Time)
	return time.Since(lastSendTime) >= sendBootstrapInterval ||
		t.counter.Load() >= sendBootstrapMsgCountInterval
}

func (t *tableStatus) update(key TopicPartitionKey, row *model.RowChangedEvent) {
	t.counter.Add(1)
	t.lastMsgReceivedTime.Store(time.Now())

	if t.totalPartition.Load() != key.TotalPartition {
		t.totalPartition.Store(key.TotalPartition)
	}

	if t.version.Load() != row.TableInfo.UpdateTS {
		t.version.Store(row.TableInfo.UpdateTS)
		t.tableInfo.Store(row.TableInfo)
	}
}

func (t *tableStatus) isActive(maxInactiveDuration time.Duration) bool {
	lastMsgReceivedTime := t.lastMsgReceivedTime.Load().(time.Time)
	return time.Since(lastMsgReceivedTime) < maxInactiveDuration
}

func (t *tableStatus) reset() {
	t.lastSendTime.Store(time.Now())
	t.counter.Store(0)
}
