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
	"go.uber.org/zap"
)

const (
	bootstrapWorkerTickerInterval = 5 * time.Second
	bootstrapWorkerGCInterval     = 30 * time.Second

	defaultMaxInactiveDuration = 30 * time.Minute

	// In the current implementation, the bootstrapWorker only sends bootstrap message
	// to the first partition of the corresponding topic of the table.
	defaultBootstrapPartitionIndex = 0
)

type bootstrapWorker struct {
	changefeedID            model.ChangeFeedID
	activeTables            sync.Map
	encoder                 RowEventEncoder
	sendBootstrapInterval   time.Duration
	sendBootstrapInMsgCount int32
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
	maxInactiveDuration time.Duration,
) *bootstrapWorker {
	log.Info("Sending bootstrap event is enabled for simple protocol,"+
		"and both send-bootstrap-interval-in-sec and send-bootstrap-in-msg-count are > 0"+
		"enable bootstrap sending function",
		zap.Stringer("changefeed", changefeedID),
		zap.Int64("sendBootstrapIntervalInSec", sendBootstrapInterval),
		zap.Int32("sendBootstrapInMsgCount", sendBootstrapInMsgCount))
	return &bootstrapWorker{
		changefeedID:            changefeedID,
		outCh:                   outCh,
		encoder:                 encoder,
		activeTables:            sync.Map{},
		sendBootstrapInterval:   time.Duration(sendBootstrapInterval) * time.Second,
		sendBootstrapInMsgCount: sendBootstrapInMsgCount,
		maxInactiveDuration:     maxInactiveDuration,
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
	table, ok := b.activeTables.Load(row.Table.TableID)
	if !ok {
		tb := newTableStatus(key, row)
		b.activeTables.Store(tb.id, tb)
		// Send bootstrap message immediately when a new table is added
		err := b.sendBootstrapMsg(ctx, tb)
		if err != nil {
			return errors.Trace(err)
		}
	} else {
		// If the table is already in the activeTables, update its status.
		table.(*tableStatistic).update(row)
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
	event, err := b.generateEvents(table.topic, tableInfo)
	if err != nil {
		return errors.Trace(err)
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case b.outCh <- event:
	}
	return nil
}

func (b *bootstrapWorker) generateEvents(
	topic string,
	tableInfo *model.TableInfo,
) (*future, error) {
	// Bootstrap messages of a table should be sent to all partitions.
	msg, err := b.encoder.EncodeDDLEvent(model.NewBootstrapDDLEvent(tableInfo))
	if err != nil {
		return nil, errors.Trace(err)
	}

	key := model.TopicPartitionKey{
		Topic:     topic,
		Partition: defaultBootstrapPartitionIndex,
	}

	f := &future{
		Key:  key,
		done: make(chan struct{}),
	}
	f.Messages = append(f.Messages, msg)
	close(f.done)
	return f, nil
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

func newTableStatus(key model.TopicPartitionKey, row *model.RowChangedEvent) *tableStatistic {
	res := &tableStatistic{
		id:    row.Table.TableID,
		topic: key.Topic,
	}
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

func (t *tableStatistic) update(row *model.RowChangedEvent) {
	t.counter.Add(1)
	t.lastMsgReceivedTime.Store(time.Now())

	if t.version.Load() != row.TableInfo.UpdateTS {
		t.version.Store(row.TableInfo.UpdateTS)
		t.tableInfo.Store(row.TableInfo)
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
