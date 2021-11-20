// Copyright 2021 PingCAP, Inc.
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

import (
	"context"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/sink/codec"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/notify"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
	"sync"
	"sync/atomic"
	"time"
)

type yakProducer struct {
	topic string
	partitionNum int32

	// clientLock is used to protect concurrent access of syncWriter and asyncWriter.
	// Since we don't close these two clients (which have an input chan) from the
	// sender routine, data race or send on closed chan could happen.
	clientLock  sync.RWMutex
	syncWriter *kafka.Writer
	asyncWriter *kafka.Writer

	partitionOffset []struct {
		flushed uint64
		sent    uint64
	}

	flushedNotifier *notify.Notifier
	flushedReceiver *notify.Receiver

	failpointCh chan error

	closeCh chan struct{}
	// atomic flag indicating whether the producer is closing
	closing kafkaProducerClosingFlag
	// released records whether asyncWriter and syncWriter have been closed properly
	released bool
}

func newWriter(async bool) *kafka.Writer {
	return &kafka.Writer{
		Addr:         nil,
		Topic:        "",
		Balancer:     nil,
		MaxAttempts:  0,
		BatchSize:    0,
		BatchBytes:   0,
		BatchTimeout: 0,
		ReadTimeout:  0,
		WriteTimeout: 0,
		RequiredAcks: 0,
		Async:        async,
		Completion:   nil,
		Compression:  0,
		Logger:       nil,
		ErrorLogger:  nil,
		Transport:    nil,
	}
}

func NewYakProducer(ctx context.Context, topic string, protocol codec.Protocol, config *Config, errCh chan error) (*yakProducer, error) {
	notifier := new(notify.Notifier)
	flushedReceiver, err := notifier.NewReceiver(50 * time.Millisecond)
	if err != nil {
		return nil, err
	}

	p := &yakProducer{
		topic: topic,
		partitionNum: config.PartitionNum,

		syncWriter: newWriter(false),
		asyncWriter: newWriter(true),

		partitionOffset: make([]struct {
			flushed uint64
			sent    uint64
		}, config.PartitionNum),
		flushedNotifier: notifier,
		flushedReceiver: flushedReceiver,
		closeCh:         make(chan struct{}),
		failpointCh:     make(chan error, 1),
		closing:         kafkaProducerRunning,
	}

	go func() {
		if err := p.run(ctx); err != nil && errors.Cause(err) != context.Canceled {
			select {
			case <-ctx.Done():
				return
			case errCh <- err:
			default:
				log.Error("error channel is full", zap.Error(err))
			}
		}
	}()

	return p, nil
}

func (p *yakProducer) run(ctx context.Context) error {
	defer func() {
		p.flushedReceiver.Stop()
		p.stop()
	}()

	for {
		select {
		case <- ctx.Done():
			return ctx.Err()
		case <- p.closeCh:
			return nil
		case err := <- p.failpointCh:
			log.Warn("receive from failpoint chan", zap.Error(err))
			return err
		// todo: fetch producer commit information from the writer in a proper way.
		// update the flushed offset, also handle commit error.
		default:
			var (
				partition int
				flushedOffset uint64
			)
			atomic.StoreUint64(&p.partitionOffset[partition].flushed, flushedOffset)
			p.flushedNotifier.Notify()
		}
	}
}

func (p *yakProducer) AsyncSendMessage(ctx context.Context, message *codec.MQMessage, partition int32) error {
	p.clientLock.RLock()
	defer p.clientLock.RUnlock()

	// Checks whether the producer is closing.
	// The atomic flag must be checked under `clientLock.RLock()`
	if atomic.LoadInt32(&p.closing) == kafkaProducerClosing {
		return nil
	}

	failpoint.Inject("KafkaSinkAsyncSendError", func() {
		// simulate sending message to input channel successfully but flushing
		// message to Kafka meets error
		log.Info("failpoint error injected")
		p.failpointCh <- errors.New("kafka sink injected error")
		failpoint.Return(nil)
	})

	failpoint.Inject("SinkFlushDMLPanic", func() {
		time.Sleep(time.Second)
		log.Panic("SinkFlushDMLPanic")
	})

	atomic.AddUint64(&p.partitionOffset[partition].sent, 1)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.closeCh:
		return nil
	default:
	}
	if err := p.asyncWriter.WriteMessages(ctx, kafka.Message{
		Topic:         p.topic,
		Partition: int(partition),
		Offset:        0,
		HighWaterMark: 0,
		Key:           message.Key,
		Value:         message.Value,
	}); err != nil {
		return cerror.WrapError(cerror.ErrKafkaAsyncSendMessage, err)
	}
	return nil
}

func (p *yakProducer) SyncBroadcastMessage(ctx context.Context, message *codec.MQMessage) error {
	p.clientLock.RLock()
	defer p.clientLock.Unlock()

	messages := make([]kafka.Message, p.partitionNum)
	for i := 0; i < int(p.partitionNum); i++ {
		messages[i] = kafka.Message{
			Topic:         p.topic,
			Partition:     i,
			Key:           message.Key,
			Value:         message.Value,
		}
	}

	select {
	case <- ctx.Done():
		return ctx.Err()
	case <- p.closeCh:
		return nil
	default:
	}
	err := p.syncWriter.WriteMessages(ctx, messages...)
	if err != nil {
		return cerror.WrapError(cerror.ErrKafkaSendMessage, err)
	}
	return nil
}

func (p *yakProducer) Flush(ctx context.Context) error {
	targetOffest := make([]uint64, p.partitionNum)
	for i := 0; i < len(p.partitionOffset); i++ {
		targetOffest[i] = atomic.LoadUint64(&p.partitionOffset[i].sent)
	}

	allFlushed := true
	for i, target := range targetOffest {
		// there is still some messages sent to kafka producer, but does not flushed to brokers yet.
		if target > atomic.LoadUint64(&p.partitionOffset[i].flushed) {
			allFlushed = false
			break
		}
	}

	if allFlushed {
		return nil
	}

flushLoop:
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-p.closeCh:
			if p.allMessagesFlushed() {
				return nil
			}
			return cerror.ErrKafkaFlushUnfinished.GenWithStackByArgs()
		case <-p.flushedReceiver.C:
			if !p.allMessagesFlushed() {
				continue flushLoop
			}
			return nil
		}
	}

}

func(p *yakProducer) allMessagesFlushed() bool {
	targetOffset := make([]uint64, p.partitionNum)
	for i := 0; i < int(p.partitionNum); i++ {
		targetOffset[i] = atomic.LoadUint64(&p.partitionOffset[i].sent)
	}

	for i, target := range targetOffset {
		// there is still some messages sent to kafka producer, but does not flushed to brokers yet.
		if target > atomic.LoadUint64(&p.partitionOffset[i].flushed) {
			return false
		}
	}

	return true
}

func (p *yakProducer) GetPartitionNum() int32 {
	return p.partitionNum
}

// stop closes the closeCh to signal other routines to exit
// It SHOULD NOT be called under `clientLock`.
func (p *yakProducer) stop() {
	if atomic.SwapInt32(&p.closing, kafkaProducerClosing) == kafkaProducerClosing {
		return
	}
	close(p.closeCh)
}

func (p *yakProducer) Close() error {
	p.stop()

	p.clientLock.Lock()
	defer p.clientLock.Unlock()

	if p.released {
		// We need to guard against double closing the clients,
		// which could lead to panic.
		return nil
	}
	p.released = true

	// In fact close sarama sync client doesn't return any error.
	// But close async client returns error if error channel is not empty, we
	// don't populate this error to the upper caller, just add a log here.
	if err := p.syncWriter.Close();err != nil {
		log.Error("close sync client with error", zap.Error(err))
	}
	if err := p.asyncWriter.Close(); err != nil {
		log.Error("close async client with error", zap.Error(err))
	}
	return nil
}
