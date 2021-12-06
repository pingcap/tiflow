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

package sink

import (
	"context"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/sink/codec"
	"github.com/pingcap/ticdc/cdc/sink/dispatcher"
	"github.com/pingcap/ticdc/cdc/sink/producer"
	"github.com/pingcap/ticdc/cdc/sink/producer/kafka"
	"github.com/pingcap/ticdc/cdc/sink/producer/pulsar"
	"github.com/pingcap/ticdc/pkg/config"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/notify"
	"github.com/pingcap/ticdc/pkg/security"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type mqEvent struct {
	row        *model.RowChangedEvent
	resolvedTs uint64
}

const (
	defaultPartitionInputChSize = 12800
	// -1 means broadcast to all partitions, it's the default for the default open protocol.
	defaultDDLDispatchPartition = -1
)

type mqSink struct {
	mqProducer     producer.Producer
	dispatcher     dispatcher.Dispatcher
	encoderBuilder codec.EncoderBuilder
	filter         *filter.Filter
	protocol       config.Protocol

	partitionNum        int32
	partitionInput      []chan mqEvent
	partitionResolvedTs []uint64
	tableCheckpointTs   map[model.TableID]uint64
	resolvedNotifier    *notify.Notifier
	resolvedReceiver    *notify.Receiver

	statistics *Statistics
}

func newMqSink(
	ctx context.Context, credential *security.Credential, mqProducer producer.Producer,
	filter *filter.Filter, replicaConfig *config.ReplicaConfig, opts map[string]string, errCh chan error,
) (*mqSink, error) {
	var protocol config.Protocol
	protocol.FromString(replicaConfig.Sink.Protocol)
	encoderBuilder, err := codec.NewEventBatchEncoderBuilder(protocol, credential, opts)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaInvalidConfig, err)
	}
	// pre-flight verification of encoder parameters
	if _, err := encoderBuilder.Build(ctx); err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaInvalidConfig, err)
	}

	partitionNum := mqProducer.GetPartitionNum()
	d, err := dispatcher.NewDispatcher(replicaConfig, partitionNum)
	if err != nil {
		return nil, errors.Trace(err)
	}

	partitionInput := make([]chan mqEvent, partitionNum)
	for i := 0; i < int(partitionNum); i++ {
		partitionInput[i] = make(chan mqEvent, defaultPartitionInputChSize)
	}

	notifier := new(notify.Notifier)
	resolvedReceiver, err := notifier.NewReceiver(50 * time.Millisecond)
	if err != nil {
		return nil, errors.Trace(err)
	}

	s := &mqSink{
		mqProducer:     mqProducer,
		dispatcher:     d,
		encoderBuilder: encoderBuilder,
		filter:         filter,
		protocol:       protocol,

		partitionNum:        partitionNum,
		partitionInput:      partitionInput,
		partitionResolvedTs: make([]uint64, partitionNum),
		tableCheckpointTs:   make(map[model.TableID]uint64),
		resolvedNotifier:    notifier,
		resolvedReceiver:    resolvedReceiver,

		statistics: NewStatistics(ctx, "MQ", opts),
	}

	go func() {
		if err := s.run(ctx); err != nil && errors.Cause(err) != context.Canceled {
			select {
			case <-ctx.Done():
				return
			case errCh <- err:
			default:
				log.Error("error channel is full", zap.Error(err))
			}
		}
	}()
	return s, nil
}

func (k *mqSink) EmitRowChangedEvents(ctx context.Context, rows ...*model.RowChangedEvent) error {
	rowsCount := 0
	for _, row := range rows {
		if k.filter.ShouldIgnoreDMLEvent(row.StartTs, row.Table.Schema, row.Table.Table) {
			log.Info("Row changed event ignored", zap.Uint64("start-ts", row.StartTs))
			continue
		}
		partition := k.dispatcher.Dispatch(row)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case k.partitionInput[partition] <- struct {
			row        *model.RowChangedEvent
			resolvedTs uint64
		}{row: row}:
		}
		rowsCount++
	}
	k.statistics.AddRowsCount(rowsCount)
	return nil
}

func (k *mqSink) FlushRowChangedEvents(ctx context.Context, tableID model.TableID, resolvedTs uint64) (uint64, error) {
	if checkpointTs, ok := k.tableCheckpointTs[tableID]; ok && resolvedTs <= checkpointTs {
		return checkpointTs, nil
	}

	for i := 0; i < int(k.partitionNum); i++ {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case k.partitionInput[i] <- struct {
			row        *model.RowChangedEvent
			resolvedTs uint64
		}{resolvedTs: resolvedTs}:
		}
	}

	// waiting for all row events are sent to mq producer
flushLoop:
	for {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-k.resolvedReceiver.C:
			for i := 0; i < int(k.partitionNum); i++ {
				if resolvedTs > atomic.LoadUint64(&k.partitionResolvedTs[i]) {
					continue flushLoop
				}
			}
			break flushLoop
		}
	}
	err := k.mqProducer.Flush(ctx)
	if err != nil {
		return 0, errors.Trace(err)
	}
	k.tableCheckpointTs[tableID] = resolvedTs
	k.statistics.PrintStatus(ctx)
	return resolvedTs, nil
}

func (k *mqSink) EmitCheckpointTs(ctx context.Context, ts uint64) error {
	encoder, err := k.encoderBuilder.Build(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	msg, err := encoder.EncodeCheckpointEvent(ts)
	if err != nil {
		return errors.Trace(err)
	}
	if msg == nil {
		return nil
	}
	err = k.writeToProducer(ctx, msg, codec.EncoderNeedSyncWrite, -1)
	return errors.Trace(err)
}

func (k *mqSink) EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	if k.filter.ShouldIgnoreDDLEvent(ddl.StartTs, ddl.Type, ddl.TableInfo.Schema, ddl.TableInfo.Table) {
		log.Info(
			"DDL event ignored",
			zap.String("query", ddl.Query),
			zap.Uint64("startTs", ddl.StartTs),
			zap.Uint64("commitTs", ddl.CommitTs),
		)
		return cerror.ErrDDLEventIgnored.GenWithStackByArgs()
	}
	encoder, err := k.encoderBuilder.Build(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	msg, err := encoder.EncodeDDLEvent(ddl)
	if err != nil {
		return errors.Trace(err)
	}

	if msg == nil {
		return nil
	}

	var partition int32 = defaultDDLDispatchPartition
	// for Canal-JSON / Canal-PB, send to partition 0.
	if _, ok := encoder.(*codec.CanalFlatEventBatchEncoder); ok {
		partition = 0
	}
	if _, ok := encoder.(*codec.CanalEventBatchEncoder); ok {
		partition = 0
	}

	k.statistics.AddDDLCount()
	log.Debug("emit ddl event", zap.String("query", ddl.Query), zap.Uint64("commit-ts", ddl.CommitTs), zap.Int32("partition", partition))
	err = k.writeToProducer(ctx, msg, codec.EncoderNeedSyncWrite, partition)
	return errors.Trace(err)
}

func (k *mqSink) Close(ctx context.Context) error {
	err := k.mqProducer.Close()
	return errors.Trace(err)
}

func (k *mqSink) Barrier(cxt context.Context, tableID model.TableID) error {
	// Barrier does nothing because FlushRowChangedEvents in mq sink has flushed
	// all buffered events by force.
	return nil
}

func (k *mqSink) run(ctx context.Context) error {
	defer k.resolvedReceiver.Stop()
	wg, ctx := errgroup.WithContext(ctx)
	for i := int32(0); i < k.partitionNum; i++ {
		partition := i
		wg.Go(func() error {
			return k.runWorker(ctx, partition)
		})
	}
	return wg.Wait()
}

const batchSizeLimit = 4 * 1024 * 1024 // 4MB

func (k *mqSink) runWorker(ctx context.Context, partition int32) error {
	input := k.partitionInput[partition]
	encoder, err := k.encoderBuilder.Build(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()

	flushToProducer := func(op codec.EncoderResult) error {
		return k.statistics.RecordBatchExecution(func() (int, error) {
			messages := encoder.Build()
			thisBatchSize := len(messages)
			if thisBatchSize == 0 {
				return 0, nil
			}

			for _, msg := range messages {
				err := k.writeToProducer(ctx, msg, codec.EncoderNeedAsyncWrite, partition)
				if err != nil {
					return 0, err
				}
			}

			if op == codec.EncoderNeedSyncWrite {
				err := k.mqProducer.Flush(ctx)
				if err != nil {
					return 0, err
				}
			}
			log.Debug("MQSink flushed", zap.Int("thisBatchSize", thisBatchSize))
			return thisBatchSize, nil
		})
	}
	for {
		var e mqEvent
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
			if err := flushToProducer(codec.EncoderNeedAsyncWrite); err != nil {
				return errors.Trace(err)
			}
			continue
		case e = <-input:
		}
		if e.row == nil {
			if e.resolvedTs != 0 {
				op, err := encoder.AppendResolvedEvent(e.resolvedTs)
				if err != nil {
					return errors.Trace(err)
				}

				if err := flushToProducer(op); err != nil {
					return errors.Trace(err)
				}

				atomic.StoreUint64(&k.partitionResolvedTs[partition], e.resolvedTs)
				k.resolvedNotifier.Notify()
			}
			continue
		}
		op, err := encoder.AppendRowChangedEvent(e.row)
		if err != nil {
			return errors.Trace(err)
		}

		if encoder.Size() >= batchSizeLimit {
			op = codec.EncoderNeedAsyncWrite
		}

		if encoder.Size() >= batchSizeLimit || op != codec.EncoderNoOperation {
			if err := flushToProducer(op); err != nil {
				return errors.Trace(err)
			}
		}
	}
}

func (k *mqSink) writeToProducer(ctx context.Context, message *codec.MQMessage, op codec.EncoderResult, partition int32) error {
	switch op {
	case codec.EncoderNeedAsyncWrite:
		if partition >= 0 {
			return k.mqProducer.AsyncSendMessage(ctx, message, partition)
		}
		return cerror.ErrAsyncBroadcastNotSupport.GenWithStackByArgs()
	case codec.EncoderNeedSyncWrite:
		if partition >= 0 {
			err := k.mqProducer.AsyncSendMessage(ctx, message, partition)
			if err != nil {
				return err
			}
			return k.mqProducer.Flush(ctx)
		}
		return k.mqProducer.SyncBroadcastMessage(ctx, message)
	}

	log.Warn("writeToProducer called with no-op",
		zap.ByteString("key", message.Key),
		zap.ByteString("value", message.Value),
		zap.Int32("partition", partition))
	return nil
}

func newKafkaSaramaSink(ctx context.Context, sinkURI *url.URL, filter *filter.Filter, replicaConfig *config.ReplicaConfig, opts map[string]string, errCh chan error) (*mqSink, error) {
	producerConfig := kafka.NewConfig()
	if err := producerConfig.CompleteByOpts(sinkURI, replicaConfig, opts); err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaInvalidConfig, err)
	}
	// NOTICE: Please check after the completion, as we may get the configuration from the sinkURI.
	err := replicaConfig.Validate()
	if err != nil {
		return nil, err
	}

	topic := strings.TrimFunc(sinkURI.Path, func(r rune) bool {
		return r == '/'
	})
	if topic == "" {
		return nil, cerror.ErrKafkaInvalidConfig.GenWithStack("no topic is specified in sink-uri")
	}

	sProducer, err := kafka.NewKafkaSaramaProducer(ctx, topic, producerConfig, errCh)
	if err != nil {
		return nil, errors.Trace(err)
	}
	sink, err := newMqSink(ctx, producerConfig.Credential, sProducer, filter, replicaConfig, opts, errCh)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return sink, nil
}

func newPulsarSink(ctx context.Context, sinkURI *url.URL, filter *filter.Filter, replicaConfig *config.ReplicaConfig, opts map[string]string, errCh chan error) (*mqSink, error) {
	producer, err := pulsar.NewProducer(sinkURI, errCh)
	if err != nil {
		return nil, errors.Trace(err)
	}
	s := sinkURI.Query().Get("protocol")
	if s != "" {
		replicaConfig.Sink.Protocol = s
	}
	// These two options are not used by Pulsar producer itself, but the encoders
	s = sinkURI.Query().Get("max-message-bytes")
	if s != "" {
		opts["max-message-bytes"] = s
	}

	s = sinkURI.Query().Get("max-batch-size")
	if s != "" {
		opts["max-batch-size"] = s
	}
	err = replicaConfig.Validate()
	if err != nil {
		return nil, err
	}
	// For now, it's a placeholder. Avro format have to make connection to Schema Registry,
	// and it may need credential.
	credential := &security.Credential{}
	sink, err := newMqSink(ctx, credential, producer, filter, replicaConfig, opts, errCh)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return sink, nil
}
