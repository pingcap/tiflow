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
	"fmt"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/codec"
	"github.com/pingcap/tiflow/cdc/sink/dispatcher"
	"github.com/pingcap/tiflow/cdc/sink/producer"
	"github.com/pingcap/tiflow/cdc/sink/producer/kafka"
	"github.com/pingcap/tiflow/cdc/sink/producer/pulsar"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/filter"
	"github.com/pingcap/tiflow/pkg/notify"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type mqSink struct {
	mqProducer producer.Producer
	dispatcher dispatcher.Dispatcher
	newEncoder func() codec.EventBatchEncoder
	filter     *filter.Filter
	protocol   codec.Protocol

	partitionNum   int32
	partitionInput []chan struct {
		row        *model.RowChangedEvent
		resolvedTs uint64
	}
	partitionResolvedTs []uint64
	tableCheckpointTs   map[model.TableID]uint64
	resolvedNotifier    *notify.Notifier
	resolvedReceiver    *notify.Receiver

	statistics *Statistics
}

func newMqSink(
	ctx context.Context, credential *security.Credential, mqProducer producer.Producer,
	filter *filter.Filter, config *config.ReplicaConfig, opts map[string]string, errCh chan error,
) (*mqSink, error) {
	partitionNum := mqProducer.GetPartitionNum()
	partitionInput := make([]chan struct {
		row        *model.RowChangedEvent
		resolvedTs uint64
	}, partitionNum)
	for i := 0; i < int(partitionNum); i++ {
		partitionInput[i] = make(chan struct {
			row        *model.RowChangedEvent
			resolvedTs uint64
		}, 12800)
	}
	d, err := dispatcher.NewDispatcher(config, mqProducer.GetPartitionNum())
	if err != nil {
		return nil, errors.Trace(err)
	}
	notifier := new(notify.Notifier)
	var protocol codec.Protocol
	protocol.FromString(config.Sink.Protocol)

	newEncoder := codec.NewEventBatchEncoder(protocol)
	if protocol == codec.ProtocolAvro {
		registryURI, ok := opts["registry"]
		if !ok {
			return nil, cerror.ErrPrepareAvroFailed.GenWithStack(`Avro protocol requires parameter "registry"`)
		}
		keySchemaManager, err := codec.NewAvroSchemaManager(ctx, credential, registryURI, "-key")
		if err != nil {
			return nil, errors.Annotate(
				cerror.WrapError(cerror.ErrPrepareAvroFailed, err),
				"Could not create Avro schema manager for message keys")
		}
		valueSchemaManager, err := codec.NewAvroSchemaManager(ctx, credential, registryURI, "-value")
		if err != nil {
			return nil, errors.Annotate(
				cerror.WrapError(cerror.ErrPrepareAvroFailed, err),
				"Could not create Avro schema manager for message values")
		}
		newEncoder1 := newEncoder
		newEncoder = func() codec.EventBatchEncoder {
			avroEncoder := newEncoder1().(*codec.AvroEventBatchEncoder)
			avroEncoder.SetKeySchemaManager(keySchemaManager)
			avroEncoder.SetValueSchemaManager(valueSchemaManager)
			avroEncoder.SetTimeZone(util.TimezoneFromCtx(ctx))
			return avroEncoder
		}
	} else if (protocol == codec.ProtocolCanal || protocol == codec.ProtocolCanalJSON || protocol == codec.ProtocolMaxwell) && !config.EnableOldValue {
		log.Error(fmt.Sprintf("Old value is not enabled when using `%s` protocol. "+
			"Please update changefeed config", protocol.String()))
		return nil, cerror.WrapError(cerror.ErrKafkaInvalidConfig, errors.New(fmt.Sprintf("%s protocol requires old value to be enabled", protocol.String())))
	}

	// pre-flight verification of encoder parameters
	if err := newEncoder().SetParams(opts); err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaInvalidConfig, err)
	}

	newEncoder1 := newEncoder
	newEncoder = func() codec.EventBatchEncoder {
		ret := newEncoder1()
		err := ret.SetParams(opts)
		if err != nil {
			log.Panic("MQ Encoder could not parse parameters", zap.Error(err))
		}
		return ret
	}

	resolvedReceiver, err := notifier.NewReceiver(50 * time.Millisecond)
	if err != nil {
		return nil, err
	}
	k := &mqSink{
		mqProducer: mqProducer,
		dispatcher: d,
		newEncoder: newEncoder,
		filter:     filter,
		protocol:   protocol,

		partitionNum:        partitionNum,
		partitionInput:      partitionInput,
		partitionResolvedTs: make([]uint64, partitionNum),
		tableCheckpointTs:   make(map[model.TableID]uint64),
		resolvedNotifier:    notifier,
		resolvedReceiver:    resolvedReceiver,

		statistics: NewStatistics(ctx, "MQ", opts),
	}

	go func() {
		if err := k.run(ctx); err != nil && errors.Cause(err) != context.Canceled {
			select {
			case <-ctx.Done():
				return
			case errCh <- err:
			default:
				log.Error("error channel is full", zap.Error(err))
			}
		}
	}()
	return k, nil
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
	encoder := k.newEncoder()
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
	encoder := k.newEncoder()
	msg, err := encoder.EncodeDDLEvent(ddl)
	if err != nil {
		return errors.Trace(err)
	}

	if msg == nil {
		return nil
	}

	k.statistics.AddDDLCount()
	log.Debug("emit ddl event", zap.String("query", ddl.Query), zap.Uint64("commit-ts", ddl.CommitTs))
	err = k.writeToProducer(ctx, msg, codec.EncoderNeedSyncWrite, -1)
	return errors.Trace(err)
}

func (k *mqSink) Close(ctx context.Context) error {
	err := k.mqProducer.Close()
	return errors.Trace(err)
}

func (k *mqSink) Barrier(cxt context.Context, tableID model.TableID) error {
	// Barrier does nothing because FlushRowChangedEvents in mq sink has flushed
	// all buffered events forcedlly.
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
	encoder := k.newEncoder()
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()

	flushToProducer := func(op codec.EncoderResult) error {
		return k.statistics.RecordBatchExecution(func() (int, error) {
			messages := encoder.Build()
			thisBatchSize := 0
			if len(messages) == 0 {
				return 0, nil
			}

			for _, msg := range messages {
				err := k.writeToProducer(ctx, msg, codec.EncoderNeedAsyncWrite, partition)
				if err != nil {
					return 0, err
				}
				thisBatchSize += msg.GetRowsCount()
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
		var e struct {
			row        *model.RowChangedEvent
			resolvedTs uint64
		}
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
			return k.mqProducer.SendMessage(ctx, message, partition)
		}
		return cerror.ErrAsyncBroadcastNotSupport.GenWithStackByArgs()
	case codec.EncoderNeedSyncWrite:
		if partition >= 0 {
			err := k.mqProducer.SendMessage(ctx, message, partition)
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
	scheme := strings.ToLower(sinkURI.Scheme)
	if scheme != "kafka" && scheme != "kafka+ssl" {
		return nil, cerror.ErrKafkaInvalidConfig.GenWithStack("can't create MQ sink with unsupported scheme: %s", scheme)
	}

	config := kafka.NewConfig()
	if err := config.Initialize(sinkURI, replicaConfig, opts); err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaInvalidConfig, err)
	}

	topic := strings.TrimFunc(sinkURI.Path, func(r rune) bool {
		return r == '/'
	})
	if topic == "" {
		return nil, cerror.ErrKafkaInvalidConfig.GenWithStack("no topic is specified in sink-uri")
	}

	producer, err := kafka.NewKafkaSaramaProducer(ctx, topic, config, opts, errCh)
	if err != nil {
		return nil, errors.Trace(err)
	}
	sink, err := newMqSink(ctx, config.Credential, producer, filter, replicaConfig, opts, errCh)
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
	// For now, it's a placeholder. Avro format have to make connection to Schema Registry,
	// and it may need credential.
	credential := &security.Credential{}
	sink, err := newMqSink(ctx, credential, producer, filter, replicaConfig, opts, errCh)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return sink, nil
}
