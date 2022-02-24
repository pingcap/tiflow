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
	"sync"

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
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type resolvedTsEvent struct {
	tableID    model.TableID
	resolvedTs model.Ts
}

const (
	// Depend on this size, every `partitionInputCh` will take
	// approximately 16.3 KiB memory.
	defaultPartitionInputChSize = 1024
	// Depend on this size, `resolvedBuffer` will take
	// approximately 2 KiB memory.
	defaultResolvedTsEventBufferSize = 128
	// -1 means broadcast to all partitions, it's the default for the default open protocol.
	defaultDDLDispatchPartition = -1
)

type mqSink struct {
	mqProducer     producer.Producer
	dispatcher     dispatcher.Dispatcher
	encoderBuilder codec.EncoderBuilder
	filter         *filter.Filter
	protocol       config.Protocol

	flushWorker          *flushWorker
	tableCheckpointTsMap sync.Map
	resolvedBuffer       chan resolvedTsEvent

	statistics *Statistics

	role util.Role
	id   model.ChangeFeedID
}

func newMqSink(
	ctx context.Context, credential *security.Credential, mqProducer producer.Producer,
	filter *filter.Filter, replicaConfig *config.ReplicaConfig, opts map[string]string,
	errCh chan error,
) (*mqSink, error) {
	var protocol config.Protocol
	err := protocol.FromString(replicaConfig.Sink.Protocol)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaInvalidConfig, err)
	}
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

	changefeedID := util.ChangefeedIDFromCtx(ctx)
	role := util.RoleFromCtx(ctx)

	encoder, err := encoderBuilder.Build(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	statistics := NewStatistics(ctx, "MQ")
	flushWorker := newFlushWorker(encoder, mqProducer, statistics)

	s := &mqSink{
		mqProducer:     mqProducer,
		dispatcher:     d,
		encoderBuilder: encoderBuilder,
		filter:         filter,
		protocol:       protocol,
		flushWorker:    flushWorker,
		resolvedBuffer: make(chan resolvedTsEvent, defaultResolvedTsEventBufferSize),
		statistics:     statistics,
		role:           role,
		id:             changefeedID,
	}

	go func() {
		if err := s.run(ctx); err != nil && errors.Cause(err) != context.Canceled {
			select {
			case <-ctx.Done():
				return
			case errCh <- err:
			default:
				log.Error("error channel is full", zap.Error(err),
					zap.String("changefeed", changefeedID), zap.Any("role", s.role))
			}
		}
	}()
	return s, nil
}

// TryEmitRowChangedEvents just calls EmitRowChangedEvents internally,
// it still blocking in current implementation.
// TODO(dongmen): We should make this method truly non-blocking after we remove buffer sink
func (k *mqSink) TryEmitRowChangedEvents(ctx context.Context, rows ...*model.RowChangedEvent) (bool, error) {
	err := k.EmitRowChangedEvents(ctx, rows...)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (k *mqSink) EmitRowChangedEvents(ctx context.Context, rows ...*model.RowChangedEvent) error {
	rowsCount := 0
	for _, row := range rows {
		if k.filter.ShouldIgnoreDMLEvent(row.StartTs, row.Table.Schema, row.Table.Table) {
			log.Info("Row changed event ignored",
				zap.Uint64("start-ts", row.StartTs),
				zap.String("changefeed", k.id),
				zap.Any("role", k.role))
			continue
		}
		partition := k.dispatcher.Dispatch(row)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case k.flushWorker.msgChan <- mqEvent{row: row, partition: partition}:
		}
		rowsCount++
	}
	k.statistics.AddRowsCount(rowsCount)
	return nil
}

// FlushRowChangedEvents is thread-safety
func (k *mqSink) FlushRowChangedEvents(ctx context.Context, tableID model.TableID, resolvedTs uint64) (uint64, error) {
	var checkpointTs uint64
	v, ok := k.tableCheckpointTsMap.Load(tableID)
	if ok {
		checkpointTs = v.(uint64)
	}
	if resolvedTs <= checkpointTs {
		return checkpointTs, nil
	}
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case k.resolvedBuffer <- resolvedTsEvent{
		tableID:    tableID,
		resolvedTs: resolvedTs,
	}:
	}
	k.statistics.PrintStatus(ctx)
	return checkpointTs, nil
}

// bgFlushTs flush resolvedTs to workers and flush the mqProducer
func (k *mqSink) bgFlushTs(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case msg := <-k.resolvedBuffer:
			resolvedTs := msg.resolvedTs
			err := k.flushTsToWorker(ctx, resolvedTs)
			if err != nil {
				return errors.Trace(err)
			}
			err = k.mqProducer.Flush(ctx)
			if err != nil {
				return errors.Trace(err)
			}
			// Since CDC does not guarantee exactly once semantic, it won't cause any problem
			// here even if the table was moved or removed.
			// ref: https://github.com/pingcap/tiflow/pull/4356#discussion_r787405134
			k.tableCheckpointTsMap.Store(msg.tableID, resolvedTs)
		}
	}
}

func (k *mqSink) flushTsToWorker(ctx context.Context, resolvedTs model.Ts) error {
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	case k.flushWorker.msgChan <- mqEvent{resolvedTs: resolvedTs}:
	}

	return nil
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
	err = k.writeToProducer(ctx, msg, -1)
	return errors.Trace(err)
}

func (k *mqSink) EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	if k.filter.ShouldIgnoreDDLEvent(ddl.StartTs, ddl.Type, ddl.TableInfo.Schema, ddl.TableInfo.Table) {
		log.Info(
			"DDL event ignored",
			zap.String("query", ddl.Query),
			zap.Uint64("startTs", ddl.StartTs),
			zap.Uint64("commitTs", ddl.CommitTs),
			zap.String("changefeed", k.id),
			zap.Any("role", k.role),
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
	log.Debug("emit ddl event", zap.String("query", ddl.Query),
		zap.Uint64("commitTs", ddl.CommitTs), zap.Int32("partition", partition),
		zap.String("changefeed", k.id), zap.Any("role", k.role))
	err = k.writeToProducer(ctx, msg, partition)
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
	wg, ctx := errgroup.WithContext(ctx)
	wg.Go(func() error {
		return k.bgFlushTs(ctx)
	})
	wg.Go(func() error {
		return k.flushWorker.run(ctx)
	})
	return wg.Wait()
}

func (k *mqSink) writeToProducer(ctx context.Context, message *codec.MQMessage, partition int32) error {
	if partition >= 0 {
		err := k.mqProducer.AsyncSendMessage(ctx, message, partition)
		if err != nil {
			return err
		}
		return k.mqProducer.Flush(ctx)
	}

	return k.mqProducer.SyncBroadcastMessage(ctx, message)
}

func newKafkaSaramaSink(ctx context.Context, sinkURI *url.URL,
	filter *filter.Filter, replicaConfig *config.ReplicaConfig,
	opts map[string]string, errCh chan error) (*mqSink, error) {
	producerConfig := kafka.NewConfig()
	if err := kafka.CompleteConfigsAndOpts(sinkURI, producerConfig, replicaConfig, opts); err != nil {
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

	sProducer, err := kafka.NewKafkaSaramaProducer(ctx, topic, producerConfig, opts, errCh)
	if err != nil {
		return nil, errors.Trace(err)
	}
	sink, err := newMqSink(ctx, producerConfig.Credential, sProducer, filter, replicaConfig, opts, errCh)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return sink, nil
}

func newPulsarSink(ctx context.Context, sinkURI *url.URL, filter *filter.Filter,
	replicaConfig *config.ReplicaConfig, opts map[string]string, errCh chan error) (*mqSink, error) {
	producer, err := pulsar.NewProducer(sinkURI, errCh)
	if err != nil {
		return nil, errors.Trace(err)
	}
	s := sinkURI.Query().Get(config.ProtocolKey)
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
