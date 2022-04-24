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

package mq

import (
	"context"
	"net/url"
	"strings"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/codec"
	"github.com/pingcap/tiflow/cdc/sink/metrics"
	"github.com/pingcap/tiflow/cdc/sink/mq/dispatcher"
	"github.com/pingcap/tiflow/cdc/sink/mq/manager"
	kafkamanager "github.com/pingcap/tiflow/cdc/sink/mq/manager/kafka"
	pulsarmanager "github.com/pingcap/tiflow/cdc/sink/mq/manager/pulsar"
	"github.com/pingcap/tiflow/cdc/sink/mq/producer"
	"github.com/pingcap/tiflow/cdc/sink/mq/producer/kafka"
	"github.com/pingcap/tiflow/cdc/sink/mq/producer/pulsar"
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
	// Depend on this size, `resolvedBuffer` will take
	// approximately 2 KiB memory.
	defaultResolvedTsEventBufferSize = 128
)

type mqSink struct {
	mqProducer     producer.Producer
	eventRouter    *dispatcher.EventRouter
	encoderBuilder codec.EncoderBuilder
	filter         *filter.Filter
	protocol       config.Protocol

	topicManager         manager.TopicManager
	flushWorker          *flushWorker
	tableCheckpointTsMap sync.Map
	resolvedBuffer       chan resolvedTsEvent

	statistics *metrics.Statistics

	role util.Role
	id   model.ChangeFeedID
}

func newMqSink(
	ctx context.Context,
	credential *security.Credential,
	topicManager manager.TopicManager,
	mqProducer producer.Producer,
	filter *filter.Filter,
	defaultTopic string,
	replicaConfig *config.ReplicaConfig, encoderConfig *codec.Config,
	errCh chan error,
) (*mqSink, error) {
	encoderBuilder, err := codec.NewEventBatchEncoderBuilder(encoderConfig, credential)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaInvalidConfig, err)
	}

	eventRouter, err := dispatcher.NewEventRouter(replicaConfig, defaultTopic)
	if err != nil {
		return nil, errors.Trace(err)
	}

	changefeedID := util.ChangefeedIDFromCtx(ctx)
	role := util.RoleFromCtx(ctx)

	encoder := encoderBuilder.Build()
	statistics := metrics.NewStatistics(ctx, metrics.SinkTypeMQ)
	flushWorker := newFlushWorker(encoder, mqProducer, statistics)

	s := &mqSink{
		mqProducer:     mqProducer,
		eventRouter:    eventRouter,
		encoderBuilder: encoderBuilder,
		filter:         filter,
		protocol:       encoderConfig.Protocol(),
		topicManager:   topicManager,
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

// Init table sink resources
func (k *mqSink) Init(tableID model.TableID) error {
	return nil
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
		topic := k.eventRouter.GetTopicForRowChange(row)
		partitionNum, err := k.topicManager.Partitions(topic)
		if err != nil {
			return errors.Trace(err)
		}
		partition := k.eventRouter.GetPartitionForRowChange(row, partitionNum)
		err = k.flushWorker.addEvent(ctx, mqEvent{
			row: row,
			key: topicPartitionKey{
				topic: topic, partition: partition,
			},
		})
		if err != nil {
			return err
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
			// Since CDC does not guarantee exactly once semantic, it won't cause any problem
			// here even if the table was moved or removed.
			// ref: https://github.com/pingcap/tiflow/pull/4356#discussion_r787405134
			k.tableCheckpointTsMap.Store(msg.tableID, resolvedTs)
		}
	}
}

func (k *mqSink) flushTsToWorker(ctx context.Context, resolvedTs model.Ts) error {
	if err := k.flushWorker.addEvent(ctx, mqEvent{resolvedTs: resolvedTs}); err != nil {
		if errors.Cause(err) != context.Canceled {
			log.Warn("failed to flush TS to worker", zap.Error(err))
		} else {
			log.Debug("flushing TS to worker has been canceled", zap.Error(err))
		}
		return err
	}
	return nil
}

func (k *mqSink) EmitCheckpointTs(ctx context.Context, ts uint64, tables []model.TableName) error {
	encoder := k.encoderBuilder.Build()
	msg, err := encoder.EncodeCheckpointEvent(ts)
	if err != nil {
		return errors.Trace(err)
	}
	if msg == nil {
		return nil
	}
	// NOTICE: When there is no table sync,
	// we need to send checkpoint ts to the default topic. T
	// This will be compatible with the old behavior.
	if len(tables) == 0 {
		topic := k.eventRouter.GetDefaultTopic()
		partitionNum, err := k.topicManager.Partitions(topic)
		if err != nil {
			return errors.Trace(err)
		}
		log.Debug("emit checkpointTs to default topic",
			zap.String("topic", topic), zap.Uint64("checkpointTs", ts))
		err = k.mqProducer.SyncBroadcastMessage(ctx, topic, partitionNum, msg)
		return errors.Trace(err)
	}
	topics := k.eventRouter.GetActiveTopics(tables)
	log.Debug("MQ sink current active topics", zap.Any("topics", topics))
	for _, topic := range topics {
		partitionNum, err := k.topicManager.Partitions(topic)
		if err != nil {
			return errors.Trace(err)
		}
		log.Debug("emit checkpointTs to active topic",
			zap.String("topic", topic), zap.Uint64("checkpointTs", ts))
		err = k.mqProducer.SyncBroadcastMessage(ctx, topic, partitionNum, msg)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
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

	encoder := k.encoderBuilder.Build()
	msg, err := encoder.EncodeDDLEvent(ddl)
	if err != nil {
		return errors.Trace(err)
	}
	if msg == nil {
		return nil
	}

	topic := k.eventRouter.GetTopicForDDL(ddl)
	partitionRule := k.eventRouter.GetDLLDispatchRuleByProtocol(k.protocol)
	k.statistics.AddDDLCount()
	log.Debug("emit ddl event",
		zap.Uint64("commitTs", ddl.CommitTs), zap.String("query", ddl.Query),
		zap.String("changefeed", k.id), zap.Any("role", k.role))
	if partitionRule == dispatcher.PartitionAll {
		partitionNum, err := k.topicManager.Partitions(topic)
		if err != nil {
			return errors.Trace(err)
		}
		err = k.mqProducer.SyncBroadcastMessage(ctx, topic, partitionNum, msg)
		return errors.Trace(err)
	}
	// Notice: We must call Partitions here,
	// which will be responsible for automatically creating topics when they don't exist.
	// If it is not called here and kafka has `auto.create.topics.enable` turned on,
	// then the auto-created topic will not be created as configured by ticdc.
	_, err = k.topicManager.Partitions(topic)
	if err != nil {
		return errors.Trace(err)
	}
	err = k.asyncFlushToPartitionZero(ctx, topic, msg)
	return errors.Trace(err)
}

// Close the producer asynchronously, does not care closed successfully or not.
func (k *mqSink) Close(ctx context.Context) error {
	go k.mqProducer.Close()
	return nil
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

// asyncFlushToPartitionZero writes message to
// partition zero asynchronously and flush it immediately.
func (k *mqSink) asyncFlushToPartitionZero(
	ctx context.Context, topic string, message *codec.MQMessage,
) error {
	err := k.mqProducer.AsyncSendMessage(ctx, topic, dispatcher.PartitionZero, message)
	if err != nil {
		return err
	}
	return k.mqProducer.Flush(ctx)
}

// NewKafkaSaramaSink creates a new Kafka mqSink.
func NewKafkaSaramaSink(ctx context.Context, sinkURI *url.URL,
	filter *filter.Filter, replicaConfig *config.ReplicaConfig,
	opts map[string]string, errCh chan error,
) (*mqSink, error) {
	topic := strings.TrimFunc(sinkURI.Path, func(r rune) bool {
		return r == '/'
	})
	if topic == "" {
		return nil, cerror.ErrKafkaInvalidConfig.GenWithStack("no topic is specified in sink-uri")
	}

	baseConfig := kafka.NewConfig()
	if err := baseConfig.Apply(sinkURI); err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaInvalidConfig, err)
	}

	if err := replicaConfig.ApplyProtocol(sinkURI).Validate(); err != nil {
		return nil, errors.Trace(err)
	}

	saramaConfig, err := kafka.NewSaramaConfig(ctx, baseConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}

	adminClient, err := kafka.NewAdminClientImpl(baseConfig.BrokerEndpoints, saramaConfig)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaNewSaramaProducer, err)
	}

	if err := kafka.AdjustConfig(adminClient, baseConfig, saramaConfig, topic); err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaInvalidConfig, err)
	}

	var protocol config.Protocol
	if err := protocol.FromString(replicaConfig.Sink.Protocol); err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaInvalidConfig, err)
	}

	encoderConfig := codec.NewConfig(protocol, util.TimezoneFromCtx(ctx))
	if err := encoderConfig.Apply(sinkURI, opts); err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaInvalidConfig, err)
	}
	// always set encoder's `MaxMessageBytes` equal to producer's `MaxMessageBytes`
	// to prevent that the encoder generate batched message too large then cause producer meet `message too large`
	encoderConfig = encoderConfig.WithMaxMessageBytes(saramaConfig.Producer.MaxMessageBytes)

	if err := encoderConfig.Validate(); err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaInvalidConfig, err)
	}

	client, err := sarama.NewClient(baseConfig.BrokerEndpoints, saramaConfig)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaNewSaramaProducer, err)
	}

	topicManager := kafkamanager.NewTopicManager(
		client,
		adminClient,
		baseConfig.DeriveTopicConfig(),
	)
	if _, err := topicManager.CreateTopic(topic); err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaCreateTopic, err)
	}

	sProducer, err := kafka.NewKafkaSaramaProducer(
		ctx,
		client,
		adminClient,
		baseConfig,
		saramaConfig,
		errCh,
	)
	if err != nil {
		return nil, errors.Trace(err)
	}

	sink, err := newMqSink(
		ctx,
		baseConfig.Credential,
		topicManager,
		sProducer,
		filter,
		topic,
		replicaConfig,
		encoderConfig,
		errCh,
	)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return sink, nil
}

// NewPulsarSink creates a new Pulsar mqSink.
func NewPulsarSink(ctx context.Context, sinkURI *url.URL, filter *filter.Filter,
	replicaConfig *config.ReplicaConfig, opts map[string]string, errCh chan error,
) (*mqSink, error) {
	s := sinkURI.Query().Get(config.ProtocolKey)
	if s != "" {
		replicaConfig.Sink.Protocol = s
	}
	err := replicaConfig.Validate()
	if err != nil {
		return nil, err
	}

	var protocol config.Protocol
	if err := protocol.FromString(replicaConfig.Sink.Protocol); err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaInvalidConfig, err)
	}

	encoderConfig := codec.NewConfig(protocol, util.TimezoneFromCtx(ctx))
	if err := encoderConfig.Apply(sinkURI, opts); err != nil {
		return nil, errors.Trace(err)
	}
	// todo: set by pulsar producer's `max.message.bytes`
	// encoderConfig = encoderConfig.WithMaxMessageBytes()
	if err := encoderConfig.Validate(); err != nil {
		return nil, errors.Trace(err)
	}

	producer, err := pulsar.NewProducer(sinkURI, errCh)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// For now, it's a placeholder. Avro format have to make connection to Schema Registry,
	// and it may need credential.
	credential := &security.Credential{}
	fakeTopicManager := pulsarmanager.NewTopicManager(
		producer.GetPartitionNum(),
	)
	sink, err := newMqSink(
		ctx,
		credential,
		fakeTopicManager,
		producer,
		filter,
		"",
		replicaConfig,
		encoderConfig,
		errCh,
	)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return sink, nil
}
