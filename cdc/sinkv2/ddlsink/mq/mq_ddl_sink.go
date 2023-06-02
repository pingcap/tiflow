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

package mq

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/codec"
	"github.com/pingcap/tiflow/cdc/sink/codec/builder"
	"github.com/pingcap/tiflow/cdc/sink/codec/common"
	"github.com/pingcap/tiflow/cdc/sink/mq/dispatcher"
	"github.com/pingcap/tiflow/cdc/sink/mq/manager"
	"github.com/pingcap/tiflow/cdc/sinkv2/ddlsink"
	"github.com/pingcap/tiflow/cdc/sinkv2/ddlsink/mq/ddlproducer"
	"github.com/pingcap/tiflow/cdc/sinkv2/metrics"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink"
	"go.uber.org/zap"
)

// Assert DDLEventSink implementation
var _ ddlsink.DDLEventSink = (*ddlSink)(nil)

type ddlSink struct {
	// id indicates which processor (changefeed) this sink belongs to.
	id model.ChangeFeedID
	// protocol indicates the protocol used by this sink.
	protocol config.Protocol
	// eventRouter used to route events to the right topic and partition.
	eventRouter *dispatcher.EventRouter
	// topicManager used to manage topics.
	// It is also responsible for creating topics.
	topicManager manager.TopicManager
	// encoderBuilder builds encoder for the sink.
	encoderBuilder codec.EncoderBuilder
	// producer used to send events to the MQ system.
	// Usually it is a sync producer.
	producer ddlproducer.DDLProducer
	// statistics is used to record DDL metrics.
	statistics *metrics.Statistics
}

func newDDLSink(ctx context.Context,
	producer ddlproducer.DDLProducer,
	topicManager manager.TopicManager,
	eventRouter *dispatcher.EventRouter,
	encoderConfig *common.Config,
) (*ddlSink, error) {
	changefeedID := contextutil.ChangefeedIDFromCtx(ctx)

	encoderBuilder, err := builder.NewEventBatchEncoderBuilder(ctx, encoderConfig)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaInvalidConfig, err)
	}

	s := &ddlSink{
		id:             changefeedID,
		protocol:       encoderConfig.Protocol,
		eventRouter:    eventRouter,
		topicManager:   topicManager,
		encoderBuilder: encoderBuilder,
		producer:       producer,
		statistics:     metrics.NewStatistics(ctx, sink.RowSink),
	}

	return s, nil
}

func (k *ddlSink) WriteDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	encoder := k.encoderBuilder.Build()
	msg, err := encoder.EncodeDDLEvent(ddl)
	if err != nil {
		return errors.Trace(err)
	}
	if msg == nil {
		log.Info("Skip ddl event", zap.Uint64("commitTs", ddl.CommitTs),
			zap.String("query", ddl.Query),
			zap.String("protocol", k.protocol.String()),
			zap.String("namespace", k.id.Namespace),
			zap.String("changefeed", k.id.ID))
		return nil
	}

	topic := k.eventRouter.GetTopicForDDL(ddl)
	partitionRule := k.eventRouter.GetDLLDispatchRuleByProtocol(k.protocol)
	log.Debug("Emit ddl event",
		zap.Uint64("commitTs", ddl.CommitTs),
		zap.String("query", ddl.Query),
		zap.String("namespace", k.id.Namespace),
		zap.String("changefeed", k.id.ID))
	if partitionRule == dispatcher.PartitionAll {
		partitionNum, err := k.topicManager.GetPartitionNum(topic)
		if err != nil {
			return errors.Trace(err)
		}
		err = k.statistics.RecordDDLExecution(func() error {
			return k.producer.SyncBroadcastMessage(ctx, topic, partitionNum, msg)
		})
		return errors.Trace(err)
	}
	// Notice: We must call GetPartitionNum here,
	// which will be responsible for automatically creating topics when they don't exist.
	// If it is not called here and kafka has `auto.create.topics.enable` turned on,
	// then the auto-created topic will not be created as configured by ticdc.
	_, err = k.topicManager.GetPartitionNum(topic)
	if err != nil {
		return errors.Trace(err)
	}
	err = k.statistics.RecordDDLExecution(func() error {
		return k.producer.SyncSendMessage(ctx, topic, dispatcher.PartitionZero, msg)
	})
	return errors.Trace(err)
}

func (k *ddlSink) WriteCheckpointTs(ctx context.Context,
	ts uint64, tables []*model.TableInfo,
) error {
	encoder := k.encoderBuilder.Build()
	msg, err := encoder.EncodeCheckpointEvent(ts)
	if err != nil {
		return errors.Trace(err)
	}
	if msg == nil {
		return nil
	}
	// NOTICE: When there are no tables to replicate,
	// we need to send checkpoint ts to the default topic.
	// This will be compatible with the old behavior.
	if len(tables) == 0 {
		topic := k.eventRouter.GetDefaultTopic()
		partitionNum, err := k.topicManager.GetPartitionNum(topic)
		if err != nil {
			return errors.Trace(err)
		}
		log.Debug("Emit checkpointTs to default topic",
			zap.String("topic", topic), zap.Uint64("checkpointTs", ts))
		err = k.producer.SyncBroadcastMessage(ctx, topic, partitionNum, msg)
		return errors.Trace(err)
	}
	var tableNames []model.TableName
	for _, table := range tables {
		tableNames = append(tableNames, table.TableName)
	}
	topics := k.eventRouter.GetActiveTopics(tableNames)
	for _, topic := range topics {
		partitionNum, err := k.topicManager.GetPartitionNum(topic)
		if err != nil {
			return errors.Trace(err)
		}
		err = k.producer.SyncBroadcastMessage(ctx, topic, partitionNum, msg)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (k *ddlSink) Close() error {
	k.topicManager.Close()
	k.producer.Close()
	return nil
}
