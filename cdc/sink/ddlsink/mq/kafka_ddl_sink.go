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
	"net/url"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/ddlsink/mq/ddlproducer"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink/mq/dispatcher"
	"github.com/pingcap/tiflow/cdc/sink/util"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec/builder"
	"github.com/pingcap/tiflow/pkg/sink/kafka"
	tiflowutil "github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

// NewKafkaDDLSink will verify the config and create a Kafka DDL Sink.
func NewKafkaDDLSink(
	ctx context.Context,
	changefeedID model.ChangeFeedID,
	sinkURI *url.URL,
	replicaConfig *config.ReplicaConfig,
	factoryCreator kafka.FactoryCreator,
	producerCreator ddlproducer.Factory,
) (_ *DDLSink, err error) {
	topic, err := util.GetTopic(sinkURI)
	if err != nil {
		return nil, errors.Trace(err)
	}

	options := kafka.NewOptions()
	if err := options.Apply(changefeedID, sinkURI, replicaConfig); err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaInvalidConfig, err)
	}

	factory, err := factoryCreator(options, changefeedID)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaNewProducer, err)
	}

	adminClient, err := factory.AdminClient(ctx)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaNewProducer, err)
	}
	// We must close adminClient when this func return cause by an error
	// otherwise the adminClient will never be closed and lead to a goroutine leak.
	defer func() {
		if err != nil && adminClient != nil {
			adminClient.Close()
		}
	}()

	if err := kafka.AdjustOptions(ctx, adminClient, options, topic); err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaNewProducer, err)
	}

	protocol, err := util.GetProtocol(tiflowutil.GetOrZero(replicaConfig.Sink.Protocol))
	if err != nil {
		return nil, errors.Trace(err)
	}

	topicManager, err := util.GetTopicManagerAndTryCreateTopic(
		ctx,
		changefeedID,
		topic,
		options.DeriveTopicConfig(),
		adminClient,
	)
	if err != nil {
		return nil, errors.Trace(err)
	}

	eventRouter, err := dispatcher.NewEventRouter(replicaConfig, protocol, topic, sinkURI.Scheme)
	if err != nil {
		return nil, errors.Trace(err)
	}

	encoderConfig, err := util.GetEncoderConfig(changefeedID, sinkURI, protocol, replicaConfig, options.MaxMessageBytes)
	if err != nil {
		return nil, errors.Trace(err)
	}

	encoderBuilder, err := builder.NewRowEventEncoderBuilder(ctx, encoderConfig)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaInvalidConfig, err)
	}

	start := time.Now()
	log.Info("Try to create a DDL sink producer",
		zap.String("changefeed", changefeedID.String()))
	syncProducer, err := factory.SyncProducer(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}

	ddlProducer := producerCreator(ctx, changefeedID, syncProducer)
	s := newDDLSink(changefeedID, ddlProducer, adminClient, topicManager, eventRouter, encoderBuilder, protocol)
	log.Info("DDL sink producer client created", zap.Duration("duration", time.Since(start)))
	return s, nil
}
