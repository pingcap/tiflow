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

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink/mq/dispatcher"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink/mq/dmlproducer"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink/mq/transformer/columnselector"
	"github.com/pingcap/tiflow/cdc/sink/util"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink"
	"github.com/pingcap/tiflow/pkg/sink/codec"
	"github.com/pingcap/tiflow/pkg/sink/codec/builder"
	"github.com/pingcap/tiflow/pkg/sink/kafka"
	tiflowutil "github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

// NewKafkaDMLSink will verify the config and create a KafkaSink.
func NewKafkaDMLSink(
	ctx context.Context,
	changefeedID model.ChangeFeedID,
	sinkURI *url.URL,
	replicaConfig *config.ReplicaConfig,
	errCh chan error,
	factoryCreator kafka.FactoryCreator,
	producerCreator dmlproducer.Factory,
) (_ *dmlSink, err error) {
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

	// adjust the option configuration before creating the kafka client
	if err = kafka.AdjustOptions(ctx, adminClient, options, topic); err != nil {
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

	scheme := sink.GetScheme(sinkURI)
	eventRouter, err := dispatcher.NewEventRouter(replicaConfig, protocol, topic, scheme)
	if err != nil {
		return nil, errors.Trace(err)
	}

	trans, err := columnselector.New(replicaConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}

	encoderConfig, err := util.GetEncoderConfig(changefeedID, sinkURI, protocol, replicaConfig, options.MaxMessageBytes)
	if err != nil {
		return nil, errors.Trace(err)
	}

	encoderBuilder, err := builder.NewRowEventEncoderBuilder(ctx, encoderConfig)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaNewProducer, err)
	}

	failpointCh := make(chan error, 1)
	asyncProducer, err := factory.AsyncProducer(ctx, failpointCh)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaNewProducer, err)
	}

	metricsCollector := factory.MetricsCollector(tiflowutil.RoleProcessor, adminClient)
	dmlProducer := producerCreator(ctx, changefeedID, asyncProducer, metricsCollector, errCh, failpointCh)
	encoderGroup := codec.NewEncoderGroup(replicaConfig.Sink, encoderBuilder, changefeedID)
	s := newDMLSink(ctx, changefeedID, dmlProducer, adminClient, topicManager,
		eventRouter, trans, encoderGroup, protocol, scheme, errCh)
	log.Info("DML sink producer created",
		zap.String("namespace", changefeedID.Namespace),
		zap.String("changefeedID", changefeedID.ID))

	return s, nil
}
