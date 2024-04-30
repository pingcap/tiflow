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

package mq

import (
	"context"
	"net/url"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink/mq/dispatcher"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink/mq/dmlproducer"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink/mq/manager"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink/mq/transformer/columnselector"
	"github.com/pingcap/tiflow/cdc/sink/util"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink"
	"github.com/pingcap/tiflow/pkg/sink/codec"
	"github.com/pingcap/tiflow/pkg/sink/codec/builder"
	pulsarConfig "github.com/pingcap/tiflow/pkg/sink/pulsar"
	tiflowutil "github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

// NewPulsarDMLSink will verify the config and create a PulsarSink.
func NewPulsarDMLSink(
	ctx context.Context,
	changefeedID model.ChangeFeedID,
	sinkURI *url.URL,
	replicaConfig *config.ReplicaConfig,
	errCh chan error,
	pulsarTopicManagerCreator manager.PulsarTopicManager,
	clientCreator pulsarConfig.FactoryCreator,
	producerCreator dmlproducer.PulsarFactory,
) (_ *dmlSink, err error) {
	log.Info("Starting pulsar DML producer ...",
		zap.String("namespace", changefeedID.Namespace),
		zap.String("changefeed", changefeedID.ID))

	defaultTopic, err := util.GetTopic(sinkURI)
	if err != nil {
		return nil, errors.Trace(err)
	}

	protocol, err := util.GetProtocol(tiflowutil.GetOrZero(replicaConfig.Sink.Protocol))
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !util.IsPulsarSupportedProtocols(protocol) {
		return nil, cerror.ErrSinkURIInvalid.
			GenWithStackByArgs("unsupported protocol, " +
				"pulsar sink currently only support these protocols: [canal-json, canal, maxwell]")
	}

	pConfig, err := pulsarConfig.NewPulsarConfig(sinkURI, replicaConfig.Sink.PulsarConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}

	client, err := clientCreator(pConfig, changefeedID, replicaConfig.Sink)
	if err != nil {
		log.Error("DML sink producer client create fail", zap.Error(err))
		return nil, cerror.WrapError(cerror.ErrPulsarNewClient, err)
	}

	failpointCh := make(chan error, 1)
	log.Info("Try to create a DML sink producer", zap.String("changefeed", changefeedID.String()))
	start := time.Now()
	p, err := producerCreator(ctx, changefeedID, client, replicaConfig.Sink, errCh, failpointCh)
	log.Info("DML sink producer created",
		zap.String("changefeed", changefeedID.String()),
		zap.Duration("duration", time.Since(start)))
	if err != nil {
		defer func() {
			if p != nil {
				p.Close()
			}
		}()
		return nil, cerror.WrapError(cerror.ErrPulsarNewProducer, err)
	}

	scheme := sink.GetScheme(sinkURI)
	// The topicManager is not actually used in pulsar , it is only used to create dmlSink.
	// TODO: Find a way to remove it in newDMLSink.
	topicManager, err := pulsarTopicManagerCreator(pConfig, client)
	if err != nil {
		return nil, errors.Trace(err)
	}
	eventRouter, err := dispatcher.NewEventRouter(replicaConfig, protocol, defaultTopic, scheme)
	if err != nil {
		return nil, errors.Trace(err)
	}

	trans, err := columnselector.New(replicaConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}

	encoderConfig, err := util.GetEncoderConfig(changefeedID, sinkURI, protocol, replicaConfig,
		config.DefaultMaxMessageBytes)
	if err != nil {
		return nil, errors.Trace(err)
	}

	encoderBuilder, err := builder.NewRowEventEncoderBuilder(ctx, encoderConfig)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrPulsarInvalidConfig, err)
	}

	encoderGroup := codec.NewEncoderGroup(replicaConfig.Sink, encoderBuilder, changefeedID)

	s := newDMLSink(ctx, changefeedID, p, nil, topicManager,
		eventRouter, trans, encoderGroup, protocol, scheme, errCh)

	return s, nil
}
