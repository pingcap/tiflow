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
	"github.com/pingcap/tiflow/cdc/sink/ddlsink/mq/ddlproducer"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink/mq/dispatcher"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink/mq/manager"
	// pulsarMetric "github.com/pingcap/tiflow/cdc/sink/metrics/mq/pulsar"
	"github.com/pingcap/tiflow/cdc/sink/util"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec/builder"
	pulsarConfig "github.com/pingcap/tiflow/pkg/sink/pulsar"
	tiflowutil "github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

// NewPulsarDDLSink will verify the config and create a Pulsar DDL Sink.
func NewPulsarDDLSink(
	ctx context.Context,
	changefeedID model.ChangeFeedID,
	sinkURI *url.URL,
	replicaConfig *config.ReplicaConfig,
	pulsarTopicManagerCreator manager.PulsarTopicManager,
	clientCreator pulsarConfig.FactoryCreator,
	producerCreator ddlproducer.PulsarFactory,
) (_ *DDLSink, err error) {
	// changefeedID := contextutil.ChangefeedIDFromCtx(ctx)
	log.Info("Starting pulsar DDL producer ...",
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

	pConfig, err := pulsarConfig.NewPulsarConfig(sinkURI)
	if err != nil {
		return nil, errors.Trace(err)
	}

	log.Info("Try to create a DDL sink producer", zap.Any("pulsarConfig", pConfig))

	start := time.Now()
	client, err := clientCreator(pConfig, changefeedID)
	if err != nil {
		log.Error("DDL sink producer client create fail", zap.Error(err))
		return nil, cerror.WrapError(cerror.ErrPulsarNewClient, err)
	}

	p, err := producerCreator(ctx, changefeedID, pConfig, client)
	log.Info("DDL sink producer client created", zap.Duration("duration", time.Since(start)))
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrPulsarNewProducer, err)
	}

	// NewEventRouter
	eventRouter, err := dispatcher.NewEventRouter(replicaConfig, defaultTopic)
	if err != nil {
		return nil, errors.Trace(err)
	}

	encoderConfig, err := util.GetEncoderConfig(sinkURI, protocol, replicaConfig, pConfig.MaxMessageBytes)
	if err != nil {
		return nil, errors.Trace(err)
	}

	encoderBuilder, err := builder.NewRowEventEncoderBuilder(ctx, changefeedID, encoderConfig)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaInvalidConfig, err)
	}

	topicManager, err := pulsarTopicManagerCreator(pConfig, client)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// s, err := newDDLSink(ctx, changefeedID, p, topicManager, eventRouter, encoderConfig)
	s := newDDLSink(ctx, changefeedID, p, nil, topicManager, eventRouter, encoderBuilder, protocol)

	return s, nil
}

// str2Pointer returns the pointer of the string.
func str2Pointer(str string) *string {
	return &str
}
