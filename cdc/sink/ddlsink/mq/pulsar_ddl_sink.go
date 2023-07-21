package mq

import (
	"context"
	"net/url"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/ddlsink/mq/ddlproducer"
	"github.com/pingcap/tiflow/cdc/sink/mq/dispatcher"
	"github.com/pingcap/tiflow/cdc/sink/mq/manager"
	"github.com/pingcap/tiflow/cdc/sink/util"
	pulsarMetric "github.com/pingcap/tiflow/cdc/sinkv/metrics/mq/pulsar"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	pulsarConfig "github.com/pingcap/tiflow/pkg/sink/pulsar"
	"go.uber.org/zap"
)

// NewPulsarDDLSink will verify the config and create a Pulsar DDL Sink.
func NewPulsarDDLSink(
	ctx context.Context,
	sinkURI *url.URL,
	replicaConfig *config.ReplicaConfig,
	producerCreator ddlproducer.PulsarFactory,
) (_ *ddlSink, err error) {

	changefeedID := contextutil.ChangefeedIDFromCtx(ctx)
	log.Info("Starting pulsar DDL producer ...",
		zap.String("namespace", changefeedID.Namespace),
		zap.String("changefeed", changefeedID.ID))

	defaultTopic, err := util.GetTopic(sinkURI)
	if err != nil {
		return nil, errors.Trace(err)
	}

	protocol, err := util.GetProtocol(replicaConfig.Sink.Protocol)
	if err != nil {
		return nil, errors.Trace(err)
	}

	pConfig, err := pulsarConfig.NewPulsarConfig(sinkURI)
	if err != nil {
		return nil, errors.Trace(err)
	}

	log.Info("Try to create a DDL sink producer", zap.Any("pulsarConfig", pConfig))

	client, err := createPulsarClient(pConfig, changefeedID)
	if err != nil {
		log.Error("DDL sink producer client create fail", zap.Error(err))
		return nil, cerror.WrapError(cerror.ErrPulsarNewClient, err)
	}

	start := time.Now()
	p, err := producerCreator(ctx, pConfig, client)
	log.Info("DDL sink producer client created", zap.Duration("duration", time.Since(start)))
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrPulsarNewProducer, err)
	}

	// NewEventRouter
	eventRouter, err := dispatcher.NewEventRouter(replicaConfig, defaultTopic)
	if err != nil {
		return nil, errors.Trace(err)
	}

	encoderConfig, err := util.GetEncoderConfig(sinkURI, protocol, replicaConfig,
		pConfig.MaxMessageBytes)
	if err != nil {
		return nil, errors.Trace(err)
	}

	topicManager, err := manager.NewPulsarTopicManager(pConfig, client)
	if err != nil {
		return nil, errors.Trace(err)
	}

	s, err := newDDLSink(ctx, p, topicManager, eventRouter, encoderConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return s, nil
}

func createPulsarClient(config *pulsarConfig.PulsarConfig, changefeedID model.ChangeFeedID) (pulsar.Client, error) {

	op := pulsar.ClientOptions{
		URL:               config.URL,
		MetricsRegisterer: pulsarMetric.GetMetricRegistry(),
		CustomMetricsLabels: map[string]string{
			"changefeed": changefeedID.ID,
			"namespace":  changefeedID.Namespace,
		},
	}

	if len(config.AuthenticationToken) > 0 {
		op.Authentication = pulsar.NewAuthenticationToken(config.AuthenticationToken)
	} else if len(config.TokenFromFile) > 0 {
		op.Authentication = pulsar.NewAuthenticationTokenFromFile(config.TokenFromFile)
	} else if len(config.BasicUserName) > 0 && len(config.BasicPassword) > 0 {
		authentication, err := pulsar.NewAuthenticationBasic(config.BasicUserName, config.BasicPassword)
		if err != nil {
			return nil, err
		}
		op.Authentication = authentication
	}

	if config.ConnectionTimeout > 0 {
		op.ConnectionTimeout = config.ConnectionTimeout
	}
	if config.OperationTimeout > 0 {
		op.OperationTimeout = config.OperationTimeout
	}

	pulsarClient, err := pulsar.NewClient(op)
	if err != nil {
		log.L().Error("Cannot connect to pulsar", zap.Error(err))
		return nil, err
	}
	return pulsarClient, nil
}
