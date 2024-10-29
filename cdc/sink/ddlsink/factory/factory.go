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

package factory

import (
	"context"
	"net/url"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/ddlsink"
	"github.com/pingcap/tiflow/cdc/sink/ddlsink/blackhole"
	"github.com/pingcap/tiflow/cdc/sink/ddlsink/cloudstorage"
	"github.com/pingcap/tiflow/cdc/sink/ddlsink/mq"
	"github.com/pingcap/tiflow/cdc/sink/ddlsink/mq/ddlproducer"
	"github.com/pingcap/tiflow/cdc/sink/ddlsink/mysql"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink/mq/manager"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink"
	"github.com/pingcap/tiflow/pkg/sink/kafka"
	kafkav2 "github.com/pingcap/tiflow/pkg/sink/kafka/v2"
	pulsarConfig "github.com/pingcap/tiflow/pkg/sink/pulsar"
	"github.com/pingcap/tiflow/pkg/util"
)

// New creates a new ddlsink.Sink by scheme.
func New(
	ctx context.Context,
	changefeedID model.ChangeFeedID,
	sinkURIStr string,
	cfg *config.ReplicaConfig,
) (ddlsink.Sink, error) {
	sinkURI, err := url.Parse(sinkURIStr)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrSinkURIInvalid, err)
	}
	scheme := sink.GetScheme(sinkURI)
	switch scheme {
	case sink.KafkaScheme, sink.KafkaSSLScheme:
		factoryCreator := kafka.NewSaramaFactory
		if util.GetOrZero(cfg.Sink.EnableKafkaSinkV2) {
			factoryCreator = kafkav2.NewFactory
		}
		return mq.NewKafkaDDLSink(ctx, changefeedID, sinkURI, cfg,
			factoryCreator, ddlproducer.NewKafkaDDLProducer)
	case sink.BlackHoleScheme:
		return blackhole.NewDDLSink(), nil
	case sink.MySQLSSLScheme, sink.MySQLScheme, sink.TiDBScheme, sink.TiDBSSLScheme:
		return mysql.NewDDLSink(ctx, changefeedID, sinkURI, cfg)
	case sink.S3Scheme, sink.FileScheme, sink.GCSScheme, sink.GSScheme, sink.AzblobScheme, sink.AzureScheme, sink.CloudStorageNoopScheme:
		return cloudstorage.NewDDLSink(ctx, changefeedID, sinkURI, cfg)
	case sink.PulsarScheme, sink.PulsarSSLScheme, sink.PulsarHTTPScheme, sink.PulsarHTTPSScheme:
		return mq.NewPulsarDDLSink(ctx, changefeedID, sinkURI, cfg, manager.NewPulsarTopicManager,
			pulsarConfig.NewCreatorFactory, ddlproducer.NewPulsarProducer)
	default:
		return nil,
			cerror.ErrSinkURIInvalid.GenWithStack("the sink scheme (%s) is not supported", scheme)
	}
}
