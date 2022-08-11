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
	"strings"

	"github.com/pingcap/tiflow/cdc/sink/mq/producer/kafka"
	"github.com/pingcap/tiflow/cdc/sinkv2/ddlsink"
	"github.com/pingcap/tiflow/cdc/sinkv2/ddlsink/blackhole"
	"github.com/pingcap/tiflow/cdc/sinkv2/ddlsink/mq"
	"github.com/pingcap/tiflow/cdc/sinkv2/ddlsink/mq/ddlproducer"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink"
)

// New creates a new ddlsink.DDLEventSink by schema.
func New(
	ctx context.Context,
	sinkURIStr string,
	cfg *config.ReplicaConfig,
) (ddlsink.DDLEventSink, error) {
	sinkURI, err := config.GetSinkURIAndAdjustConfigWithSinkURI(sinkURIStr, cfg)
	if err != nil {
		return nil, err
	}
	schema := strings.ToLower(sinkURI.Scheme)
	// TODO: add more sink factory here.
	switch schema {
	case sink.KafkaSchema, sink.KafkaSSLSchema:
		return mq.NewKafkaDDLSink(ctx, sinkURI, cfg,
			kafka.NewAdminClientImpl, ddlproducer.NewKafkaDDLProducer)
	case sink.BlackHoleSchema:
		return blackhole.New(), nil
	default:
		return nil,
			cerror.ErrSinkURIInvalid.GenWithStack("the sink scheme (%s) is not supported", schema)
	}
}
