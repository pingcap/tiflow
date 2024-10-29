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
// limitations under the License

package factory

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink/mq"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink/mq/dmlproducer"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/pingcap/tiflow/pkg/sink/kafka"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func newForTest(ctx context.Context,
	sinkURIStr string,
	cfg *config.ReplicaConfig,
	errCh chan error,
) (*SinkFactory, error) {
	sinkURI, err := url.Parse(sinkURIStr)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrSinkURIInvalid, err)
	}

	s := &SinkFactory{}
	schema := strings.ToLower(sinkURI.Scheme)
	switch schema {
	case "kafka", "kafka+ssl":
		mqs, err := mq.NewKafkaDMLSink(ctx, model.DefaultChangeFeedID("test"),
			sinkURI, cfg, errCh,
			// Use mock kafka clients for test.
			kafka.NewMockFactory, dmlproducer.NewDMLMockProducer)
		if err != nil {
			return nil, err
		}
		s.txnSink = mqs
	default:
		return nil,
			cerror.ErrSinkURIInvalid.GenWithStack("the sink scheme (%s) is not supported", schema)
	}
	return s, nil
}

func TestSinkFactory(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctx = context.WithValue(ctx, "testing.T", t)
	uriTemplate := "kafka://%s/%s?kafka-version=0.9.0.0&max-batch-size=1" +
		"&max-message-bytes=1048576&partition-num=1" +
		"&kafka-client-id=unit-test&auto-create-topic=false&compression=gzip&protocol=open-protocol"
	uri := fmt.Sprintf(uriTemplate, "127.0.0.1:9092", kafka.DefaultMockTopicName)

	sinkURI, err := url.Parse(uri)
	require.NoError(t, err)
	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Sink.KafkaConfig = &config.KafkaConfig{}
	require.NoError(t, replicaConfig.ValidateAndAdjust(sinkURI))
	errCh := make(chan error, 1)

	sinkFactory, err := newForTest(ctx, uri, replicaConfig, errCh)
	require.NotNil(t, sinkFactory)
	require.NoError(t, err)
	require.NotNil(t, sinkFactory.txnSink)

	tableSink := sinkFactory.CreateTableSink(model.DefaultChangeFeedID("1"),
		spanz.TableIDToComparableSpan(1),
		0,
		pdutil.NewClock4Test(),
		prometheus.NewCounter(prometheus.CounterOpts{}),
		prometheus.NewHistogram(prometheus.HistogramOpts{}))
	require.NotNil(t, tableSink, "table sink can be created")

	sinkFactory.Close()
}
