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

package dmlproducer

import (
	"context"
	"net/url"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	pulsarConfig "github.com/pingcap/tiflow/pkg/sink/pulsar"
	"github.com/stretchr/testify/require"
)

// newPulsarConfig set config
func newPulsarConfig(t *testing.T) (sinkURI *url.URL, replicaConfig *config.ReplicaConfig) {
	sinkURL := "pulsar://127.0.0.1:6650/persistent://public/default/test?" +
		"protocol=canal-json&pulsar-version=v2.10.0&enable-tidb-extension=true&" +
		"authentication-token=eyJhbcGcixxxxxxxxxxxxxx"
	var err error
	sinkURI, err = url.Parse(sinkURL)
	require.NoError(t, err)
	replicaConfig = config.GetDefaultReplicaConfig()
	require.NoError(t, replicaConfig.ValidateAndAdjust(sinkURI))
	var c *config.PulsarConfig
	c, err = pulsarConfig.NewPulsarConfig(sinkURI, replicaConfig.Sink.PulsarConfig)
	require.NoError(t, err)
	replicaConfig.Sink.PulsarConfig = c
	return sinkURI, replicaConfig
}

func TestNewPulsarDMLProducer(t *testing.T) {
	t.Parallel()

	sinkURI, rc := newPulsarConfig(t)
	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Sink = &config.SinkConfig{
		Protocol: aws.String("canal-json"),
	}
	t.Logf(sinkURI.String())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)

	ctx = context.WithValue(ctx, "testing.T", t)
	changefeed := model.DefaultChangeFeedID("changefeed-test")

	failpointCh := make(chan error, 1)

	client, err := pulsarConfig.NewMockCreatorFactory(rc.Sink.PulsarConfig, changefeed, rc.Sink)
	require.NoError(t, err)
	dml, err := NewPulsarDMLProducerMock(ctx, changefeed, client, rc.Sink, errCh, failpointCh)
	require.NoError(t, err)
	require.NotNil(t, dml)
}

func Test_pulsarDMLProducer_AsyncSendMessage(t *testing.T) {
	t.Parallel()

	_, rc := newPulsarConfig(t)
	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Sink = &config.SinkConfig{
		Protocol: aws.String("canal-json"),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)

	ctx = context.WithValue(ctx, "testing.T", t)
	changefeed := model.DefaultChangeFeedID("changefeed-test")

	failpointCh := make(chan error, 1)

	client, err := pulsarConfig.NewMockCreatorFactory(rc.Sink.PulsarConfig, changefeed, rc.Sink)
	require.NoError(t, err)
	dml, err := NewPulsarDMLProducerMock(ctx, changefeed, client, rc.Sink, errCh, failpointCh)
	require.NoError(t, err)
	require.NotNil(t, dml)

	err = dml.AsyncSendMessage(ctx, "test", 0, &common.Message{
		Value:        []byte("this value for test input data"),
		PartitionKey: str2Pointer("test_key"),
	})
	require.NoError(t, err)
}
