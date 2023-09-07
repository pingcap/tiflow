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

package manager

import (
	"context"
	"net/url"
	"testing"

	"github.com/pingcap/tiflow/pkg/config"
	pulsarConfig "github.com/pingcap/tiflow/pkg/sink/pulsar"
	"github.com/stretchr/testify/require"
)

func newPulsarConfig(t *testing.T) (*config.PulsarConfig, *url.URL) {
	sinkURL := "pulsar://127.0.0.1:6650/persistent://public/default/test?" +
		"protocol=canal-json&pulsar-version=v2.10.0&enable-tidb-extension=true&" +
		"authentication-token=eyJhbGciOiJSUzIxxxxxxxxxxxxxxxx"

	sinkURI, err := url.Parse(sinkURL)
	require.NoError(t, err)
	replicaConfig := config.GetDefaultReplicaConfig()
	require.NoError(t, replicaConfig.ValidateAndAdjust(sinkURI))
	require.NoError(t, err)
	c, err := pulsarConfig.NewPulsarConfig(sinkURI, replicaConfig.Sink.PulsarConfig)
	require.NoError(t, err)
	return c, sinkURI
}

func TestGetPartitionNumMock(t *testing.T) {
	t.Parallel()

	cfg, _ := newPulsarConfig(t)

	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Sink = &config.SinkConfig{
		Protocol: str2Pointer("canal-json"),
	}

	ctx := context.Background()

	ctx = context.WithValue(ctx, "testing.T", t)
	pm, err := NewMockPulsarTopicManager(cfg, nil)
	require.NoError(t, err)
	require.NotNil(t, pm)

	pn, err := pm.GetPartitionNum(ctx, "test")
	require.NoError(t, err)
	require.Equal(t, int32(3), pn)
}
