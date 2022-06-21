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

package v2

import (
	"context"
	"testing"

	"github.com/pingcap/tiflow/cdc/capture"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/upstream"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
)

func TestWithUpstreamConfig(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	api := OpenAPIV2{capture: &capture.Capture{}}
	api.capture.UpstreamManager = upstream.NewManager(ctx, "abc")
	api.capture.UpstreamManager.AddUpstream(1,
		&model.UpstreamInfo{
			PDEndpoints: "http://127.0.0.1:22379",
		})
	upstreamConfig := &UpstreamConfig{
		ID: 1,
	}
	err := api.withUpstreamConfig(ctx, upstreamConfig, func(ctx context.Context,
		client pd.Client) error {
		return nil
	})
	upstreamConfig = &UpstreamConfig{
		ID: 2,
	}
	err = api.withUpstreamConfig(ctx, upstreamConfig, func(ctx context.Context,
		client pd.Client) error {
		return nil
	})
	require.NotNil(t, err)
	upstreamConfig = &UpstreamConfig{PDAddrs: []string{"http://127.0.0.1:22379"}}
	err = api.withUpstreamConfig(ctx, upstreamConfig, func(ctx context.Context,
		client pd.Client) error {
		return nil
	})
	require.NotNil(t, err)
	upstreamConfig = &UpstreamConfig{}

	require.Panics(t, func() {
		_ = api.withUpstreamConfig(ctx, upstreamConfig, func(ctx context.Context,
			client pd.Client) error {
			return nil
		})
	})
}
