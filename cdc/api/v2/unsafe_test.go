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

	"github.com/golang/mock/gomock"
	"github.com/pingcap/errors"
	mock_capture "github.com/pingcap/tiflow/cdc/capture/mock"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/upstream"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
)

var withFuntion = func(context.Context, pd.Client) error { return nil }

func TestWithUpstreamConfig(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	upManager := upstream.NewManager(ctx, "abc")
	upManager.AddUpstream(1,
		&model.UpstreamInfo{
			PDEndpoints: "http://127.0.0.1:22379",
		})
	cpCtrl := gomock.NewController(t)
	cp := mock_capture.NewMockCapture(cpCtrl)
	hpCtrl := gomock.NewController(t)
	helpers := NewMockAPIV2Helpers(hpCtrl)

	cp.EXPECT().GetUpstreamManager().Return(upManager).AnyTimes()
	api := NewOpenAPIV2ForTest(cp, helpers)

	// upStream ID > 0 , getUpstream config ok
	upstreamConfig := &UpstreamConfig{
		ID: 1,
	}
	err := api.withUpstreamConfig(ctx, upstreamConfig, withFuntion)
	require.Nil(t, err)

	// upStream ID > 0, getUpstream config not ok
	upstreamConfig = &UpstreamConfig{
		ID: 2,
	}
	err = api.withUpstreamConfig(ctx, upstreamConfig, withFuntion)
	require.NotNil(t, err)

	// upStreamConfig.ID = 0, len(pdAddress) > 0 : failed to getPDClient
	upstreamConfig = &UpstreamConfig{
		PDConfig: PDConfig{
			PDAddrs: []string{"http://127.0.0.1:22379"},
		},
	}
	helpers.EXPECT().
		getPDClient(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&mockPDClient{}, nil)
	err = api.withUpstreamConfig(ctx, upstreamConfig, withFuntion)
	require.Nil(t, err)

	// upStreamConfig.ID = 0, len(pdAddress) > 0, get PDClient succeed
	upstreamConfig = &UpstreamConfig{
		PDConfig: PDConfig{
			PDAddrs: []string{"http://127.0.0.1:22379"},
		},
	}
	helpers.EXPECT().
		getPDClient(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&mockPDClient{}, errors.New("getPDClient failed"))
	err = api.withUpstreamConfig(ctx, upstreamConfig, withFuntion)
	require.NotNil(t, err)

	// upStreamConfig.ID = 0, len(pdAddress) > 0, no default upstream
	upstreamConfig = &UpstreamConfig{}
	require.Panics(t, func() {
		_ = api.withUpstreamConfig(ctx, upstreamConfig, withFuntion)
	})

	// success
	upManager = upstream.NewManager4Test(&mockPDClient{})
	cp = mock_capture.NewMockCapture(gomock.NewController(t))
	cp.EXPECT().GetUpstreamManager().Return(upManager).AnyTimes()
	api = NewOpenAPIV2ForTest(cp, helpers)
	upstreamConfig = &UpstreamConfig{}
	err = api.withUpstreamConfig(ctx, upstreamConfig, withFuntion)
	require.Nil(t, err)
}
