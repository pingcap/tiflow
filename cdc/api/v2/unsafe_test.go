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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/errors"
	mock_capture "github.com/pingcap/tiflow/cdc/capture/mock"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	mock_etcd "github.com/pingcap/tiflow/pkg/etcd/mock"
	"github.com/pingcap/tiflow/pkg/upstream"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

var withFuntion = func(context.Context, pd.Client) error { return nil }

func TestCDCMetaData(t *testing.T) {
	t.Parallel()

	metaData := testCase{url: "/api/v2/unsafe/metadata", method: "GET"}

	cp := mock_capture.NewMockCapture(gomock.NewController(t))
	helpers := NewMockAPIV2Helpers(gomock.NewController(t))
	apiV2 := NewOpenAPIV2ForTest(cp, helpers)
	router := newRouter(apiV2)

	etcdClient := mock_etcd.NewMockCDCEtcdClient(gomock.NewController(t))
	cp.EXPECT().IsController().Return(true).AnyTimes()
	cp.EXPECT().IsReady().Return(true).AnyTimes()
	cp.EXPECT().GetEtcdClient().Return(etcdClient).AnyTimes()

	// case 1: failed
	etcdClient.EXPECT().GetAllCDCInfo(gomock.Any()).Return(nil, cerror.ErrPDEtcdAPIError).Times(1)

	w := httptest.NewRecorder()
	req, _ := http.NewRequestWithContext(context.Background(), metaData.method, metaData.url, nil)
	router.ServeHTTP(w, req)
	respErr := model.HTTPError{}
	err := json.NewDecoder(w.Body).Decode(&respErr)
	require.Nil(t, err)
	require.Contains(t, respErr.Code, "ErrPDEtcdAPIError")
	require.Equal(t, http.StatusInternalServerError, w.Code)

	// case 2: success
	kvs := []*mvccpb.KeyValue{{Key: []byte{byte(0)}, Value: []byte{byte(1)}}}
	etcdClient.EXPECT().GetAllCDCInfo(gomock.Any()).Return(kvs, nil).Times(1)

	w = httptest.NewRecorder()
	req, _ = http.NewRequestWithContext(context.Background(), metaData.method, metaData.url, nil)
	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var resp []EtcdData
	err = json.NewDecoder(w.Body).Decode(&resp)
	require.Nil(t, err)
	require.Equal(t, len(kvs), len(resp))
	for i, kv := range resp {
		require.Equal(t, string(kvs[i].Key), kv.Key)
		require.Equal(t, string(kvs[i].Key), kv.Key)
	}
}

func TestWithUpstreamConfig(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	upManager := upstream.NewManager(ctx, upstream.CaptureTopologyCfg{GCServiceID: "abc"})
	upManager.AddUpstream(&model.UpstreamInfo{
		ID:          uint64(1),
		PDEndpoints: "http://127.0.0.1:22379",
	})
	cpCtrl := gomock.NewController(t)
	cp := mock_capture.NewMockCapture(cpCtrl)
	hpCtrl := gomock.NewController(t)
	helpers := NewMockAPIV2Helpers(hpCtrl)

	cp.EXPECT().GetUpstreamManager().Return(upManager, nil).AnyTimes()
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
	err = api.withUpstreamConfig(ctx, upstreamConfig, withFuntion)
	require.NotNil(t, err)

	// success
	upManager = upstream.NewManager4Test(&mockPDClient{})
	cp = mock_capture.NewMockCapture(gomock.NewController(t))
	cp.EXPECT().GetUpstreamManager().Return(upManager, nil).AnyTimes()
	api = NewOpenAPIV2ForTest(cp, helpers)
	upstreamConfig = &UpstreamConfig{}
	err = api.withUpstreamConfig(ctx, upstreamConfig, withFuntion)
	require.Nil(t, err)
}
