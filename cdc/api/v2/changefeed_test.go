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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/golang/mock/gomock"
	tidbkv "github.com/pingcap/tidb/kv"
	mock_v2 "github.com/pingcap/tiflow/cdc/api/v2/mock"
	mock_capture "github.com/pingcap/tiflow/cdc/capture/mock"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/owner"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/upstream"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

var (
	changeFeedID         = model.DefaultChangeFeedID("test-changeFeed")
	captureID            = "test-capture"
	nonExistChangefeedID = model.DefaultChangeFeedID("non-exist-changefeed")
	blackHoleSink        = "blackhole://"
)

type mockUpstreamManager struct {
	up *upstream.Upstream
	x  upstream.Manager
}

func (m *mockUpstreamManager) GetDefaultUpstream() *upstream.Upstream {
	return m.up
}

type mockEtcdClient struct {
	etcd.CDCEtcdClientForAPI
	upstreamExists  bool
	createOpSuccess bool
	updateOpSuccess bool
	err             error
}

func (*mockEtcdClient) GetEnsureGCServiceID() string {
	return "demo-gc-service-id"
}

func (m *mockEtcdClient) GetUpstreamInfo(
	context.Context, model.UpstreamID, string,
) (*model.UpstreamInfo, error) {
	if m.upstreamExists {
		return nil, nil
	}
	return nil, cerror.ErrUpstreamNotFound.FastGen("MockEtcdError")
}

func (m *mockEtcdClient) CreateChangefeedInfo(ctx context.Context,
	upstreamInfo *model.UpstreamInfo,
	info *model.ChangeFeedInfo,
	changeFeedID model.ChangeFeedID,
) error {
	if m.createOpSuccess {
		return nil
	}
	return cerror.ErrChangeFeedAlreadyExists.GenWithStackByArgs(changeFeedID)
}

func (m *mockEtcdClient) UpdateChangefeedAndUpstream(ctx context.Context,
	upstreamInfo *model.UpstreamInfo,
	changeFeedInfo *model.ChangeFeedInfo,
	changeFeedID model.ChangeFeedID,
) error {
	if m.updateOpSuccess {
		return nil
	}
	return cerror.ErrChangefeedUpdateFailedTransaction.GenWithStackByArgs(changeFeedID)
}

func (m *mockEtcdClient) GetAllCDCInfo(ctx context.Context) ([]*mvccpb.KeyValue, error) {
	return nil, nil
}

func (m *mockEtcdClient) GetGCServiceID() string {
	return fmt.Sprintf("ticdc-%s-%d", "defalut", 0)
}

func TestCreateChangefeed(t *testing.T) {
	t.Parallel()

	helperCtrl := gomock.NewController(t)
	helper := mock_v2.NewMockAPIV2Helpers(helperCtrl)
	captureCtrl := gomock.NewController(t)
	cp := mock_capture.NewMockCaptureInfoProvider(captureCtrl)
	apiV2 := NewOpenAPIV2ForTest(cp, helper)
	router := newRouter(apiV2)

	pdClient := &mockPDClient{}
	statusProvider := &mockStatusProvider{}
	mockUpManager := upstream.NewManager4Test(pdClient)

	helper.EXPECT().
		GetPDClient(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(pdClient, nil)
	helper.EXPECT().
		CreateTiStore(gomock.Any(), gomock.Any()).
		Return(nil, nil)
	helper.EXPECT().
		VerifyCreateChangefeedConfig(gomock.Any(), gomock.Any(), gomock.Any(),
			gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context,
			cfg *ChangefeedConfig,
			pdClient pd.Client,
			statusProvider owner.StatusProvider,
			ensureGCServiceID string,
			kvStorage tidbkv.Storage,
		) (*model.ChangeFeedInfo, error) {
			require.EqualValues(t, cfg.ID, changeFeedID.ID)
			require.EqualValues(t, cfg.SinkURI, blackHoleSink)
			return &model.ChangeFeedInfo{
				UpstreamID: 0,
				ID:         cfg.ID,
				SinkURI:    cfg.SinkURI,
			}, nil
		})

	mockEtcdCli := &mockEtcdClient{
		upstreamExists:  true,
		createOpSuccess: true,
	}

	cp.EXPECT().
		StatusProvider().
		Return(statusProvider)
	cp.EXPECT().
		GetEtcdClient().
		Return(mockEtcdCli).AnyTimes()
	cp.EXPECT().
		GetUpstreamManager().
		Return(mockUpManager).AnyTimes()
	cp.EXPECT().IsReady().Return(true)
	cp.EXPECT().IsOwner().Return(true)

	// test succeed
	config1 := struct {
		ID      string   `json:"changefeed_id"`
		SinkURI string   `json:"sink_uri"`
		PDAddrs []string `json:"pd_addrs"`
	}{ID: changeFeedID.ID, SinkURI: blackHoleSink, PDAddrs: []string{"http://127.0.0.1:2379"}}
	b, err := json.Marshal(&config1)
	require.Nil(t, err)
	body := bytes.NewReader(b)

	case1 := testCase{url: "/api/v2/changefeeds", method: "POST"}
	w := httptest.NewRecorder()
	req, _ := http.NewRequestWithContext(context.Background(), case1.method, case1.url, body)
	router.ServeHTTP(w, req)
	require.Equal(t, 201, w.Code)

	require.Nil(t, err)
}

func TestUpdateChangefeed(t *testing.T) {}
