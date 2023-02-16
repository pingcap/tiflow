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
	mock_capture "github.com/pingcap/tiflow/cdc/capture/mock"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/owner"
	mock_owner "github.com/pingcap/tiflow/cdc/owner/mock"
	"github.com/pingcap/tiflow/pkg/config"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/etcd"
	mock_etcd "github.com/pingcap/tiflow/pkg/etcd/mock"
	"github.com/pingcap/tiflow/pkg/upstream"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
)

var (
	changeFeedID  = model.DefaultChangeFeedID("test-changeFeed")
	blackholeSink = "blackhole://"
	mysqlSink     = "mysql://root:123456@127.0.0.1:3306"
)

func TestCreateChangefeed(t *testing.T) {
	t.Parallel()
	create := testCase{url: "/api/v2/changefeeds", method: "POST"}

	pdClient := &mockPDClient{}
	helpers := NewMockAPIV2Helpers(gomock.NewController(t))
	cp := mock_capture.NewMockCapture(gomock.NewController(t))
	etcdClient := mock_etcd.NewMockCDCEtcdClient(gomock.NewController(t))
	apiV2 := NewOpenAPIV2ForTest(cp, helpers)
	router := newRouter(apiV2)

	o := mock_owner.NewMockOwner(gomock.NewController(t))
	mockUpManager := upstream.NewManager4Test(pdClient)
	statusProvider := &mockStatusProvider{}
	etcdClient.EXPECT().
		GetEnsureGCServiceID(gomock.Any()).
		Return(etcd.GcServiceIDForTest()).AnyTimes()
	cp.EXPECT().StatusProvider().Return(statusProvider).AnyTimes()
	cp.EXPECT().GetEtcdClient().Return(etcdClient, nil).AnyTimes()
	cp.EXPECT().GetUpstreamManager().Return(mockUpManager, nil).AnyTimes()
	cp.EXPECT().IsReady().Return(true).AnyTimes()
	cp.EXPECT().IsOwner().Return(true).AnyTimes()
	cp.EXPECT().GetOwner().Return(o, nil).AnyTimes()
	o.EXPECT().ValidateChangefeed(gomock.Any()).Return(nil).AnyTimes()

	// case 1: json format mismatches with the spec.
	errConfig := struct {
		ID      string `json:"changefeed_id"`
		SinkURI string `json:"sink_uri"`
		PDAddrs string `json:"pd_addrs"` // should be an array
	}{
		ID:      changeFeedID.ID,
		SinkURI: blackholeSink,
		PDAddrs: "http://127.0.0.1:2379",
	}
	bodyErr, err := json.Marshal(&errConfig)
	require.Nil(t, err)
	w := httptest.NewRecorder()
	req, _ := http.NewRequestWithContext(context.Background(),
		create.method, create.url, bytes.NewReader(bodyErr))
	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code)
	respErr := model.HTTPError{}
	err = json.NewDecoder(w.Body).Decode(&respErr)
	require.Nil(t, err)
	require.Contains(t, respErr.Code, "ErrAPIInvalidParam")

	cfConfig := struct {
		ID      string   `json:"changefeed_id"`
		SinkURI string   `json:"sink_uri"`
		PDAddrs []string `json:"pd_addrs"`
	}{
		ID:      changeFeedID.ID,
		SinkURI: blackholeSink,
		PDAddrs: []string{},
	}
	body, err := json.Marshal(&cfConfig)
	require.Nil(t, err)

	// case 2: getPDClient failed, it may happen with wrong PDAddrs
	helpers.EXPECT().
		getPDClient(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, cerrors.ErrAPIGetPDClientFailed).Times(1)

	w = httptest.NewRecorder()
	req, _ = http.NewRequestWithContext(context.Background(), create.method,
		create.url, bytes.NewReader(body))
	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusInternalServerError, w.Code)
	respErr = model.HTTPError{}
	err = json.NewDecoder(w.Body).Decode(&respErr)
	require.Nil(t, err)
	require.Contains(t, respErr.Code, "ErrAPIGetPDClientFailed")

	// case 3: failed to create TiStore
	helpers.EXPECT().
		getPDClient(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(pdClient, nil).AnyTimes()
	helpers.EXPECT().
		createTiStore(gomock.Any(), gomock.Any()).
		Return(nil, cerrors.ErrNewStore).
		Times(1)
	cfConfig.PDAddrs = []string{"http://127.0.0.1:2379", "http://127.0.0.1:2382"}
	body, err = json.Marshal(&cfConfig)
	require.Nil(t, err)
	w = httptest.NewRecorder()
	req, _ = http.NewRequestWithContext(context.Background(), create.method,
		create.url, bytes.NewReader(body))
	router.ServeHTTP(w, req)
	respErr = model.HTTPError{}
	err = json.NewDecoder(w.Body).Decode(&respErr)
	require.Nil(t, err)
	require.Contains(t, respErr.Code, "ErrNewStore")
	require.Equal(t, http.StatusInternalServerError, w.Code)

	// case 4: failed to verify tables
	helpers.EXPECT().
		createTiStore(gomock.Any(), gomock.Any()).
		Return(nil, nil).
		AnyTimes()
	helpers.EXPECT().
		verifyCreateChangefeedConfig(gomock.Any(), gomock.Any(), gomock.Any(),
			gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, cerrors.ErrSinkURIInvalid.GenWithStackByArgs(
			"sink_uri is empty, can't not create a changefeed without sink_uri"))
	cfConfig.SinkURI = ""
	body, err = json.Marshal(&cfConfig)
	require.Nil(t, err)

	w = httptest.NewRecorder()
	req, _ = http.NewRequestWithContext(context.Background(), create.method,
		create.url, bytes.NewReader(body))
	router.ServeHTTP(w, req)
	respErr = model.HTTPError{}
	err = json.NewDecoder(w.Body).Decode(&respErr)
	require.Nil(t, err)
	require.Contains(t, respErr.Code, "ErrSinkURIInvalid")
	require.Equal(t, http.StatusBadRequest, w.Code)

	// case 5:
	helpers.EXPECT().getVerfiedTables(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, nil, nil).
		AnyTimes()
	helpers.EXPECT().
		verifyCreateChangefeedConfig(gomock.Any(), gomock.Any(), gomock.Any(),
			gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context,
			cfg *ChangefeedConfig,
			pdClient pd.Client,
			statusProvider owner.StatusProvider,
			ensureGCServiceID string,
			kvStorage tidbkv.Storage,
		) (*model.ChangeFeedInfo, error) {
			require.EqualValues(t, cfg.ID, changeFeedID.ID)
			require.EqualValues(t, cfg.SinkURI, mysqlSink)
			return &model.ChangeFeedInfo{
				UpstreamID: 1,
				ID:         cfg.ID,
				SinkURI:    cfg.SinkURI,
			}, nil
		}).AnyTimes()
	etcdClient.EXPECT().
		CreateChangefeedInfo(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(cerrors.ErrPDEtcdAPIError).Times(1)

	cfConfig.SinkURI = mysqlSink
	body, err = json.Marshal(&cfConfig)
	require.Nil(t, err)
	w = httptest.NewRecorder()
	req, _ = http.NewRequestWithContext(context.Background(), create.method,
		create.url, bytes.NewReader(body))
	router.ServeHTTP(w, req)
	respErr = model.HTTPError{}
	err = json.NewDecoder(w.Body).Decode(&respErr)
	require.Nil(t, err)
	require.Contains(t, respErr.Code, "ErrPDEtcdAPIError")
	require.Equal(t, http.StatusInternalServerError, w.Code)

	// case 6: success
	etcdClient.EXPECT().
		CreateChangefeedInfo(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil).
		AnyTimes()
	w = httptest.NewRecorder()
	req, _ = http.NewRequestWithContext(context.Background(), create.method,
		create.url, bytes.NewReader(body))
	router.ServeHTTP(w, req)
	resp := ChangeFeedInfo{}
	err = json.NewDecoder(w.Body).Decode(&resp)
	require.Nil(t, err)
	require.Equal(t, cfConfig.ID, resp.ID)
	mysqlSink, err = util.MaskSinkURI(mysqlSink)
	require.Nil(t, err)
	require.Equal(t, mysqlSink, resp.SinkURI)
	require.Equal(t, http.StatusCreated, w.Code)
}

func TestUpdateChangefeed(t *testing.T) {
	t.Parallel()
	update := testCase{url: "/api/v2/changefeeds/%s", method: "PUT"}
	helpers := NewMockAPIV2Helpers(gomock.NewController(t))
	cp := mock_capture.NewMockCapture(gomock.NewController(t))
	apiV2 := NewOpenAPIV2ForTest(cp, helpers)
	router := newRouter(apiV2)

	statusProvider := &mockStatusProvider{}
	cp.EXPECT().StatusProvider().Return(statusProvider).AnyTimes()
	cp.EXPECT().IsReady().Return(true).AnyTimes()
	cp.EXPECT().IsOwner().Return(true).AnyTimes()

	// case 1 invalid id
	invalidID := "Invalid_#"
	w := httptest.NewRecorder()
	req, _ := http.NewRequestWithContext(context.Background(), update.method,
		fmt.Sprintf(update.url, invalidID), nil)
	router.ServeHTTP(w, req)
	respErr := model.HTTPError{}
	err := json.NewDecoder(w.Body).Decode(&respErr)
	require.Nil(t, err)
	require.Contains(t, respErr.Code, "ErrAPIInvalidParam")
	require.Equal(t, http.StatusBadRequest, w.Code)

	// case 2: failed to get changefeedInfo
	validID := changeFeedID.ID
	statusProvider.err = cerrors.ErrChangeFeedNotExists.GenWithStackByArgs(validID)
	w = httptest.NewRecorder()
	req, _ = http.NewRequestWithContext(context.Background(), update.method,
		fmt.Sprintf(update.url, validID), nil)
	router.ServeHTTP(w, req)
	respErr = model.HTTPError{}
	err = json.NewDecoder(w.Body).Decode(&respErr)
	require.Nil(t, err)
	require.Contains(t, respErr.Code, "ErrChangeFeedNotExists")
	require.Equal(t, http.StatusBadRequest, w.Code)

	// case 3: changefeed not stopped
	oldCfInfo := &model.ChangeFeedInfo{
		ID:         validID,
		State:      "normal",
		UpstreamID: 1,
		Namespace:  model.DefaultNamespace,
		Config:     &config.ReplicaConfig{},
	}
	statusProvider.err = nil
	statusProvider.changefeedInfo = oldCfInfo
	w = httptest.NewRecorder()
	req, _ = http.NewRequestWithContext(context.Background(), update.method,
		fmt.Sprintf(update.url, validID), nil)
	router.ServeHTTP(w, req)
	respErr = model.HTTPError{}
	err = json.NewDecoder(w.Body).Decode(&respErr)
	require.Nil(t, err)
	require.Contains(t, respErr.Code, "ErrChangefeedUpdateRefused")
	require.Equal(t, http.StatusBadRequest, w.Code)

	// case 4: changefeed stopped, but get upstream failed: not found
	oldCfInfo.UpstreamID = 100
	oldCfInfo.State = "stopped"
	etcdClient := mock_etcd.NewMockCDCEtcdClient(gomock.NewController(t))
	etcdClient.EXPECT().
		GetUpstreamInfo(gomock.Any(), gomock.Eq(uint64(100)), gomock.Any()).
		Return(nil, cerrors.ErrUpstreamNotFound).Times(1)
	cp.EXPECT().GetEtcdClient().Return(etcdClient, nil).AnyTimes()

	w = httptest.NewRecorder()
	req, _ = http.NewRequestWithContext(context.Background(), update.method,
		fmt.Sprintf(update.url, validID), nil)
	router.ServeHTTP(w, req)
	respErr = model.HTTPError{}
	err = json.NewDecoder(w.Body).Decode(&respErr)
	require.Nil(t, err)
	require.Contains(t, respErr.Code, "ErrUpstreamNotFound")
	require.Equal(t, http.StatusInternalServerError, w.Code)

	// case 5: json failed
	oldCfInfo.UpstreamID = 1
	etcdClient.EXPECT().
		GetUpstreamInfo(gomock.Any(), gomock.Eq(uint64(1)), gomock.Any()).
		Return(nil, nil).AnyTimes()
	cp.EXPECT().GetEtcdClient().Return(etcdClient, nil).AnyTimes()

	w = httptest.NewRecorder()
	req, _ = http.NewRequestWithContext(context.Background(), update.method,
		fmt.Sprintf(update.url, validID), nil)
	router.ServeHTTP(w, req)
	respErr = model.HTTPError{}
	err = json.NewDecoder(w.Body).Decode(&respErr)
	require.Nil(t, err)
	require.Contains(t, respErr.Code, "ErrAPIInvalidParam")
	require.Equal(t, http.StatusBadRequest, w.Code)

	// case 5: verify upstream failed
	helpers.EXPECT().
		verifyUpstream(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(cerrors.ErrUpstreamMissMatch).Times(1)
	updateCfg := &ChangefeedConfig{}
	body, err := json.Marshal(&updateCfg)
	require.Nil(t, err)
	w = httptest.NewRecorder()
	req, _ = http.NewRequestWithContext(context.Background(), update.method,
		fmt.Sprintf(update.url, validID), bytes.NewReader(body))
	router.ServeHTTP(w, req)
	respErr = model.HTTPError{}
	err = json.NewDecoder(w.Body).Decode(&respErr)
	require.Nil(t, err)
	require.Contains(t, respErr.Code, "ErrUpstreamMissMatch")
	require.Equal(t, http.StatusInternalServerError, w.Code)

	// case 6: verify update changefeed info failed
	helpers.EXPECT().
		verifyUpstream(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil).AnyTimes()
	helpers.EXPECT().
		createTiStore(gomock.Any(), gomock.Any()).
		Return(nil, nil).
		AnyTimes()
	helpers.EXPECT().
		verifyUpdateChangefeedConfig(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&model.ChangeFeedInfo{}, &model.UpstreamInfo{}, cerrors.ErrChangefeedUpdateRefused).
		Times(1)

	statusProvider.changefeedStatus = &model.ChangeFeedStatus{
		CheckpointTs: 1,
	}
	w = httptest.NewRecorder()
	req, _ = http.NewRequestWithContext(context.Background(), update.method,
		fmt.Sprintf(update.url, validID), bytes.NewReader(body))
	router.ServeHTTP(w, req)
	respErr = model.HTTPError{}
	err = json.NewDecoder(w.Body).Decode(&respErr)
	require.Nil(t, err)
	require.Contains(t, respErr.Code, "ErrChangefeedUpdateRefused")
	require.Equal(t, http.StatusBadRequest, w.Code)

	// case 7: update transaction failed
	helpers.EXPECT().
		verifyUpdateChangefeedConfig(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&model.ChangeFeedInfo{}, &model.UpstreamInfo{}, nil).
		Times(1)
	etcdClient.EXPECT().
		UpdateChangefeedAndUpstream(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(cerrors.ErrEtcdAPIError).Times(1)

	w = httptest.NewRecorder()
	req, _ = http.NewRequestWithContext(context.Background(), update.method,
		fmt.Sprintf(update.url, validID), bytes.NewReader(body))
	router.ServeHTTP(w, req)
	respErr = model.HTTPError{}
	err = json.NewDecoder(w.Body).Decode(&respErr)
	require.Nil(t, err)
	require.Contains(t, respErr.Code, "ErrEtcdAPIError")
	require.Equal(t, http.StatusInternalServerError, w.Code)

	// case 8: success
	helpers.EXPECT().
		verifyUpdateChangefeedConfig(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(oldCfInfo, &model.UpstreamInfo{}, nil).
		Times(1)
	etcdClient.EXPECT().
		UpdateChangefeedAndUpstream(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil).Times(1)

	w = httptest.NewRecorder()
	req, _ = http.NewRequestWithContext(context.Background(), update.method,
		fmt.Sprintf(update.url, validID), bytes.NewReader(body))
	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
}

func TestGetChangeFeedMetaInfo(t *testing.T) {
	t.Parallel()

	metaInfo := testCase{url: "/api/v2/changefeeds/%s/meta_info", method: "GET"}
	statusProvider := &mockStatusProvider{}
	cp := mock_capture.NewMockCapture(gomock.NewController(t))
	cp.EXPECT().IsReady().Return(true).AnyTimes()
	cp.EXPECT().IsOwner().Return(true).AnyTimes()

	apiV2 := NewOpenAPIV2ForTest(cp, APIV2HelpersImpl{})
	router := newRouter(apiV2)

	// case 1: invalid id
	w := httptest.NewRecorder()
	invalidID := "@^Invalid"
	req, _ := http.NewRequestWithContext(context.Background(),
		metaInfo.method, fmt.Sprintf(metaInfo.url, invalidID), nil)
	router.ServeHTTP(w, req)
	respErr := model.HTTPError{}
	err := json.NewDecoder(w.Body).Decode(&respErr)
	require.Nil(t, err)
	require.Contains(t, respErr.Code, "ErrAPIInvalidParam")
	// fmt.Sprintf("/api/v2/changefeeds/%s/meta_info", invalidID)

	// validId but not exists
	validID := "changefeed-valid-id"
	statusProvider.err = cerrors.ErrChangeFeedNotExists.GenWithStackByArgs(validID)
	cp.EXPECT().StatusProvider().Return(statusProvider).AnyTimes()

	w = httptest.NewRecorder()
	req, _ = http.NewRequestWithContext(context.Background(),
		metaInfo.method, fmt.Sprintf(metaInfo.url, validID), nil)
	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code)
	respErr = model.HTTPError{}
	err = json.NewDecoder(w.Body).Decode(&respErr)
	require.Nil(t, err)
	require.Contains(t, respErr.Code, "ErrChangeFeedNotExists")

	// valid but changefeed contains runtime error
	statusProvider.err = nil
	statusProvider.changefeedInfo = &model.ChangeFeedInfo{
		ID: validID,
		Error: &model.RunningError{
			Code: string(cerrors.ErrGCTTLExceeded.RFCCode()),
		},
	}
	w = httptest.NewRecorder()
	req, _ = http.NewRequestWithContext(context.Background(),
		metaInfo.method, fmt.Sprintf(metaInfo.url, validID), nil)
	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	resp := ChangeFeedInfo{}
	err = json.NewDecoder(w.Body).Decode(&resp)
	require.Nil(t, err)
	require.Equal(t, resp.ID, validID)
	require.Contains(t, resp.Error.Code, "ErrGCTTLExceeded")

	// success
	statusProvider.changefeedInfo = &model.ChangeFeedInfo{ID: validID}
	w = httptest.NewRecorder()
	req, _ = http.NewRequestWithContext(context.Background(),
		metaInfo.method, fmt.Sprintf(metaInfo.url, validID), nil)
	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	resp = ChangeFeedInfo{}
	err = json.NewDecoder(w.Body).Decode(&resp)
	require.Nil(t, err)
	require.Equal(t, resp.ID, validID)
	require.Nil(t, resp.Error)
}

func TestVerifyTable(t *testing.T) {
	t.Parallel()

	verify := &testCase{url: "/api/v2/verify_table", method: "POST"}

	pdClient := &mockPDClient{}
	upManager := upstream.NewManager4Test(pdClient)
	helpers := NewMockAPIV2Helpers(gomock.NewController(t))
	cp := mock_capture.NewMockCapture(gomock.NewController(t))
	// statusProvider := &mockStatusProvider{}
	// cp.EXPECT().StatusProvider().Return(statusProvider).AnyTimes()
	cp.EXPECT().GetUpstreamManager().Return(upManager, nil).AnyTimes()
	cp.EXPECT().IsOwner().Return(true).AnyTimes()
	cp.EXPECT().IsReady().Return(true).AnyTimes()

	apiV2 := NewOpenAPIV2ForTest(cp, helpers)
	router := newRouter(apiV2)

	// case 1: json format error
	w := httptest.NewRecorder()
	req, _ := http.NewRequestWithContext(context.Background(),
		verify.method, verify.url, nil)
	router.ServeHTTP(w, req)
	respErr := model.HTTPError{}
	err := json.NewDecoder(w.Body).Decode(&respErr)
	require.Nil(t, err)
	require.Contains(t, respErr.Code, "ErrAPIInvalidParam")

	// case 2: kv create failed
	updateCfg := getDefaultVerifyTableConfig()
	body, err := json.Marshal(&updateCfg)
	require.Nil(t, err)
	helpers.EXPECT().
		createTiStore(gomock.Any(), gomock.Any()).
		Return(nil, cerrors.ErrNewStore).
		Times(1)

	w = httptest.NewRecorder()
	req, _ = http.NewRequestWithContext(context.Background(),
		verify.method, verify.url, bytes.NewReader(body))
	router.ServeHTTP(w, req)
	respErr = model.HTTPError{}
	err = json.NewDecoder(w.Body).Decode(&respErr)
	require.Nil(t, err)
	require.Contains(t, respErr.Code, "ErrNewStore")

	// case 3: getVerfiedTables failed
	helpers.EXPECT().
		createTiStore(gomock.Any(), gomock.Any()).
		Return(nil, nil).
		AnyTimes()
	helpers.EXPECT().getVerfiedTables(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, nil, cerrors.ErrFilterRuleInvalid).
		Times(1)

	w = httptest.NewRecorder()
	req, _ = http.NewRequestWithContext(context.Background(),
		verify.method, verify.url, bytes.NewReader(body))
	router.ServeHTTP(w, req)
	respErr = model.HTTPError{}
	err = json.NewDecoder(w.Body).Decode(&respErr)
	require.Nil(t, err)
	require.Contains(t, respErr.Code, "ErrFilterRuleInvalid")

	// case 4: success
	eligible := []model.TableName{
		{Schema: "test", Table: "validTable1"},
		{Schema: "test", Table: "validTable2"},
	}
	ineligible := []model.TableName{
		{Schema: "test", Table: "invalidTable"},
	}
	helpers.EXPECT().getVerfiedTables(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(eligible, ineligible, nil)

	w = httptest.NewRecorder()
	req, _ = http.NewRequestWithContext(context.Background(),
		verify.method, verify.url, bytes.NewReader(body))
	router.ServeHTTP(w, req)
	resp := Tables{}
	err = json.NewDecoder(w.Body).Decode(&resp)
	require.Nil(t, err)
	require.Equal(t, http.StatusOK, w.Code)
}

func TestResumeChangefeed(t *testing.T) {
	resume := testCase{url: "/api/v2/changefeeds/%s/resume", method: "POST"}
	helpers := NewMockAPIV2Helpers(gomock.NewController(t))
	cp := mock_capture.NewMockCapture(gomock.NewController(t))
	owner := mock_owner.NewMockOwner(gomock.NewController(t))
	apiV2 := NewOpenAPIV2ForTest(cp, helpers)
	router := newRouter(apiV2)

	pdClient := &mockPDClient{}
	etcdClient := mock_etcd.NewMockCDCEtcdClient(gomock.NewController(t))
	mockUpManager := upstream.NewManager4Test(pdClient)
	statusProvider := &mockStatusProvider{}

	etcdClient.EXPECT().
		GetEnsureGCServiceID(gomock.Any()).
		Return(etcd.GcServiceIDForTest()).AnyTimes()
	cp.EXPECT().StatusProvider().Return(statusProvider).AnyTimes()
	cp.EXPECT().GetEtcdClient().Return(etcdClient, nil).AnyTimes()
	cp.EXPECT().GetUpstreamManager().Return(mockUpManager, nil).AnyTimes()
	cp.EXPECT().IsReady().Return(true).AnyTimes()
	cp.EXPECT().IsOwner().Return(true).AnyTimes()
	cp.EXPECT().GetOwner().Return(owner, nil).AnyTimes()
	owner.EXPECT().EnqueueJob(gomock.Any(), gomock.Any()).
		Do(func(adminJob model.AdminJob, done chan<- error) {
			require.EqualValues(t, changeFeedID, adminJob.CfID)
			require.EqualValues(t, model.AdminResume, adminJob.Type)
			close(done)
		}).AnyTimes()

	// case 1: invalid changefeed id
	w := httptest.NewRecorder()
	invalidID := "@^Invalid"
	req, _ := http.NewRequestWithContext(context.Background(),
		resume.method, fmt.Sprintf(resume.url, invalidID), nil)
	router.ServeHTTP(w, req)
	respErr := model.HTTPError{}
	err := json.NewDecoder(w.Body).Decode(&respErr)
	require.Nil(t, err)
	require.Contains(t, respErr.Code, "ErrAPIInvalidParam")

	// case 2: failed to get changefeedInfo
	validID := changeFeedID.ID
	statusProvider.err = cerrors.ErrChangeFeedNotExists.GenWithStackByArgs(validID)
	w = httptest.NewRecorder()
	req, _ = http.NewRequestWithContext(context.Background(), resume.method,
		fmt.Sprintf(resume.url, validID), nil)
	router.ServeHTTP(w, req)
	respErr = model.HTTPError{}
	err = json.NewDecoder(w.Body).Decode(&respErr)
	require.Nil(t, err)
	require.Contains(t, respErr.Code, "ErrChangeFeedNotExists")
	require.Equal(t, http.StatusBadRequest, w.Code)

	// case 3: failed to verify config
	statusProvider.err = nil
	statusProvider.changefeedInfo = &model.ChangeFeedInfo{ID: validID}
	helpers.EXPECT().
		getPDClient(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(pdClient, nil).AnyTimes()
	helpers.EXPECT().
		verifyResumeChangefeedConfig(gomock.Any(), gomock.Any(),
			gomock.Any(), gomock.Any(), gomock.Any()).Return(cerrors.ErrStartTsBeforeGC).Times(1)
	resumeCfg := &ResumeChangefeedConfig{}
	resumeCfg.OverwriteCheckpointTs = 100
	body, err := json.Marshal(&resumeCfg)
	require.Nil(t, err)
	w = httptest.NewRecorder()
	req, _ = http.NewRequestWithContext(context.Background(), resume.method,
		fmt.Sprintf(resume.url, validID), bytes.NewReader(body))
	router.ServeHTTP(w, req)
	respErr = model.HTTPError{}
	err = json.NewDecoder(w.Body).Decode(&respErr)
	require.Nil(t, err)
	require.Contains(t, respErr.Code, "ErrStartTsBeforeGC")
	require.Equal(t, http.StatusBadRequest, w.Code)

	// case 4: success without overwriting checkpointTs
	statusProvider.err = nil
	statusProvider.changefeedInfo = &model.ChangeFeedInfo{ID: validID}
	helpers.EXPECT().
		verifyResumeChangefeedConfig(gomock.Any(), gomock.Any(),
			gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(1)
	resumeCfg = &ResumeChangefeedConfig{}
	body, err = json.Marshal(&resumeCfg)
	require.Nil(t, err)
	w = httptest.NewRecorder()
	req, _ = http.NewRequestWithContext(context.Background(), resume.method,
		fmt.Sprintf(resume.url, validID), bytes.NewReader(body))
	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	// case 5: success with overwriting checkpointTs
	statusProvider.err = nil
	statusProvider.changefeedInfo = &model.ChangeFeedInfo{ID: validID}
	helpers.EXPECT().
		getPDClient(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(pdClient, nil).AnyTimes()
	helpers.EXPECT().
		verifyResumeChangefeedConfig(gomock.Any(), gomock.Any(),
			gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(1)
	resumeCfg = &ResumeChangefeedConfig{}
	resumeCfg.OverwriteCheckpointTs = 100
	body, err = json.Marshal(&resumeCfg)
	require.Nil(t, err)
	w = httptest.NewRecorder()
	req, _ = http.NewRequestWithContext(context.Background(), resume.method,
		fmt.Sprintf(resume.url, validID), bytes.NewReader(body))
	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
}
