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

package v2

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/golang/mock/gomock"
	mock_capture "github.com/pingcap/tiflow/cdc/capture/mock"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/stretchr/testify/require"
)

const (
	captureID = "test-capture"
)

func TestGetProcessor(t *testing.T) {
	t.Parallel()

	// case 1: invalid changefeed id.
	{
		cp := mock_capture.NewMockCapture(gomock.NewController(t))
		cp.EXPECT().IsReady().Return(true).AnyTimes()
		cp.EXPECT().IsOwner().Return(true).AnyTimes()

		apiV2 := NewOpenAPIV2ForTest(cp, APIV2HelpersImpl{})
		router := newRouter(apiV2)
		metainfo := testCase{
			url:    "/api/v2/processors/%s/%s",
			method: "GET",
		}
		invalidID := "@^Invalid"
		w := httptest.NewRecorder()
		req, _ := http.NewRequestWithContext(
			context.Background(),
			metainfo.method,
			fmt.Sprintf(metainfo.url, invalidID, captureID),
			nil,
		)
		router.ServeHTTP(w, req)
		respErr := model.HTTPError{}
		err := json.NewDecoder(w.Body).Decode(&respErr)
		require.Nil(t, err)
		require.Contains(t, respErr.Code, "ErrAPIInvalidParam")
		require.Equal(t, http.StatusBadRequest, w.Code)
	}

	// case 2: invalid capture id.
	{
		cp := mock_capture.NewMockCapture(gomock.NewController(t))
		cp.EXPECT().IsReady().Return(true).AnyTimes()
		cp.EXPECT().IsOwner().Return(true).AnyTimes()

		apiV2 := NewOpenAPIV2ForTest(cp, APIV2HelpersImpl{})
		router := newRouter(apiV2)
		metainfo := testCase{
			url:    "/api/v2/processors/%s/%s",
			method: "GET",
		}
		invalidID := "@^Invalid"
		w := httptest.NewRecorder()
		req, _ := http.NewRequestWithContext(
			context.Background(),
			metainfo.method,
			fmt.Sprintf(metainfo.url, changeFeedID.ID, invalidID),
			nil,
		)
		router.ServeHTTP(w, req)
		respErr := model.HTTPError{}
		err := json.NewDecoder(w.Body).Decode(&respErr)
		require.Nil(t, err)
		require.Contains(t, respErr.Code, "ErrAPIInvalidParam")
		require.Equal(t, http.StatusBadRequest, w.Code)
	}

	// case 3: abnormal changefeed state.
	{
		provider := &mockStatusProvider{
			changefeedInfo: &model.ChangeFeedInfo{
				State: model.StatePending,
			},
		}
		cp := mock_capture.NewMockCapture(gomock.NewController(t))
		cp.EXPECT().IsReady().Return(true).AnyTimes()
		cp.EXPECT().IsOwner().Return(true).AnyTimes()

		apiV2 := NewOpenAPIV2ForTest(cp, APIV2HelpersImpl{})
		router := newRouter(apiV2)
		metainfo := testCase{
			url:    "/api/v2/processors/%s/%s",
			method: "GET",
		}
		cp.EXPECT().StatusProvider().Return(provider).AnyTimes()

		w := httptest.NewRecorder()
		req, _ := http.NewRequestWithContext(
			context.Background(),
			metainfo.method,
			fmt.Sprintf(metainfo.url, changeFeedID.ID, captureID),
			nil,
		)
		router.ServeHTTP(w, req)
		respErr := model.HTTPError{}
		err := json.NewDecoder(w.Body).Decode(&respErr)
		require.Nil(t, err)
		require.Contains(t, respErr.Code, "ErrAPIInvalidParam")
		require.Contains(t, respErr.Error, "changefeed in abnormal state")
		require.Equal(t, http.StatusBadRequest, w.Code)
	}

	// case 4: captureID not exist.
	{
		provider := &mockStatusProvider{
			changefeedInfo: &model.ChangeFeedInfo{
				State: model.StateNormal,
			},
			processors: []*model.ProcInfoSnap{
				{
					CaptureID: "nonexist-capture",
				},
			},
		}
		cp := mock_capture.NewMockCapture(gomock.NewController(t))
		cp.EXPECT().StatusProvider().Return(provider).AnyTimes()
		cp.EXPECT().IsReady().Return(true).AnyTimes()
		cp.EXPECT().IsOwner().Return(true).AnyTimes()

		apiV2 := NewOpenAPIV2ForTest(cp, APIV2HelpersImpl{})
		router := newRouter(apiV2)
		metainfo := testCase{
			url:    "/api/v2/processors/%s/%s",
			method: "GET",
		}

		w := httptest.NewRecorder()
		req, _ := http.NewRequestWithContext(
			context.Background(),
			metainfo.method,
			fmt.Sprintf(metainfo.url, changeFeedID.ID, captureID),
			nil,
		)
		router.ServeHTTP(w, req)
		respErr := model.HTTPError{}
		err := json.NewDecoder(w.Body).Decode(&respErr)
		require.Nil(t, err)
		require.Contains(t, respErr.Code, "ErrCaptureNotExist")
		require.Equal(t, http.StatusBadRequest, w.Code)
	}

	// case 5: corresponding task status not found.
	{
		provider := &mockStatusProvider{
			changefeedInfo: &model.ChangeFeedInfo{
				State: model.StateNormal,
			},
			processors: []*model.ProcInfoSnap{
				{
					CaptureID: captureID,
				},
			},
			taskStatus: map[model.CaptureID]*model.TaskStatus{},
		}
		cp := mock_capture.NewMockCapture(gomock.NewController(t))
		cp.EXPECT().StatusProvider().Return(provider).AnyTimes()
		cp.EXPECT().IsReady().Return(true).AnyTimes()
		cp.EXPECT().IsOwner().Return(true).AnyTimes()

		apiV2 := NewOpenAPIV2ForTest(cp, APIV2HelpersImpl{})
		router := newRouter(apiV2)
		metainfo := testCase{
			url:    "/api/v2/processors/%s/%s",
			method: "GET",
		}
		w := httptest.NewRecorder()
		req, _ := http.NewRequestWithContext(
			context.Background(),
			metainfo.method,
			fmt.Sprintf(metainfo.url, changeFeedID.ID, captureID),
			nil,
		)

		router.ServeHTTP(w, req)
		resp := model.ProcessorDetail{}
		err := json.NewDecoder(w.Body).Decode(&resp)
		require.Nil(t, err)
		require.Equal(t, http.StatusOK, w.Code)
		// if task status not exist, return an empty processor detail
		require.Equal(t, model.ProcessorDetail{}, resp)
	}

	// case 6: success get the desired processor info.
	{
		provider := &mockStatusProvider{
			changefeedInfo: &model.ChangeFeedInfo{
				State: model.StateNormal,
			},
			processors: []*model.ProcInfoSnap{
				{
					CaptureID: captureID,
				},
			},
			taskStatus: map[model.CaptureID]*model.TaskStatus{
				model.CaptureID(captureID): {
					Tables: map[model.TableID]*model.TableReplicaInfo{
						model.TableID(0): {},
						model.TableID(1): {},
					},
				},
			},
		}
		cp := mock_capture.NewMockCapture(gomock.NewController(t))
		cp.EXPECT().StatusProvider().Return(provider).AnyTimes()
		cp.EXPECT().IsReady().Return(true).AnyTimes()
		cp.EXPECT().IsOwner().Return(true).AnyTimes()
		apiV2 := NewOpenAPIV2ForTest(cp, APIV2HelpersImpl{})
		router := newRouter(apiV2)

		metainfo := testCase{
			url:    "/api/v2/processors/%s/%s",
			method: "GET",
		}
		w := httptest.NewRecorder()
		req, _ := http.NewRequestWithContext(
			context.Background(),
			metainfo.method,
			fmt.Sprintf(metainfo.url, changeFeedID.ID, captureID),
			nil,
		)

		router.ServeHTTP(w, req)
		resp1 := model.ProcessorDetail{}
		err := json.NewDecoder(w.Body).Decode(&resp1)
		require.Nil(t, err)
		require.Equal(t, http.StatusOK, w.Code)
		require.Equal(t, 2, len(resp1.Tables))
	}
}
