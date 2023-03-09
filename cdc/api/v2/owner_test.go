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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/golang/mock/gomock"
	mock_capture "github.com/pingcap/tiflow/cdc/capture/mock"
	mock_owner "github.com/pingcap/tiflow/cdc/owner/mock"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestResignOwner(t *testing.T) {
	t.Parallel()
	// case 1: get owner successfully.
	{
		ctrl := gomock.NewController(t)
		cp := mock_capture.NewMockCapture(ctrl)
		cp.EXPECT().IsReady().Return(true).AnyTimes()
		cp.EXPECT().IsOwner().Return(true).AnyTimes()
		mo := mock_owner.NewMockOwner(ctrl)
		cp.EXPECT().GetOwner().Return(mo, nil)
		mo.EXPECT().AsyncStop()
		apiV2 := NewOpenAPIV2ForTest(cp, APIV2HelpersImpl{})
		router := newRouter(apiV2)
		w := httptest.NewRecorder()
		req, _ := http.NewRequestWithContext(
			context.Background(),
			"POST",
			"/api/v2/owner/resign",
			nil,
		)
		router.ServeHTTP(w, req)
		require.Equal(t, "{}", w.Body.String())
		require.Equal(t, http.StatusOK, w.Code)
	}

	// case 2: get owner failed
	{
		ctrl := gomock.NewController(t)
		cp := mock_capture.NewMockCapture(ctrl)
		cp.EXPECT().IsReady().Return(true).AnyTimes()
		cp.EXPECT().IsOwner().Return(true).AnyTimes()
		cp.EXPECT().GetOwner().Return(nil, errors.New("fake"))
		apiV2 := NewOpenAPIV2ForTest(cp, APIV2HelpersImpl{})
		router := newRouter(apiV2)
		w := httptest.NewRecorder()
		req, _ := http.NewRequestWithContext(
			context.Background(),
			"POST",
			"/api/v2/owner/resign",
			nil,
		)
		router.ServeHTTP(w, req)
		require.Equal(t, "{}", w.Body.String())
		require.Equal(t, http.StatusOK, w.Code)
	}
}
