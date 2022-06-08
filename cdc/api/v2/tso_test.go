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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/tiflow/cdc/capture"
	mock_owner "github.com/pingcap/tiflow/cdc/owner/mock"
	"github.com/pingcap/tiflow/pkg/upstream"
	"github.com/stretchr/testify/require"
)

func TestGetTso(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mockOwner := mock_owner.NewMockOwner(ctrl)
	mockPDClient := &MockPDClient{}
	mockManager := upstream.NewManager4Test(mockPDClient)
	cp := capture.NewCaptureWithManager4Test(mockOwner, mockManager)

	router := newRouter(cp)
	w := httptest.NewRecorder()

	tc := testCase{url: "/api/v2/tso", method: "GET"}
	req, err := http.NewRequestWithContext(context.Background(), tc.method, tc.url, nil)
	require.Nil(t, err)
	router.ServeHTTP(w, req)
	require.Equal(t, 200, w.Code)
}
