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
	"github.com/tikv/client-go/v2/oracle"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/tiflow/cdc/capture"
	mock_owner "github.com/pingcap/tiflow/cdc/owner/mock"
	"github.com/pingcap/tiflow/pkg/upstream"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
)

// MockPDClient mocks pd.Client to facilitate unit testing.
type mockPDClient struct {
	pd.Client
}

func (m *mockPDClient) GetTS(_ context.Context) (int64, int64, error) {
	return oracle.GetPhysical(time.Now()), 0, nil
}

func TestGetTso(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mockOwner := mock_owner.NewMockOwner(ctrl)
	mockPDClient := &MockPDClient{}
	mockManager := upstream.NewManager4Test(mockPDClient)
	cp := capture.NewCaptureWithManager4Test(mockOwner, mockManager)

	apiV2 := NewOpenAPIV2ForTest(cp, &mockStubs{testUpstreamManager: mockManager})
	router := newRouter(apiV2)
	w := httptest.NewRecorder()

	tc := testCase{url: "/api/v2/tso", method: "POST"}
	buf := &bytes.Buffer{}
	buf.WriteString("{}")
	req, err := http.NewRequestWithContext(context.Background(),
		tc.method, tc.url, buf)
	require.Nil(t, err)
	router.ServeHTTP(w, req)
	require.Equal(t, 200, w.Code)
}
