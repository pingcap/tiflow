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

package pdutil

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
)

type mockPDClient struct {
	pd.Client
	testServer *httptest.Server
	url        string
}

func (m *mockPDClient) GetLeaderAddr() string {
	return m.url
}

func newMockPDClient(ctx context.Context, normal bool) *mockPDClient {
	mock := &mockPDClient{}
	status := http.StatusOK
	if !normal {
		status = http.StatusNotFound
	}
	mock.testServer = httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(status)
		},
	))
	mock.url = mock.testServer.URL

	return mock
}

func TestMetaLabelNormal(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mockClient := newMockPDClient(ctx, true)

	err := UpdateMetaLabel(ctx, mockClient)
	require.Nil(t, err)
	mockClient.testServer.Close()
}

func TestMetaLabelFail(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mockClient := newMockPDClient(ctx, false)
	pc, err := newPDApiClient(ctx, mockClient)
	require.Nil(t, err)
	mockClient.url = "http://127.0.1.1:2345"

	// test url error
	err = pc.patchMetaLabel(ctx)
	require.NotNil(t, err)

	// test 404
	mockClient.url = mockClient.testServer.URL
	err = pc.patchMetaLabel(ctx)
	require.Regexp(t, ".*404.*", err)

	err = UpdateMetaLabel(ctx, mockClient)
	require.ErrorIs(t, err, cerror.ErrReachMaxTry)
	mockClient.testServer.Close()
}
