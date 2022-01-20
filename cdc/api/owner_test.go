// Copyright 2020 PingCAP, Inc.
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

package api

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/clientv3/concurrency"
)

func TestHTTPStatus(t *testing.T) {
	router := gin.New()
	RegisterOwnerAPIRoutes(router, nil)

	ts := httptest.NewServer(router)
	defer ts.Close()

	addr := ts.URL
	testReisgnOwner(t, addr)
	testHandleChangefeedAdmin(t, addr)
	testHandleRebalance(t, addr)
	testHandleMoveTable(t, addr)
	testHandleChangefeedQuery(t, addr)
	testHandleFailpoint(t, addr)
}

func testReisgnOwner(t *testing.T, addr string) {
	uri := fmt.Sprintf("%s/capture/owner/resign", addr)
	testRequestNonOwnerFailed(t, uri)
}

func testHandleChangefeedAdmin(t *testing.T, addr string) {
	uri := fmt.Sprintf("%s/capture/owner/admin", addr)
	testRequestNonOwnerFailed(t, uri)
}

func testHandleRebalance(t *testing.T, addr string) {
	uri := fmt.Sprintf("%s/capture/owner/rebalance_trigger", addr)
	testRequestNonOwnerFailed(t, uri)
}

func testHandleMoveTable(t *testing.T, addr string) {
	uri := fmt.Sprintf("%s/capture/owner/move_table", addr)
	testRequestNonOwnerFailed(t, uri)
}

func testHandleChangefeedQuery(t *testing.T, addr string) {
	uri := fmt.Sprintf("%s/capture/owner/changefeed/query", addr)
	testRequestNonOwnerFailed(t, uri)
}

func testRequestNonOwnerFailed(t *testing.T, uri string) {
	resp, err := http.PostForm(uri, url.Values{})
	require.Nil(t, err)
	data, err := io.ReadAll(resp.Body)
	require.Nil(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.Equal(t, concurrency.ErrElectionNotLeader.Error(), string(data))
}

func testHandleFailpoint(t *testing.T, addr string) {
	fp := "github.com/pingcap/tiflow/cdc/TestHandleFailpoint"
	uri := fmt.Sprintf("%s/debug/fail/%s", addr, fp)
	body := bytes.NewReader([]byte("return(true)"))
	req, err := http.NewRequest("PUT", uri, body)
	require.Nil(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.Nil(t, err)
	defer resp.Body.Close()
	require.GreaterOrEqual(t, resp.StatusCode, 200)
	require.Less(t, resp.StatusCode, 300)

	failpointHit := false
	failpoint.Inject("TestHandleFailpoint", func() {
		failpointHit = true
	})
	require.True(t, failpointHit)

	req, err = http.NewRequest("DELETE", uri, body)
	require.Nil(t, err)
	resp, err = http.DefaultClient.Do(req)
	require.Nil(t, err)
	defer resp.Body.Close()
	require.GreaterOrEqual(t, resp.StatusCode, 200)
	require.Less(t, resp.StatusCode, 300)

	failpointHit = false
	failpoint.Inject("TestHandleFailpoint", func() {
		failpointHit = true
	})
	require.False(t, failpointHit)
}
