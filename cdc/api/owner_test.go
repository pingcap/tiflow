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

	"github.com/gin-gonic/gin"
	"github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tiflow/cdc/capture"
	"github.com/pingcap/tiflow/pkg/util/testleak"
	"go.etcd.io/etcd/clientv3/concurrency"
)

type httpStatusSuite struct{}

var _ = check.Suite(&httpStatusSuite{})

func (s *httpStatusSuite) TestHTTPStatus(c *check.C) {
	defer testleak.AfterTest(c)()

	router := gin.New()
	RegisterRoutes(router, capture.NewCapture4Test(), nil)

	ts := httptest.NewServer(router)
	defer ts.Close()

	addr := ts.URL
	testPprof(c, addr)
	testReisgnOwner(c, addr)
	testHandleChangefeedAdmin(c, addr)
	testHandleRebalance(c, addr)
	testHandleMoveTable(c, addr)
	testHandleChangefeedQuery(c, addr)
	testHandleFailpoint(c, addr)
}

func testPprof(c *check.C, addr string) {
	testValidPprof := func(uri string) {
		resp, err := http.Get(uri)
		c.Assert(err, check.IsNil)
		defer resp.Body.Close()
		c.Assert(resp.StatusCode, check.Equals, 200)
		_, err = io.ReadAll(resp.Body)
		c.Assert(err, check.IsNil)
	}
	testValidPprof(fmt.Sprintf("%s/debug/pprof", addr))
	testValidPprof(fmt.Sprintf("%s/debug/pprof/cmdline", addr))
	testValidPprof(fmt.Sprintf("%s/debug/pprof/mutex", addr))
	testValidPprof(fmt.Sprintf("%s/debug/pprof/heap?debug=1", addr))
}

func testReisgnOwner(c *check.C, addr string) {
	uri := fmt.Sprintf("%s/capture/owner/resign", addr)
	testRequestNonOwnerFailed(c, uri)
}

func testHandleChangefeedAdmin(c *check.C, addr string) {
	uri := fmt.Sprintf("%s/capture/owner/admin", addr)
	testRequestNonOwnerFailed(c, uri)
}

func testHandleRebalance(c *check.C, addr string) {
	uri := fmt.Sprintf("%s/capture/owner/rebalance_trigger", addr)
	testRequestNonOwnerFailed(c, uri)
}

func testHandleMoveTable(c *check.C, addr string) {
	uri := fmt.Sprintf("%s/capture/owner/move_table", addr)
	testRequestNonOwnerFailed(c, uri)
}

func testHandleChangefeedQuery(c *check.C, addr string) {
	uri := fmt.Sprintf("%s/capture/owner/changefeed/query", addr)
	testRequestNonOwnerFailed(c, uri)
}

func testRequestNonOwnerFailed(c *check.C, uri string) {
	resp, err := http.PostForm(uri, url.Values{})
	c.Assert(err, check.IsNil)
	data, err := io.ReadAll(resp.Body)
	c.Assert(err, check.IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, check.Equals, http.StatusBadRequest)
	c.Assert(string(data), check.Equals, concurrency.ErrElectionNotLeader.Error())
}

func testHandleFailpoint(c *check.C, addr string) {
	fp := "github.com/pingcap/tiflow/cdc/TestHandleFailpoint"
	uri := fmt.Sprintf("%s/debug/fail/%s", addr, fp)
	body := bytes.NewReader([]byte("return(true)"))
	req, err := http.NewRequest("PUT", uri, body)
	c.Assert(err, check.IsNil)

	resp, err := http.DefaultClient.Do(req)
	c.Assert(err, check.IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, check.GreaterEqual, 200)
	c.Assert(resp.StatusCode, check.Less, 300)

	failpointHit := false
	failpoint.Inject("TestHandleFailpoint", func() {
		failpointHit = true
	})
	c.Assert(failpointHit, check.IsTrue)

	req, err = http.NewRequest("DELETE", uri, body)
	c.Assert(err, check.IsNil)
	resp, err = http.DefaultClient.Do(req)
	c.Assert(err, check.IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, check.GreaterEqual, 200)
	c.Assert(resp.StatusCode, check.Less, 300)

	failpointHit = false
	failpoint.Inject("TestHandleFailpoint", func() {
		failpointHit = true
	})
	c.Assert(failpointHit, check.IsFalse)
}
