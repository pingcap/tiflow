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

package cdc

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	"github.com/pingcap/check"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"go.etcd.io/etcd/clientv3/concurrency"
)

type httpStatusSuite struct{}

var _ = check.Suite(&httpStatusSuite{})

const retryTime = 20

var testingServerOptions = options{
	pdEndpoints:   "http://127.0.0.1:2379",
	addr:          "127.0.0.1:8300",
	advertiseAddr: "127.0.0.1:8300",
	timezone:      nil,
	gcTTL:         DefaultCDCGCSafePointTTL,
}

func (s *httpStatusSuite) waitUntilServerOnline(c *check.C) {
	statusURL := fmt.Sprintf("http://%s/status", testingServerOptions.advertiseAddr)
	for i := 0; i < retryTime; i++ {
		resp, err := http.Get(statusURL)
		if err == nil {
			_, err := ioutil.ReadAll(resp.Body)
			c.Assert(err, check.IsNil)
			resp.Body.Close()
			return
		}
		time.Sleep(time.Millisecond * 50)
	}
	c.Errorf("failed to connect http status for %d retries in every 50ms", retryTime)
}

func (s *httpStatusSuite) TestHTTPStatus(c *check.C) {
	server := &Server{opts: testingServerOptions}
	err := server.startStatusHTTP()
	c.Assert(err, check.IsNil)
	defer func() {
		c.Assert(server.statusServer.Close(), check.IsNil)
	}()

	s.waitUntilServerOnline(c)

	testPprof(c)
	testReisgnOwner(c)
	testHandleChangefeedAdmin(c)
	testHandleRebalance(c)
	testHandleMoveTable(c)
	testHandleChangefeedQuery(c)
}

func testPprof(c *check.C) {
	resp, err := http.Get(fmt.Sprintf("http://%s/debug/pprof/cmdline", testingServerOptions.advertiseAddr))
	c.Assert(err, check.IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, check.Equals, 200)
	_, err = ioutil.ReadAll(resp.Body)
	c.Assert(err, check.IsNil)
}

func testReisgnOwner(c *check.C) {
	uri := fmt.Sprintf("http://%s/capture/owner/resign", testingServerOptions.advertiseAddr)
	testHTTPPostOnly(c, uri)
	testRequestNonOwnerFailed(c, uri)
}

func testHandleChangefeedAdmin(c *check.C) {
	uri := fmt.Sprintf("http://%s/capture/owner/admin", testingServerOptions.advertiseAddr)
	testHTTPPostOnly(c, uri)
	testRequestNonOwnerFailed(c, uri)
}

func testHandleRebalance(c *check.C) {
	uri := fmt.Sprintf("http://%s/capture/owner/rebalance_trigger", testingServerOptions.advertiseAddr)
	testHTTPPostOnly(c, uri)
	testRequestNonOwnerFailed(c, uri)
}

func testHandleMoveTable(c *check.C) {
	uri := fmt.Sprintf("http://%s/capture/owner/move_table", testingServerOptions.advertiseAddr)
	testHTTPPostOnly(c, uri)
	testRequestNonOwnerFailed(c, uri)
}

func testHandleChangefeedQuery(c *check.C) {
	uri := fmt.Sprintf("http://%s/capture/owner/changefeed/query", testingServerOptions.advertiseAddr)
	testHTTPPostOnly(c, uri)
	testRequestNonOwnerFailed(c, uri)
}

func testHTTPPostOnly(c *check.C, uri string) {
	resp, err := http.Get(uri)
	c.Assert(err, check.IsNil)
	data, err := ioutil.ReadAll(resp.Body)
	c.Assert(err, check.IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, check.Equals, http.StatusBadRequest)
	c.Assert(string(data), check.Equals, cerror.ErrSupportPostOnly.Error())
}

func testRequestNonOwnerFailed(c *check.C, uri string) {
	resp, err := http.PostForm(uri, url.Values{})
	c.Assert(err, check.IsNil)
	data, err := ioutil.ReadAll(resp.Body)
	c.Assert(err, check.IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, check.Equals, http.StatusBadRequest)
	c.Assert(string(data), check.Equals, concurrency.ErrElectionNotLeader.Error())
}
