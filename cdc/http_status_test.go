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
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/util/testleak"
	"go.etcd.io/etcd/clientv3/concurrency"
)

type httpStatusSuite struct{}

var _ = check.Suite(&httpStatusSuite{})

const retryTime = 20

var advertiseAddr4Test = "127.0.0.1:8300"

func (s *httpStatusSuite) waitUntilServerOnline(c *check.C) {
	statusURL := fmt.Sprintf("http://%s/status", advertiseAddr4Test)
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
	defer testleak.AfterTest(c)()
	conf := config.GetDefaultServerConfig()
	conf.Addr = advertiseAddr4Test
	conf.AdvertiseAddr = advertiseAddr4Test
	config.StoreGlobalServerConfig(conf)
	server, err := NewServer([]string{"http://127.0.0.1:2379"})
	c.Assert(err, check.IsNil)
	err = server.startStatusHTTP()
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
	testHandleFailpoint(c)
}

func testPprof(c *check.C) {
	resp, err := http.Get(fmt.Sprintf("http://%s/debug/pprof/cmdline", advertiseAddr4Test))
	c.Assert(err, check.IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, check.Equals, 200)
	_, err = ioutil.ReadAll(resp.Body)
	c.Assert(err, check.IsNil)
}

func testReisgnOwner(c *check.C) {
	uri := fmt.Sprintf("http://%s/capture/owner/resign", advertiseAddr4Test)
	testHTTPPostOnly(c, uri)
	testRequestNonOwnerFailed(c, uri)
}

func testHandleChangefeedAdmin(c *check.C) {
	uri := fmt.Sprintf("http://%s/capture/owner/admin", advertiseAddr4Test)
	testHTTPPostOnly(c, uri)
	testRequestNonOwnerFailed(c, uri)
}

func testHandleRebalance(c *check.C) {
	uri := fmt.Sprintf("http://%s/capture/owner/rebalance_trigger", advertiseAddr4Test)
	testHTTPPostOnly(c, uri)
	testRequestNonOwnerFailed(c, uri)
}

func testHandleMoveTable(c *check.C) {
	uri := fmt.Sprintf("http://%s/capture/owner/move_table", advertiseAddr4Test)
	testHTTPPostOnly(c, uri)
	testRequestNonOwnerFailed(c, uri)
}

func testHandleChangefeedQuery(c *check.C) {
	uri := fmt.Sprintf("http://%s/capture/owner/changefeed/query", advertiseAddr4Test)
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

func testHandleFailpoint(c *check.C) {
	fp := "github.com/pingcap/tiflow/cdc/TestHandleFailpoint"
	uri := fmt.Sprintf("http://%s/debug/fail/%s", advertiseAddr4Test, fp)
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
