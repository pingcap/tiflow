// Copyright 2019 PingCAP, Inc.
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
	"time"

	"github.com/pingcap/check"
)

type httpStatusSuite struct{}

var _ = check.Suite(&httpStatusSuite{})

const retryTime = 20

func (s *httpStatusSuite) waitUntilServerOnline(c *check.C) {
	statusURL := fmt.Sprintf("http://%s:%d/status", defaultServerOptions.statusHost, defaultServerOptions.statusPort)
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
	server := &Server{opts: defaultServerOptions}
	server.startStatusHTTP()
	defer func() {
		c.Assert(server.statusServer.Close(), check.IsNil)
	}()

	s.waitUntilServerOnline(c)

	testPprof(c)
	testReisgnOwner(c)
}

func testPprof(c *check.C) {
	resp, err := http.Get(fmt.Sprintf("http://%s:%d/debug/pprof/cmdline", defaultServerOptions.statusHost, defaultServerOptions.statusPort))
	c.Assert(err, check.IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, check.Equals, 200)
	_, err = ioutil.ReadAll(resp.Body)
	c.Assert(err, check.IsNil)
}

func testReisgnOwner(c *check.C) {
	uri := fmt.Sprintf("http://%s:%d/capture/owner/resign", defaultServerOptions.statusHost, defaultServerOptions.statusPort)
	resp, err := http.Get(uri)
	c.Assert(err, check.IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, check.Equals, http.StatusBadRequest)
}
