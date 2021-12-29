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
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/httputil"
	"github.com/pingcap/tiflow/cdc/capture"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/retry"
	security2 "github.com/pingcap/tiflow/pkg/security"
	"github.com/pingcap/tiflow/pkg/util/testleak"
	"github.com/tikv/pd/pkg/tempurl"
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
			_, err := io.ReadAll(resp.Body)
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

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := server.tcpServer.Run(ctx)
		c.Check(err, check.ErrorMatches, ".*ErrTCPServerClosed.*")
	}()

	err = server.startStatusHTTP(server.tcpServer.HTTP1Listener())
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

	cancel()
	wg.Wait()
}

func testPprof(c *check.C) {
	testValidPprof := func(uri string) {
		resp, err := http.Get(uri)
		c.Assert(err, check.IsNil)
		defer resp.Body.Close()
		c.Assert(resp.StatusCode, check.Equals, 200)
		_, err = io.ReadAll(resp.Body)
		c.Assert(err, check.IsNil)
	}
	testValidPprof(fmt.Sprintf("http://%s/debug/pprof", advertiseAddr4Test))
	testValidPprof(fmt.Sprintf("http://%s/debug/pprof/cmdline", advertiseAddr4Test))
	testValidPprof(fmt.Sprintf("http://%s/debug/pprof/mutex", advertiseAddr4Test))
	testValidPprof(fmt.Sprintf("http://%s/debug/pprof/heap?debug=1", advertiseAddr4Test))
}

func testReisgnOwner(c *check.C) {
	uri := fmt.Sprintf("http://%s/capture/owner/resign", advertiseAddr4Test)
	testRequestNonOwnerFailed(c, uri)
}

func testHandleChangefeedAdmin(c *check.C) {
	uri := fmt.Sprintf("http://%s/capture/owner/admin", advertiseAddr4Test)
	testRequestNonOwnerFailed(c, uri)
}

func testHandleRebalance(c *check.C) {
	uri := fmt.Sprintf("http://%s/capture/owner/rebalance_trigger", advertiseAddr4Test)
	testRequestNonOwnerFailed(c, uri)
}

func testHandleMoveTable(c *check.C) {
	uri := fmt.Sprintf("http://%s/capture/owner/move_table", advertiseAddr4Test)
	testRequestNonOwnerFailed(c, uri)
}

func testHandleChangefeedQuery(c *check.C) {
	uri := fmt.Sprintf("http://%s/capture/owner/changefeed/query", advertiseAddr4Test)
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

func (s *httpStatusSuite) TestServerTLSWithoutCommonName(c *check.C) {
	defer testleak.AfterTest(c)
	addr := tempurl.Alloc()[len("http://"):]
	// Do not specify common name
	security, err := security2.NewCredential4Test("")
	c.Assert(err, check.IsNil)
	conf := config.GetDefaultServerConfig()
	conf.Addr = addr
	conf.AdvertiseAddr = addr
	conf.Security = &security
	config.StoreGlobalServerConfig(conf)

	server, err := NewServer([]string{"https://127.0.0.1:2379"})
	server.capture = capture.NewCapture4Test()
	c.Assert(err, check.IsNil)
	err = server.startStatusHTTP(server.tcpServer.HTTP1Listener())
	c.Assert(err, check.IsNil)
	defer func() {
		c.Assert(server.statusServer.Close(), check.IsNil)
	}()

	statusURL := fmt.Sprintf("https://%s/api/v1/status", addr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := server.tcpServer.Run(ctx)
		c.Check(err, check.ErrorMatches, ".*ErrTCPServerClosed.*")
	}()

	// test cli sends request without a cert will success
	err = retry.Do(ctx, func() error {
		tr := &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
		cli := &http.Client{Transport: tr}
		resp, err := cli.Get(statusURL)
		if err != nil {
			return err
		}
		decoder := json.NewDecoder(resp.Body)
		captureInfo := &model.CaptureInfo{}
		err = decoder.Decode(captureInfo)
		c.Assert(err, check.IsNil)
		c.Assert(captureInfo.ID, check.Equals, server.capture.Info().ID)
		resp.Body.Close()
		return nil
	}, retry.WithMaxTries(retryTime), retry.WithBackoffBaseDelay(50), retry.WithIsRetryableErr(cerrors.IsRetryableError))
	c.Assert(err, check.IsNil)

	// test cli sends request with a cert will success
	err = retry.Do(ctx, func() error {
		tlsConfig, err := security.ToTLSConfigWithVerify()
		if err != nil {
			c.Assert(err, check.IsNil)
		}
		cli := httputil.NewClient(tlsConfig)
		resp, err := cli.Get(statusURL)
		if err != nil {
			return err
		}
		decoder := json.NewDecoder(resp.Body)
		captureInfo := &model.CaptureInfo{}
		err = decoder.Decode(captureInfo)
		c.Assert(err, check.IsNil)
		c.Assert(captureInfo.ID, check.Equals, server.capture.Info().ID)
		resp.Body.Close()
		return nil
	}, retry.WithMaxTries(retryTime), retry.WithBackoffBaseDelay(50), retry.WithIsRetryableErr(cerrors.IsRetryableError))
	c.Assert(err, check.IsNil)

	cancel()
	wg.Wait()
}

//
func (s *httpStatusSuite) TestServerTLSWithCommonName(c *check.C) {
	defer testleak.AfterTest(c)
	addr := tempurl.Alloc()[len("http://"):]
	// specify a common name
	security, err := security2.NewCredential4Test("test")
	c.Assert(err, check.IsNil)
	conf := config.GetDefaultServerConfig()
	conf.Addr = addr
	conf.AdvertiseAddr = addr
	conf.Security = &security
	config.StoreGlobalServerConfig(conf)

	server, err := NewServer([]string{"https://127.0.0.1:2379"})
	server.capture = capture.NewCapture4Test()
	c.Assert(err, check.IsNil)
	err = server.startStatusHTTP(server.tcpServer.HTTP1Listener())
	c.Assert(err, check.IsNil)
	defer func() {
		c.Assert(server.statusServer.Close(), check.IsNil)
	}()

	statusURL := fmt.Sprintf("https://%s/api/v1/status", addr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := server.tcpServer.Run(ctx)
		c.Check(err, check.ErrorMatches, ".*ErrTCPServerClosed.*")
	}()

	// test cli sends request without a cert will fail
	err = retry.Do(ctx, func() error {
		tr := &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
		cli := &http.Client{Transport: tr}
		resp, err := cli.Get(statusURL)
		if err != nil {
			return err
		}
		decoder := json.NewDecoder(resp.Body)
		captureInfo := &model.CaptureInfo{}
		err = decoder.Decode(captureInfo)
		c.Assert(err, check.IsNil)
		c.Assert(captureInfo.ID, check.Equals, server.capture.Info().ID)
		resp.Body.Close()
		return nil
	}, retry.WithMaxTries(retryTime), retry.WithBackoffBaseDelay(50), retry.WithIsRetryableErr(cerrors.IsRetryableError))
	c.Assert(strings.Contains(err.Error(), "remote error: tls: bad certificate"), check.IsTrue)

	// test cli sends request with a cert will success
	err = retry.Do(ctx, func() error {
		tlsConfig, err := security.ToTLSConfigWithVerify()
		if err != nil {
			c.Assert(err, check.IsNil)
		}
		cli := httputil.NewClient(tlsConfig)
		resp, err := cli.Get(statusURL)
		if err != nil {
			return err
		}
		decoder := json.NewDecoder(resp.Body)
		captureInfo := &model.CaptureInfo{}
		err = decoder.Decode(captureInfo)
		c.Assert(err, check.IsNil)
		c.Assert(captureInfo.ID, check.Equals, server.capture.Info().ID)
		resp.Body.Close()
		return nil
	}, retry.WithMaxTries(retryTime), retry.WithBackoffBaseDelay(50), retry.WithIsRetryableErr(cerrors.IsRetryableError))
	c.Assert(err, check.IsNil)
}
