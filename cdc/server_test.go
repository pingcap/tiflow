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
	"context"
	"net/url"
	"path/filepath"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/pingcap/tiflow/pkg/util/testleak"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
	"golang.org/x/sync/errgroup"
)

type serverSuite struct {
	server    *Server
	e         *embed.Etcd
	clientURL *url.URL
	ctx       context.Context
	cancel    context.CancelFunc
	errg      *errgroup.Group
}

func (s *serverSuite) SetUpTest(c *check.C) {
	var err error
	dir := c.MkDir()
	s.clientURL, s.e, err = etcd.SetupEmbedEtcd(dir)
	c.Assert(err, check.IsNil)

	pdEndpoints := []string{
		"http://" + s.clientURL.Host,
		"http://invalid-pd-host:2379",
	}
	server, err := NewServer(pdEndpoints)
	c.Assert(err, check.IsNil)
	c.Assert(server, check.NotNil)
	s.server = server

	s.ctx, s.cancel = context.WithCancel(context.Background())
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   s.server.pdEndpoints,
		Context:     s.ctx,
		DialTimeout: 5 * time.Second,
	})
	c.Assert(err, check.IsNil)
	etcdClient := etcd.NewCDCEtcdClient(s.ctx, client)
	s.server.etcdClient = &etcdClient

	s.errg = util.HandleErrWithErrGroup(s.ctx, s.e.Err(), func(e error) { c.Log(e) })
}

func (s *serverSuite) TearDownTest(c *check.C) {
	s.server.Close()
	s.e.Close()
	s.cancel()
	err := s.errg.Wait()
	if err != nil {
		c.Errorf("Error group error: %s", err)
	}
}

var _ = check.Suite(&serverSuite{})

func (s *serverSuite) TestEtcdHealthChecker(c *check.C) {
	defer testleak.AfterTest(c)()
	defer s.TearDownTest(c)

	s.errg.Go(func() error {
		err := s.server.etcdHealthChecker(s.ctx)
		c.Assert(err, check.Equals, context.Canceled)
		return nil
	})
	// longer than one check tick 3s
	time.Sleep(time.Second * 4)
	s.cancel()
}

func (s *serverSuite) TestSetUpDataDir(c *check.C) {
	defer testleak.AfterTest(c)()
	defer s.TearDownTest(c)

	conf := config.GetGlobalServerConfig()
	// DataDir is not set, and no changefeed exist, use the default
	conf.DataDir = ""
	err := s.server.setUpDataDir(s.ctx)
	c.Assert(err, check.IsNil)
	c.Assert(conf.DataDir, check.Equals, defaultDataDir)
	c.Assert(conf.Sorter.SortDir, check.Equals, filepath.Join(defaultDataDir, config.DefaultSortDir))

	// DataDir is not set, but has existed changefeed, use the one with the largest available space
	conf.DataDir = ""
	dir := c.MkDir()
	err = s.server.etcdClient.SaveChangeFeedInfo(s.ctx, &model.ChangeFeedInfo{SortDir: dir}, "a")
	c.Assert(err, check.IsNil)

	err = s.server.etcdClient.SaveChangeFeedInfo(s.ctx, &model.ChangeFeedInfo{}, "b")
	c.Assert(err, check.IsNil)

	err = s.server.setUpDataDir(s.ctx)
	c.Assert(err, check.IsNil)

	c.Assert(conf.DataDir, check.Equals, dir)
	c.Assert(conf.Sorter.SortDir, check.Equals, filepath.Join(dir, config.DefaultSortDir))

	conf.DataDir = c.MkDir()
	// DataDir has been set, just use it
	err = s.server.setUpDataDir(s.ctx)
	c.Assert(err, check.IsNil)
	c.Assert(conf.DataDir, check.Not(check.Equals), "")
	c.Assert(conf.Sorter.SortDir, check.Equals, filepath.Join(conf.DataDir, config.DefaultSortDir))

<<<<<<< HEAD
	s.cancel()
=======
	server, err := NewServer([]string{"https://127.0.0.1:2379"})
	server.capture = capture.NewCapture4Test(nil)
	require.Nil(t, err)
	err = server.startStatusHTTP(server.tcpServer.HTTP1Listener())
	require.Nil(t, err)
	defer func() {
		require.Nil(t, server.statusServer.Close())
	}()

	statusURL := fmt.Sprintf("https://%s/api/v1/status", addr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := server.tcpServer.Run(ctx)
		require.Contains(t, err.Error(), "ErrTCPServerClosed")
	}()

	// test cli sends request without a cert will success
	err = retry.Do(ctx, func() error {
		tr := &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
		cli := httputil.NewTestClient(tr)
		resp, err := cli.Get(ctx, statusURL)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		decoder := json.NewDecoder(resp.Body)
		captureInfo := &model.CaptureInfo{}
		err = decoder.Decode(captureInfo)
		require.Nil(t, err)
		info, err := server.capture.Info()
		require.Nil(t, err)
		require.Equal(t, info.ID, captureInfo.ID)
		return nil
	}, retry.WithMaxTries(retryTime), retry.WithBackoffBaseDelay(50), retry.WithIsRetryableErr(cerrors.IsRetryableError))
	require.Nil(t, err)

	// test cli sends request with a cert will success
	err = retry.Do(ctx, func() error {
		cli, err := httputil.NewClient(&security)
		require.Nil(t, err)
		resp, err := cli.Get(ctx, statusURL)
		if err != nil {
			return err
		}
		decoder := json.NewDecoder(resp.Body)
		captureInfo := &model.CaptureInfo{}
		err = decoder.Decode(captureInfo)
		require.Nil(t, err)
		info, err := server.capture.Info()
		require.Nil(t, err)
		require.Equal(t, info.ID, captureInfo.ID)
		resp.Body.Close()
		return nil
	}, retry.WithMaxTries(retryTime), retry.WithBackoffBaseDelay(50), retry.WithIsRetryableErr(cerrors.IsRetryableError))
	require.Nil(t, err)

	cancel()
	wg.Wait()
}

func TestServerTLSWithCommonName(t *testing.T) {
	addr := tempurl.Alloc()[len("http://"):]
	// specify a common name
	security, err := security2.NewCredential4Test("test")
	require.Nil(t, err)
	conf := config.GetDefaultServerConfig()
	conf.Addr = addr
	conf.AdvertiseAddr = addr
	conf.Security = &security
	config.StoreGlobalServerConfig(conf)

	server, err := NewServer([]string{"https://127.0.0.1:2379"})
	server.capture = capture.NewCapture4Test(nil)
	require.Nil(t, err)
	err = server.startStatusHTTP(server.tcpServer.HTTP1Listener())
	require.Nil(t, err)
	defer func() {
		require.Nil(t, server.statusServer.Close())
	}()

	statusURL := fmt.Sprintf("https://%s/api/v1/status", addr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := server.tcpServer.Run(ctx)
		require.Contains(t, err.Error(), "ErrTCPServerClosed")
	}()

	// test cli sends request without a cert will fail
	err = retry.Do(ctx, func() error {
		tr := &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
		cli := httputil.NewTestClient(tr)
		require.Nil(t, err)
		resp, err := cli.Get(ctx, statusURL)
		if err != nil {
			return err
		}
		decoder := json.NewDecoder(resp.Body)
		captureInfo := &model.CaptureInfo{}
		err = decoder.Decode(captureInfo)
		require.Nil(t, err)
		info, err := server.capture.Info()
		require.Nil(t, err)
		require.Equal(t, info.ID, captureInfo.ID)
		resp.Body.Close()
		return nil
	}, retry.WithMaxTries(retryTime), retry.WithBackoffBaseDelay(50), retry.WithIsRetryableErr(cerrors.IsRetryableError))
	require.Contains(t, err.Error(), "remote error: tls: bad certificate")

	// test cli sends request with a cert will success
	err = retry.Do(ctx, func() error {
		cli, err := httputil.NewClient(&security)
		require.Nil(t, err)
		resp, err := cli.Get(ctx, statusURL)
		if err != nil {
			return err
		}
		decoder := json.NewDecoder(resp.Body)
		captureInfo := &model.CaptureInfo{}
		err = decoder.Decode(captureInfo)
		require.Nil(t, err)
		info, err := server.capture.Info()
		require.Nil(t, err)
		require.Equal(t, info.ID, captureInfo.ID)
		resp.Body.Close()
		return nil
	}, retry.WithMaxTries(retryTime), retry.WithBackoffBaseDelay(50), retry.WithIsRetryableErr(cerrors.IsRetryableError))
	require.Nil(t, err)
>>>>>>> c4a5146fa (capture (ticdc): fix http status panic (#5666))
}
