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
	"fmt"
	"net/http"
	"net/url"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/ticdc/pkg/util/testleak"
	"github.com/pingcap/ticdc/pkg/version"
	"github.com/tikv/pd/pkg/tempurl"
	"go.etcd.io/etcd/embed"
	"golang.org/x/sync/errgroup"
)

type serverSuite struct {
	e         *embed.Etcd
	clientURL *url.URL
	ctx       context.Context
	cancel    context.CancelFunc
	errg      *errgroup.Group
}

func (s *serverSuite) SetUpTest(c *check.C) {
	dir := c.MkDir()
	var err error
	s.clientURL, s.e, err = etcd.SetupEmbedEtcd(dir)
	c.Assert(err, check.IsNil)
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.errg = util.HandleErrWithErrGroup(s.ctx, s.e.Err(), func(e error) { c.Log(e) })
}

func (s *serverSuite) TearDownTest(c *check.C) {
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

	ctx, cancel := context.WithCancel(context.Background())
	pdEndpoints := []string{
		"http://" + s.clientURL.Host,
		"http://invalid-pd-host:2379",
	}
	server, err := NewServer(pdEndpoints)
	c.Assert(err, check.IsNil)
	c.Assert(server, check.NotNil)

	s.errg.Go(func() error {
		err := server.etcdHealthChecker(ctx)
		c.Assert(err, check.Equals, context.Canceled)
		return nil
	})
	// longer than one check tick 3s
	time.Sleep(time.Second * 4)
	cancel()
}

func (s *serverSuite) TestInitDataDir(c *check.C) {
	defer testleak.AfterTest(c)()
	defer s.TearDownTest(c)

	ctx, cancel := context.WithCancel(context.Background())
	pdEndpoints := []string{
		"http://" + s.clientURL.Host,
		"http://invalid-pd-host:2379",
	}
	server, err := NewServer(pdEndpoints)
	c.Assert(err, check.IsNil)
	c.Assert(server, check.NotNil)

	conf := config.GetGlobalServerConfig()
	conf.DataDir = c.MkDir()

	err = server.initDataDir(ctx)
	c.Assert(err, check.IsNil)
	c.Assert(conf.DataDir, check.Not(check.Equals), "")
	c.Assert(conf.Sorter.SortDir, check.Equals, filepath.Join(conf.DataDir, "/tmp/sorter"))
	config.StoreGlobalServerConfig(conf)

	server.etcdClient = nil
	conf.DataDir = ""
	err = server.initDataDir(ctx)
	c.Assert(err, check.IsNil)
	c.Assert(conf.DataDir, check.Not(check.Equals), "")

	cancel()
}

type checkSuite struct{}

var _ = check.Suite(&checkSuite{})

func (s *checkSuite) TestCheckAndWaitClusterVersion(c *check.C) {
	defer testleak.AfterTest(c)()
	mock := version.MockPDClient{
		Client: nil,
	}
	pdURL, _ := url.Parse(tempurl.Alloc())
	pdHTTP := fmt.Sprintf("http://%s", pdURL.Host)
	srv := http.Server{Addr: pdURL.Host, Handler: &mock}
	go func() {
		_ = srv.ListenAndServe()
	}()
	defer srv.Close()

	{
		mock.GetVersionFunc = func() string {
			return version.MinPDVersion.String()
		}
		mock.GetAllStoresFunc = func() []*metapb.Store {
			return []*metapb.Store{{Version: version.MinTiKVVersion.String()}}
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			checkAndWaitClusterVersion(ctx, &mock, []string{pdHTTP}, nil)
			done <- struct{}{}
		}()
		select {
		case <-time.After(5 * time.Second):
			c.Fatal("check timeout")
		case <-done:
		}
		cancel()
	}

	{
		pdVersionSwitch := int64(0)
		mock.GetVersionFunc = func() string {
			if atomic.LoadInt64(&pdVersionSwitch) == 0 {
				const incompatiblePD = "0.0.1"
				return incompatiblePD
			}
			return version.MinPDVersion.String()
		}
		mock.GetAllStoresFunc = func() []*metapb.Store {
			return []*metapb.Store{{Version: version.MinTiKVVersion.String()}}
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			checkAndWaitClusterVersion(ctx, &mock, []string{pdHTTP}, nil)
			done <- struct{}{}
		}()
		select {
		case <-time.After(2 * time.Second):
		case <-done:
			c.Fatal("must timeout")
		}
		atomic.StoreInt64(&pdVersionSwitch, 1)
		select {
		case <-time.After(2 * time.Second):
			c.Fatal("check timeout")
		case <-done:
		}
		cancel()
	}
}
