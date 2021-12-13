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
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/ticdc/pkg/util/testleak"
	"github.com/stretchr/testify/require"
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
	err := s.server.setUpDir(s.ctx)
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

	err = s.server.setUpDir(s.ctx)
	c.Assert(err, check.IsNil)

	c.Assert(conf.DataDir, check.Equals, dir)
	c.Assert(conf.Sorter.SortDir, check.Equals, filepath.Join(dir, config.DefaultSortDir))

	conf.DataDir = c.MkDir()
	// DataDir has been set, just use it
	err = s.server.setUpDir(s.ctx)
	c.Assert(err, check.IsNil)
	c.Assert(conf.DataDir, check.Not(check.Equals), "")
	c.Assert(conf.Sorter.SortDir, check.Equals, filepath.Join(conf.DataDir, config.DefaultSortDir))

	s.cancel()
}

func TestCheckDir(t *testing.T) {
	t.Parallel()
	me, err := user.Current()
	require.Nil(t, err)
	if me.Name == "root" || runtime.GOOS == "windows" {
		t.Skip("test case is running as a superuser or in windows")
	}

	dir := t.TempDir()
	_, err = checkDir(dir)
	require.Nil(t, err)

	readOnly := filepath.Join(dir, "readOnly")
	err = os.MkdirAll(readOnly, 0o400)
	require.Nil(t, err)
	_, err = checkDir(readOnly)
	require.Error(t, err)

	writeOnly := filepath.Join(dir, "writeOnly")
	err = os.MkdirAll(writeOnly, 0o200)
	require.Nil(t, err)
	_, err = checkDir(writeOnly)
	require.Error(t, err)

	file := filepath.Join(dir, "file")
	f, err := os.Create(file)
	require.Nil(t, err)
	require.Nil(t, f.Close())
	_, err = checkDir(file)
	require.Error(t, err)
}
