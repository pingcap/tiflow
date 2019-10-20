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
	"context"
	"net/url"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/embed"
	"github.com/pingcap/check"
	"github.com/pingcap/tidb-cdc/pkg/etcd"
)

type schedulerSuite struct {
	etcd      *embed.Etcd
	clientURL *url.URL
}

var _ = check.Suite(&schedulerSuite{})

var (
	runSubChangeFeedCount int
)

// Set up a embed etcd using free ports.
func (s *schedulerSuite) SetUpTest(c *check.C) {
	dir := c.MkDir()
	curl, e, err := etcd.SetupEmbedEtcd(dir)
	c.Assert(err, check.IsNil)
	s.clientURL = curl
	s.etcd = e
	go func() {
		c.Log(<-e.Err())
	}()
}

func (s *schedulerSuite) TearDownTest(c *check.C) {
	s.etcd.Close()
}

func mockRunSubChangeFeed(ctx context.Context, pdEndpoints []string, detail ChangeFeedDetail) (chan error, error) {
	errCh := make(chan error, 1)
	runSubChangeFeedCount += 1
	return errCh, nil
}

func (s *schedulerSuite) TestSubChangeFeedWatcher(c *check.C) {
	var (
		changefeedID = "test-changefeed"
		captureID    = "test-capture"
		pdEndpoints  = []string{}
		detail       = ChangeFeedDetail{}
		key          = getEtcdKey(keySubChangeFeed, changefeedID, captureID)
	)

	oriRunSubChangeFeed := runSubChangeFeed
	runSubChangeFeed = mockRunSubChangeFeed
	defer func() {
		runSubChangeFeed = oriRunSubChangeFeed
	}()

	curl := s.clientURL.String()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{curl},
		DialTimeout: 3 * time.Second,
	})
	c.Assert(err, check.IsNil)
	defer cli.Close()

	_, err = cli.Put(context.Background(), key, "{}")
	c.Assert(err, check.IsNil)

	errCh := make(chan error, 1)
	sw := NewSubChangeFeedWatcher(changefeedID, captureID, pdEndpoints, cli, detail)
	go sw.Watch(context.Background(), errCh)
	time.Sleep(time.Millisecond * 500)
	c.Assert(runSubChangeFeedCount, check.Equals, 1)
}
