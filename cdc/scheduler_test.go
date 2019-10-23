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
	"sync"
	"sync/atomic"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/embed"
	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-cdc/pkg/etcd"
	"github.com/pingcap/tidb-cdc/pkg/util"
)

type schedulerSuite struct {
	etcd      *embed.Etcd
	clientURL *url.URL
}

var _ = check.Suite(&schedulerSuite{})

var (
	runSubChangeFeedCount     int32
	runChangeFeedWatcherCount int32
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
	atomic.AddInt32(&runSubChangeFeedCount, 1)
	return errCh, nil
}

func mockRunSubChangeFeedError(ctx context.Context, pdEndpoints []string, detail ChangeFeedDetail) (chan error, error) {
	errCh := make(chan error, 1)
	defer func() {
		errCh <- errors.New("mock run error")
	}()
	return errCh, nil
}

func mockRunSubChangeFeedWatcher(
	tx context.Context,
	changefeedID string,
	captureID string,
	pdEndpoints []string,
	etcdCli *clientv3.Client,
	detail ChangeFeedDetail,
	errCh chan error,
) *SubChangeFeedWatcher {
	atomic.AddInt32(&runChangeFeedWatcherCount, 1)
	return nil
}

func (s *schedulerSuite) TestSubChangeFeedWatcher(c *check.C) {
	var (
		changefeedID = "test-changefeed"
		captureID    = "test-capture"
		pdEndpoints  = []string{}
		detail       = ChangeFeedDetail{}
		key          = getEtcdKeySubChangeFeed(changefeedID, captureID)
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

	// create a subchangefeed
	_, err = cli.Put(context.Background(), key, "{}")
	c.Assert(err, check.IsNil)

	// subchangefeed exists before watch starts
	errCh := make(chan error, 1)
	sw := runSubChangeFeedWatcher(context.Background(), changefeedID, captureID, pdEndpoints, cli, detail, errCh)
	c.Assert(util.WaitSomething(10, time.Millisecond*50, func() bool {
		return atomic.LoadInt32(&runSubChangeFeedCount) == 1
	}), check.IsTrue)

	// delete the subchangefeed
	_, err = cli.Delete(context.Background(), key)
	c.Assert(err, check.IsNil)
	time.Sleep(time.Second)
	sw.close()
	c.Assert(sw.isClosed(), check.IsTrue)

	// check SubChangeFeedWatcher watch subchangefeed key can ben canceled
	err = sw.reopen()
	c.Assert(err, check.IsNil)
	c.Assert(sw.isClosed(), check.IsFalse)
	ctx, cancel := context.WithCancel(context.Background())
	sw.wg.Add(1)
	go sw.Watch(ctx, errCh)
	cancel()
	sw.close()
	c.Assert(sw.isClosed(), check.IsTrue)

	// check watcher can find new subchangefeed in watch loop
	errCh2 := make(chan error, 1)
	runSubChangeFeedWatcher(context.Background(), changefeedID, captureID, pdEndpoints, cli, detail, errCh2)
	_, err = cli.Put(context.Background(), key, "{}")
	c.Assert(err, check.IsNil)
	c.Assert(util.WaitSomething(10, time.Millisecond*50, func() bool {
		return atomic.LoadInt32(&runSubChangeFeedCount) == 2
	}), check.IsTrue)
}

func (s *schedulerSuite) TestSubChangeFeedWatcherError(c *check.C) {
	var (
		changefeedID = "test-changefeed-err"
		captureID    = "test-capture-err"
		pdEndpoints  = []string{}
		detail       = ChangeFeedDetail{}
		key          = getEtcdKeySubChangeFeed(changefeedID, captureID)
	)

	oriRunSubChangeFeed := runSubChangeFeed
	runSubChangeFeed = mockRunSubChangeFeedError
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

	// create a subchangefeed
	_, err = cli.Put(context.Background(), key, "{}")
	c.Assert(err, check.IsNil)

	errCh := make(chan error, 1)
	sw := runSubChangeFeedWatcher(context.Background(), changefeedID, captureID, pdEndpoints, cli, detail, errCh)
	c.Assert(util.WaitSomething(10, time.Millisecond*50, func() bool {
		return len(errCh) == 1
	}), check.IsTrue)
	sw.close()
	c.Assert(sw.isClosed(), check.IsTrue)
}

func (s *schedulerSuite) TestChangeFeedWatcher(c *check.C) {
	var (
		changefeedID = "test-changefeed-watcher"
		captureID    = "test-capture"
		pdEndpoints  = []string{}
		sinkURI      = "root@tcp(127.0.0.1:3306)/test"
		detail       = ChangeFeedDetail{SinkURI: sinkURI}
		key          = getEtcdKeyChangeFeed(changefeedID)
	)

	oriRunSubChangeFeedWatcher := runSubChangeFeedWatcher
	runSubChangeFeedWatcher = mockRunSubChangeFeedWatcher
	defer func() {
		runSubChangeFeedWatcher = oriRunSubChangeFeedWatcher
	}()

	curl := s.clientURL.String()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{curl},
		DialTimeout: 3 * time.Second,
	})
	c.Assert(err, check.IsNil)
	defer cli.Close()

	ctx, cancel := context.WithCancel(context.Background())
	w := NewChangeFeedWatcher(captureID, pdEndpoints, cli)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err2 := w.Watch(ctx)
		if err2 != nil && errors.Cause(err2) != context.Canceled {
			c.Fatal(err2)
		}
	}()

	// short wait to ensure ChangeFeedWatcher has started watch loop
	// TODO: test watch key apperance with revision works as expected
	time.Sleep(time.Millisecond * 100)

	// create a changefeed
	err = detail.SaveChangeFeedDetail(context.Background(), cli, changefeedID)
	c.Assert(err, check.IsNil)
	c.Assert(util.WaitSomething(10, time.Millisecond*50, func() bool {
		return atomic.LoadInt32(&runChangeFeedWatcherCount) == 1
	}), check.IsTrue)
	w.lock.RLock()
	c.Assert(len(w.details), check.Equals, 1)
	w.lock.RUnlock()

	// delete the changefeed
	_, err = cli.Delete(context.Background(), key)
	c.Assert(err, check.IsNil)
	c.Assert(util.WaitSomething(10, time.Millisecond*50, func() bool {
		w.lock.RLock()
		defer w.lock.RUnlock()
		return len(w.details) == 0
	}), check.IsTrue)

	cancel()
	wg.Wait()
}
