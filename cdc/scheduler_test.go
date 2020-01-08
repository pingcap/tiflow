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
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/util"
	"golang.org/x/sync/errgroup"
)

type schedulerSuite struct {
	etcd      *embed.Etcd
	clientURL *url.URL
	ctx       context.Context
	cancel    context.CancelFunc
	errg      *errgroup.Group
}

var _ = check.Suite(&schedulerSuite{})

var (
	runProcessorCount         int32
	runChangeFeedWatcherCount int32
	errRunProcessor           = errors.New("mock run processor error")
)

// Set up a embed etcd using free ports.
func (s *schedulerSuite) SetUpTest(c *check.C) {
	dir := c.MkDir()
	var err error
	s.clientURL, s.etcd, err = etcd.SetupEmbedEtcd(dir)
	c.Assert(err, check.IsNil)
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.errg = util.HandleErrWithErrGroup(s.ctx, s.etcd.Err(), func(e error) { c.Log(e) })
}

func (s *schedulerSuite) TearDownTest(c *check.C) {
	s.etcd.Close()
	s.cancel()
	err := s.errg.Wait()
	if err != nil {
		c.Errorf("Error group error: %s", err)
	}
}

func mockRunProcessor(
	ctx context.Context,
	pdEndpoints []string,
	detail model.ChangeFeedDetail,
	changefeedID string,
	captureID string,
	_ processorCallback,
) error {
	atomic.AddInt32(&runProcessorCount, 1)
	return nil
}

func mockRunProcessorError(
	ctx context.Context,
	pdEndpoints []string,
	detail model.ChangeFeedDetail,
	changefeedID string,
	captureID string,
	_ processorCallback,
) error {
	return errRunProcessor
}

func mockRunProcessorWatcher(
	tx context.Context,
	changefeedID string,
	captureID string,
	pdEndpoints []string,
	etcdCli *clientv3.Client,
	detail model.ChangeFeedDetail,
	errCh chan error,
	_ processorCallback,
) *ProcessorWatcher {
	atomic.AddInt32(&runChangeFeedWatcherCount, 1)
	return nil
}

func (s *schedulerSuite) TestProcessorWatcher(c *check.C) {
	var (
		changefeedID = "test-changefeed"
		captureID    = "test-capture"
		pdEndpoints  = []string{}
		detail       = model.ChangeFeedDetail{}
		key          = kv.GetEtcdKeyTask(changefeedID, captureID)
	)

	oriRunProcessor := runProcessor
	runProcessor = mockRunProcessor
	defer func() {
		runProcessor = oriRunProcessor
	}()

	curl := s.clientURL.String()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{curl},
		DialTimeout: 3 * time.Second,
	})
	c.Assert(err, check.IsNil)
	defer cli.Close()

	// create a processor
	_, err = cli.Put(context.Background(), key, "{}")
	c.Assert(err, check.IsNil)

	// processor exists before watch starts
	errCh := make(chan error, 1)
	sw := runProcessorWatcher(context.Background(), changefeedID, captureID, pdEndpoints, cli, detail, errCh, nil)
	c.Assert(util.WaitSomething(10, time.Millisecond*50, func() bool {
		return atomic.LoadInt32(&runProcessorCount) == 1
	}), check.IsTrue)

	// delete the processor
	_, err = cli.Delete(context.Background(), key)
	c.Assert(err, check.IsNil)
	time.Sleep(time.Second)
	sw.close()
	c.Assert(sw.isClosed(), check.IsTrue)

	// check ProcessorWatcher watch processor key can ben canceled
	err = sw.reopen()
	c.Assert(err, check.IsNil)
	c.Assert(sw.isClosed(), check.IsFalse)
	ctx, cancel := context.WithCancel(context.Background())
	sw.wg.Add(1)
	go sw.Watch(ctx, errCh, nil)
	cancel()
	sw.close()
	c.Assert(sw.isClosed(), check.IsTrue)

	// check watcher can find new processor in watch loop
	errCh2 := make(chan error, 1)
	runProcessorWatcher(context.Background(), changefeedID, captureID, pdEndpoints, cli, detail, errCh2, nil)
	_, err = cli.Put(context.Background(), key, "{}")
	c.Assert(err, check.IsNil)
	c.Assert(util.WaitSomething(10, time.Millisecond*50, func() bool {
		return atomic.LoadInt32(&runProcessorCount) == 2
	}), check.IsTrue)
}

func (s *schedulerSuite) TestProcessorWatcherError(c *check.C) {
	var (
		changefeedID = "test-changefeed-err"
		captureID    = "test-capture-err"
		pdEndpoints  = []string{}
		detail       = model.ChangeFeedDetail{}
		key          = kv.GetEtcdKeyTask(changefeedID, captureID)
	)

	oriRunProcessor := runProcessor
	runProcessor = mockRunProcessorError
	defer func() {
		runProcessor = oriRunProcessor
	}()

	curl := s.clientURL.String()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{curl},
		DialTimeout: 3 * time.Second,
	})
	c.Assert(err, check.IsNil)
	defer cli.Close()

	// create a processor
	_, err = cli.Put(context.Background(), key, "{}")
	c.Assert(err, check.IsNil)

	errCh := make(chan error, 1)
	sw := runProcessorWatcher(context.Background(), changefeedID, captureID, pdEndpoints, cli, detail, errCh, nil)
	sw.wg.Add(1)
	go sw.Watch(context.Background(), errCh, nil)

	c.Assert(util.WaitSomething(10, time.Millisecond*50, func() bool {
		select {
		case err := <-errCh:
			return errors.Cause(err) == errRunProcessor
		default:
			return false
		}
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
		detail       = &model.ChangeFeedDetail{SinkURI: sinkURI}
		key          = kv.GetEtcdKeyChangeFeedConfig(changefeedID)
	)

	oriRunProcessorWatcher := runProcessorWatcher
	runProcessorWatcher = mockRunProcessorWatcher
	defer func() {
		runProcessorWatcher = oriRunProcessorWatcher
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
		err2 := w.Watch(ctx, nil)
		if err2 != nil && errors.Cause(err2) != context.Canceled {
			c.Fatal(err2)
		}
	}()

	// short wait to ensure ChangeFeedWatcher has started watch loop
	// TODO: test watch key apperance with revision works as expected
	time.Sleep(time.Millisecond * 100)

	// create a changefeed
	err = kv.SaveChangeFeedDetail(context.Background(), cli, detail, changefeedID)
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

	// create a changefeed
	err = kv.SaveChangeFeedDetail(context.Background(), cli, detail, changefeedID)
	c.Assert(err, check.IsNil)
	c.Assert(util.WaitSomething(10, time.Millisecond*50, func() bool {
		return atomic.LoadInt32(&runChangeFeedWatcherCount) == 2
	}), check.IsTrue)
	w.lock.RLock()
	c.Assert(len(w.details), check.Equals, 1)
	w.lock.RUnlock()

	// dispatch a stop changefeed admin job
	detail.AdminJobType = model.AdminStop
	err = kv.SaveChangeFeedDetail(context.Background(), cli, detail, changefeedID)
	c.Assert(err, check.IsNil)
	c.Assert(util.WaitSomething(10, time.Millisecond*50, func() bool {
		w.lock.RLock()
		defer w.lock.RUnlock()
		return len(w.details) == 0
	}), check.IsTrue)

	cancel()
	wg.Wait()
}
