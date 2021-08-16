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
	"sync"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/config"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/ticdc/pkg/util/testleak"
	pd "github.com/tikv/pd/client"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.etcd.io/etcd/embed"
	"golang.org/x/sync/errgroup"
)

type captureSuite struct {
	e         *embed.Etcd
	clientURL *url.URL
	client    kv.CDCEtcdClient
	ctx       context.Context
	cancel    context.CancelFunc
	errg      *errgroup.Group
}

var _ = check.Suite(&captureSuite{})

func (s *captureSuite) SetUpTest(c *check.C) {
	dir := c.MkDir()
	var err error
	s.clientURL, s.e, err = etcd.SetupEmbedEtcd(dir)
	c.Assert(err, check.IsNil)
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{s.clientURL.String()},
		DialTimeout: 3 * time.Second,
	})
	c.Assert(err, check.IsNil)
	s.client = kv.NewCDCEtcdClient(context.Background(), client)
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.errg = util.HandleErrWithErrGroup(s.ctx, s.e.Err(), func(e error) { c.Log(e) })
}

func (s *captureSuite) TearDownTest(c *check.C) {
	s.e.Close()
	s.cancel()
	err := s.errg.Wait()
	if err != nil {
		c.Errorf("Error group error: %s", err)
	}
	s.client.Close() //nolint:errcheck
}

func (s *captureSuite) TestCaptureSuicide(c *check.C) {
	defer testleak.AfterTest(c)()
	defer s.TearDownTest(c)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	capture, err := NewCapture(ctx, []string{s.clientURL.String()}, nil, nil)
	c.Assert(err, check.IsNil)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := capture.Run(ctx)
		c.Assert(cerror.ErrCaptureSuicide.Equal(err), check.IsTrue)
	}()
	// ttl is 5s, wait 1s to ensure `capture.Run` starts
	time.Sleep(time.Second)
	_, err = s.client.Client.Revoke(ctx, capture.session.Lease())
	c.Assert(err, check.IsNil)
	wg.Wait()

	err = capture.etcdClient.Close()
	if err != nil {
		c.Assert(errors.Cause(err), check.Equals, context.Canceled)
	}
}

func (s *captureSuite) TestCaptureSessionDoneDuringHandleTask(c *check.C) {
	defer testleak.AfterTest(c)()
	defer s.TearDownTest(c)
	if config.NewReplicaImpl {
		c.Skip("this case is designed for old processor")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	capture, err := NewCapture(ctx, []string{s.clientURL.String()}, nil, nil)
	c.Assert(err, check.IsNil)

	runProcessorCount := 0
	err = failpoint.Enable("github.com/pingcap/ticdc/cdc/captureHandleTaskDelay", "sleep(500)")
	c.Assert(err, check.IsNil)
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/ticdc/cdc/captureHandleTaskDelay")
	}()
	runProcessorBackup := runProcessorImpl
	runProcessorImpl = func(
		ctx context.Context, _ pd.Client, grpcPool kv.GrpcPool,
		session *concurrency.Session, info model.ChangeFeedInfo, changefeedID string,
		captureInfo model.CaptureInfo, checkpointTs uint64, flushCheckpointInterval time.Duration,
	) (*oldProcessor, error) {
		runProcessorCount++
		etcdCli := kv.NewCDCEtcdClient(ctx, session.Client())
		_, _, err := etcdCli.GetTaskStatus(ctx, changefeedID, captureInfo.ID)
		return nil, err
	}
	defer func() {
		runProcessorImpl = runProcessorBackup
	}()

	// The test simulates the following procedure
	// 1. owner: dispatches new task to a capture
	// 2. capture: detects the task, and starts to handle task
	// 3. capture: during the task handling, capture session is disconnected
	// 4. owner: observes the capture session disconnected and cleanup the task status of this capture
	// 5. capture: queries task status failed when handling task
	// 6. capture: checks session ttl, finds session disconnected and returns ErrCaptureSuicide to restart itself
	// the event sequence must be kept, especially for 2->3->4->5
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := capture.Run(ctx)
		// check step-6
		c.Assert(cerror.ErrCaptureSuicide.Equal(err), check.IsTrue)
		// check step-5 runs
		c.Assert(runProcessorCount, check.Equals, 1)
	}()
	changefeedID := "test-changefeed"
	err = s.client.SaveChangeFeedInfo(ctx, &model.ChangeFeedInfo{Config: config.GetDefaultReplicaConfig()}, changefeedID)
	c.Assert(err, check.IsNil)
	// step-1
	err = s.client.PutTaskStatus(ctx, changefeedID, capture.info.ID, &model.TaskStatus{})
	c.Assert(err, check.IsNil)
	// sleep 100ms to ensure step-2 happens, the failpoint injected delay will ensure step-4 is after step-3
	time.Sleep(time.Millisecond * 100)

	// step-3
	_, err = s.client.Client.Revoke(ctx, capture.session.Lease())
	c.Assert(err, check.IsNil)
	err = s.client.DeleteTaskStatus(ctx, changefeedID, capture.info.ID)
	c.Assert(err, check.IsNil)

	wg.Wait()

	err = capture.etcdClient.Close()
	if err != nil {
		c.Assert(errors.Cause(err), check.Equals, context.Canceled)
	}
}
