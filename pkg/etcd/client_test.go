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

package etcd

import (
	"context"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/pkg/util/testleak"
	"go.etcd.io/etcd/clientv3"
)

type clientSuite struct{}

var _ = check.Suite(&clientSuite{})

type mockClient struct {
	clientv3.KV
	getOK bool
}

func (m *mockClient) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (resp *clientv3.GetResponse, err error) {
	if m.getOK {
		m.getOK = true
		return nil, errors.New("mock error")
	}
	return &clientv3.GetResponse{}, nil
}

func (m *mockClient) Put(ctx context.Context, key, val string, opts ...clientv3.OpOption) (resp *clientv3.PutResponse, err error) {
	return nil, errors.New("mock error")
}

type mockWatcher struct {
	clientv3.Watcher
	resetCount   *int
	requestCount *int
}

func (m mockWatcher) Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
	watchCh := make(clientv3.WatchChan, 0)
	*m.resetCount++
	return watchCh
}

func (m mockWatcher) RequestProgress(ctx context.Context) error {
	*m.requestCount++
	return nil
}

func (s *clientSuite) TestRetry(c *check.C) {
	defer testleak.AfterTest(c)()
	originValue := maxTries
	// to speedup the test
	maxTries = 2

	cli := clientv3.NewCtxClient(context.TODO())
	cli.KV = &mockClient{}
	retrycli := Wrap(cli, nil)
	get, err := retrycli.Get(context.TODO(), "")
	c.Assert(err, check.IsNil)
	c.Assert(get, check.NotNil)

	_, err = retrycli.Put(context.TODO(), "", "")
	c.Assert(err, check.NotNil)
	c.Assert(errors.Cause(err), check.ErrorMatches, "mock error", check.Commentf("err:%v", err.Error()))
	maxTries = originValue
}

func (s *etcdSuite) TestDelegateLease(c *check.C) {
	defer testleak.AfterTest(c)()
	defer s.TearDownTest(c)
	ctx := context.Background()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{s.clientURL.String()},
		DialTimeout: 3 * time.Second,
	})
	c.Assert(err, check.IsNil)
	defer cli.Close()

	ttl := int64(10)
	lease, err := cli.Grant(ctx, ttl)
	c.Assert(err, check.IsNil)

	ttlResp, err := cli.TimeToLive(ctx, lease.ID)
	c.Assert(err, check.IsNil)
	c.Assert(ttlResp.GrantedTTL, check.Equals, ttl)
	c.Assert(ttlResp.TTL, check.Less, ttl)
	c.Assert(ttlResp.TTL, check.Greater, int64(0))

	_, err = cli.Revoke(ctx, lease.ID)
	c.Assert(err, check.IsNil)
	ttlResp, err = cli.TimeToLive(ctx, lease.ID)
	c.Assert(err, check.IsNil)
	c.Assert(ttlResp.TTL, check.Equals, int64(-1))
}

func (s *etcdSuite) TestWatchWithChan(c *check.C) {
	defer testleak.AfterTest(c)()
	defer s.TearDownTest(c)

	cli := clientv3.NewCtxClient(context.TODO())
	watcher := mockWatcher{}
	resetCount := 0
	requestCuount := 0
	watcher.resetCount = &resetCount
	watcher.requestCount = &requestCuount
	cli.Watcher = watcher

	mockClock := clock.NewMock()
	watchCli := Wrap(cli, nil)
	watchCli.clock = mockClock

	key := "watch-test"
	watchCh := make(chan clientv3.WatchResponse, 1)
	revision := int64(1)
	ctx, _ := context.WithTimeout(context.Background(), time.Second*2)

	closeCh := make(chan struct{})
	go func() {
		watchCli.WatchWithChan(ctx, watchCh, key, clientv3.WithPrefix(), clientv3.WithRev(revision))
		closeCh <- struct{}{}
	}()
	// wait for WatchWithChan set up
	time.Sleep(1 * time.Second)
	// move time forward
	mockClock.Add(time.Second * 20)
	<-closeCh

	// make sure watchCh has been reset since timeout
	c.Assert(*watcher.resetCount > 1, check.IsTrue)
	// make sure RequestProgress has been call since timeout
	c.Assert(*watcher.requestCount > 1, check.IsTrue)
	// make sure etcdRequestProgressDuration is less than etcdWatchChTimeoutDuration
	c.Assert(etcdRequestProgressDuration, check.Less, etcdWatchChTimeoutDuration)
}
