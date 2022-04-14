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
	"sync/atomic"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/pkg/util/testleak"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/etcdserver/api/v3rpc/rpctypes"
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

func (m *mockClient) Txn(ctx context.Context) clientv3.Txn {
	return &mockTxn{ctx: ctx}
}

type mockWatcher struct {
	clientv3.Watcher
	watchCh      chan clientv3.WatchResponse
	resetCount   *int32
	requestCount *int32
	rev          *int64
}

func (m mockWatcher) Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
	atomic.AddInt32(m.resetCount, 1)
	op := &clientv3.Op{}
	for _, opt := range opts {
		opt(op)
	}
	atomic.StoreInt64(m.rev, op.Rev())
	return m.watchCh
}

func (m mockWatcher) RequestProgress(ctx context.Context) error {
	atomic.AddInt32(m.requestCount, 1)
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Test Txn case
	// case 0: normal
	rsp, err := retrycli.Txn(ctx, nil, nil, nil)
	c.Assert(err, check.IsNil)
	c.Assert(rsp.Succeeded, check.IsFalse)

	// case 1: errors.ErrReachMaxTry
	_, err = retrycli.Txn(ctx, TxnEmptyCmps, nil, nil)
	c.Assert(err, check.ErrorMatches, ".*CDC:ErrReachMaxTry.*")

	// case 2: errors.ErrReachMaxTry
	_, err = retrycli.Txn(ctx, nil, TxnEmptyOpsThen, nil)
	c.Assert(err, check.ErrorMatches, ".*CDC:ErrReachMaxTry.*")

	// case 3: context.DeadlineExceeded
	_, err = retrycli.Txn(ctx, TxnEmptyCmps, TxnEmptyOpsThen, nil)
	c.Assert(err, check.Equals, context.DeadlineExceeded)

	// other case: mock error
	_, err = retrycli.Txn(ctx, TxnEmptyCmps, TxnEmptyOpsThen, TxnEmptyOpsElse)
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

// test no data lost when WatchCh blocked
func (s *etcdSuite) TestWatchChBlocked(c *check.C) {
	defer testleak.AfterTest(c)()
	defer s.TearDownTest(c)
	cli := clientv3.NewCtxClient(context.TODO())
	resetCount := int32(0)
	requestCount := int32(0)
	rev := int64(0)
	watchCh := make(chan clientv3.WatchResponse, 1)
	watcher := mockWatcher{watchCh: watchCh, resetCount: &resetCount, requestCount: &requestCount, rev: &rev}
	cli.Watcher = watcher

	sentRes := []clientv3.WatchResponse{
		{CompactRevision: 1},
		{CompactRevision: 2},
		{CompactRevision: 3},
		{CompactRevision: 4},
		{CompactRevision: 5},
		{CompactRevision: 6},
	}

	go func() {
		for _, r := range sentRes {
			watchCh <- r
		}
	}()

	mockClock := clock.NewMock()
	watchCli := Wrap(cli, nil)
	watchCli.clock = mockClock

	key := "testWatchChBlocked"
	outCh := make(chan clientv3.WatchResponse, 6)
	revision := int64(1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	go func() {
		watchCli.WatchWithChan(ctx, outCh, key, "", clientv3.WithPrefix(), clientv3.WithRev(revision))
	}()
	receivedRes := make([]clientv3.WatchResponse, 0)
	// wait for WatchWithChan set up
	r := <-outCh
	receivedRes = append(receivedRes, r)
	// move time forward
	mockClock.Add(time.Second * 30)

	for r := range outCh {
		receivedRes = append(receivedRes, r)
	}

	c.Check(sentRes, check.DeepEquals, receivedRes)
	// make sure watchCh has been reset since timeout
	c.Assert(atomic.LoadInt32(watcher.resetCount) > 1, check.IsTrue)
	// make sure RequestProgress has been call since timeout
	c.Assert(atomic.LoadInt32(watcher.requestCount) > 1, check.IsTrue)
	// make sure etcdRequestProgressDuration is less than etcdWatchChTimeoutDuration
	c.Assert(etcdRequestProgressDuration, check.Less, etcdWatchChTimeoutDuration)
}

// test no data lost when OutCh blocked
func (s *etcdSuite) TestOutChBlocked(c *check.C) {
	defer testleak.AfterTest(c)()
	defer s.TearDownTest(c)

	cli := clientv3.NewCtxClient(context.TODO())
	resetCount := int32(0)
	requestCount := int32(0)
	rev := int64(0)
	watchCh := make(chan clientv3.WatchResponse, 1)
	watcher := mockWatcher{watchCh: watchCh, resetCount: &resetCount, requestCount: &requestCount, rev: &rev}
	cli.Watcher = watcher

	mockClock := clock.NewMock()
	watchCli := Wrap(cli, nil)
	watchCli.clock = mockClock

	sentRes := []clientv3.WatchResponse{
		{CompactRevision: 1},
		{CompactRevision: 2},
		{CompactRevision: 3},
	}

	go func() {
		for _, r := range sentRes {
			watchCh <- r
		}
	}()

	key := "testOutChBlocked"
	outCh := make(chan clientv3.WatchResponse, 1)
	revision := int64(1)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()
	go func() {
		watchCli.WatchWithChan(ctx, outCh, key, "", clientv3.WithPrefix(), clientv3.WithRev(revision))
	}()
	receivedRes := make([]clientv3.WatchResponse, 0)
	// wait for WatchWithChan set up
	r := <-outCh
	receivedRes = append(receivedRes, r)
	// move time forward
	mockClock.Add(time.Second * 30)

	for r := range outCh {
		receivedRes = append(receivedRes, r)
	}

	c.Check(sentRes, check.DeepEquals, receivedRes)
}

func (s *clientSuite) TestRevisionNotFallBack(c *check.C) {
	defer testleak.AfterTest(c)()
	cli := clientv3.NewCtxClient(context.TODO())

	resetCount := int32(0)
	requestCount := int32(0)
	rev := int64(0)
	watchCh := make(chan clientv3.WatchResponse, 1)
	watcher := mockWatcher{watchCh: watchCh, resetCount: &resetCount, requestCount: &requestCount, rev: &rev}
	cli.Watcher = watcher
	mockClock := clock.NewMock()
	watchCli := Wrap(cli, nil)
	watchCli.clock = mockClock

	key := "testRevisionNotFallBack"
	outCh := make(chan clientv3.WatchResponse, 1)
	// watch from revision = 2
	revision := int64(2)

	sentRes := []clientv3.WatchResponse{
		{CompactRevision: 1},
	}

	go func() {
		for _, r := range sentRes {
			watchCh <- r
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()
	go func() {
		watchCli.WatchWithChan(ctx, outCh, key, "test", clientv3.WithPrefix(), clientv3.WithRev(revision))
	}()
	// wait for WatchWithChan set up
	<-outCh
	// move time forward
	mockClock.Add(time.Second * 30)
	// make sure watchCh has been reset since timeout
	c.Assert(atomic.LoadInt32(watcher.resetCount) > 1, check.IsTrue)
	// make sure revision in WatchWitchChan does not fall back
	// even if there has not any response been received from WatchCh
	// while WatchCh was reset
	c.Assert(atomic.LoadInt64(watcher.rev), check.Equals, revision)
}

type mockTxn struct {
	ctx  context.Context
	mode int
}

func (txn *mockTxn) If(cs ...clientv3.Cmp) clientv3.Txn {
	if cs != nil {
		txn.mode += 1
	}
	return txn
}

func (txn *mockTxn) Then(ops ...clientv3.Op) clientv3.Txn {
	if ops != nil {
		txn.mode += 1 << 1
	}
	return txn
}

func (txn *mockTxn) Else(ops ...clientv3.Op) clientv3.Txn {
	if ops != nil {
		txn.mode += 1 << 2
	}
	return txn
}

func (txn *mockTxn) Commit() (*clientv3.TxnResponse, error) {
	switch txn.mode {
	case 0:
		return &clientv3.TxnResponse{}, nil
	case 1:
		return nil, rpctypes.ErrNoSpace
	case 2:
		return nil, rpctypes.ErrTimeoutDueToLeaderFail
	case 3:
		return nil, context.DeadlineExceeded
	default:
		return nil, errors.New("mock error")
	}
}
