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

<<<<<<< HEAD
func (s *clientSuite) TestRetry(c *check.C) {
	defer testleak.AfterTest(c)()
=======
func (m *mockClient) Txn(ctx context.Context) clientv3.Txn {
	return &mockTxn{ctx: ctx}
}

type mockWatcher struct {
	clientv3.Watcher
	watchCh      chan clientv3.WatchResponse
	resetCount   *int
	requestCount *int
	rev          *int64
}

func (m mockWatcher) Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
	*m.resetCount++
	op := &clientv3.Op{}
	for _, opt := range opts {
		opt(op)
	}
	*m.rev = op.Rev()
	return m.watchCh
}

func (m mockWatcher) RequestProgress(ctx context.Context) error {
	*m.requestCount++
	return nil
}

func TestRetry(t *testing.T) {
>>>>>>> 8dce39fdf (etcd/client(ticdc): add retry operation for etcd transaction api (#4248) (#4474))
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
<<<<<<< HEAD
	c.Assert(err, check.NotNil)
	c.Assert(errors.Cause(err), check.ErrorMatches, "mock error", check.Commentf("err:%v", err.Error()))
=======
	require.NotNil(t, err)
	require.Containsf(t, errors.Cause(err).Error(), "mock error", "err:%v", err.Error())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Test Txn case
	// case 0: normal
	rsp, err := retrycli.Txn(ctx, nil, nil, nil)
	require.Nil(t, err)
	require.False(t, rsp.Succeeded)

	// case 1: errors.ErrReachMaxTry
	_, err = retrycli.Txn(ctx, TxnEmptyCmps, nil, nil)
	require.Regexp(t, ".*CDC:ErrReachMaxTry.*", err)

	// case 2: errors.ErrReachMaxTry
	_, err = retrycli.Txn(ctx, nil, TxnEmptyOpsThen, nil)
	require.Regexp(t, ".*CDC:ErrReachMaxTry.*", err)

	// case 3: context.DeadlineExceeded
	_, err = retrycli.Txn(ctx, TxnEmptyCmps, TxnEmptyOpsThen, nil)
	require.Equal(t, context.DeadlineExceeded, err)

	// other case: mock error
	_, err = retrycli.Txn(ctx, TxnEmptyCmps, TxnEmptyOpsThen, TxnEmptyOpsElse)
	require.Containsf(t, errors.Cause(err).Error(), "mock error", "err:%v", err.Error())

>>>>>>> 8dce39fdf (etcd/client(ticdc): add retry operation for etcd transaction api (#4248) (#4474))
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
