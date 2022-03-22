//  Copyright 2022 PingCAP, Inc.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  See the License for the specific language governing permissions and
//  limitations under the License.

package etcd

import (
	"context"
	"sync/atomic"

	"github.com/pingcap/errors"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type MockClient struct {
	clientv3.KV
	getOK bool
}

func (m *MockClient) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (resp *clientv3.GetResponse, err error) {
	if m.getOK {
		m.getOK = true
		return nil, errors.New("mock error")
	}
	return &clientv3.GetResponse{}, nil
}

func (m *MockClient) Put(ctx context.Context, key, val string, opts ...clientv3.OpOption) (resp *clientv3.PutResponse, err error) {
	return nil, errors.New("mock error")
}

func (m *MockClient) Txn(ctx context.Context) clientv3.Txn {
	return &mockTxn{ctx: ctx}
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
