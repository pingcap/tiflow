// Copyright 2022 PingCAP, Inc.
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

package metadata

import (
	"context"
	"strings"
	"sync"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var (
	_ MetaKV       = &MetaMock{}
	_ clientv3.Txn = &Txn{}
)

type Txn struct {
	m   *MetaMock
	ops []clientv3.Op
}

func (t *Txn) If(cs ...clientv3.Cmp) clientv3.Txn {
	panic("unimplemented")
}

func (t *Txn) Else(cs ...clientv3.Op) clientv3.Txn {
	panic("unimplemented")
}

func (t *Txn) Then(ops ...clientv3.Op) clientv3.Txn {
	t.ops = append(t.ops, ops...)
	return t
}

func (t *Txn) Commit() (*clientv3.TxnResponse, error) {
	var err error
	for _, op := range t.ops {
		switch {
		case op.IsDelete():
			_, err = t.m.Delete(context.Background(), string(op.KeyBytes()))
		case op.IsPut():
			_, err = t.m.Put(context.Background(), string(op.KeyBytes()), string(op.ValueBytes()))
		default:
		}
		if err != nil {
			return nil, err
		}
	}
	return nil, nil
}

type MetaMock struct {
	sync.Mutex
	store    map[string]string
	revision int64
}

func NewMetaMock() *MetaMock {
	return &MetaMock{
		store: make(map[string]string),
	}
}

func (m *MetaMock) Delete(ctx context.Context, key string, opts ...interface{}) (interface{}, error) {
	m.Lock()
	defer m.Unlock()
	delete(m.store, key)
	m.revision++
	return nil, nil
}

func (m *MetaMock) Watch(ctx context.Context, key string, opts ...interface{}) interface{} {
	panic("unimplemented")
}

func (m *MetaMock) Put(ctx context.Context, key, value string, opts ...interface{}) (interface{}, error) {
	m.Lock()
	defer m.Unlock()
	m.store[key] = value
	m.revision++
	return nil, nil
}

func (m *MetaMock) Get(ctx context.Context, key string, opts ...interface{}) (interface{}, error) {
	m.Lock()
	defer m.Unlock()
	ret := &clientv3.GetResponse{
		Header: &etcdserverpb.ResponseHeader{
			Revision: m.revision,
		},
	}
	for k, v := range m.store {
		if !strings.HasPrefix(k, key) {
			continue
		}
		ret.Kvs = append(ret.Kvs, &mvccpb.KeyValue{
			Key:   []byte(k),
			Value: []byte(v),
		})
	}
	m.revision++
	return ret, nil
}

func (m *MetaMock) Txn(ctx context.Context) interface{} {
	return &Txn{
		m: m,
	}
}
