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

package mock

import (
	"context"
	"fmt"
	"strings"
	"sync"

	metaModel "github.com/pingcap/tiflow/engine/pkg/meta/model"
	"github.com/pingcap/tiflow/pkg/errors"
)

// simple_mockclient is a simple mock kvclient using map to simulate db backend
// IT DOESN'T support options, like 'key-range'、'key-prefix'

type mockTxn struct {
	c   context.Context
	m   *MetaMock
	ops []metaModel.Op
}

func (t *mockTxn) Do(ops ...metaModel.Op) metaModel.Txn {
	t.ops = append(t.ops, ops...)
	return t
}

func (t *mockTxn) Commit() (*metaModel.TxnResponse, metaModel.Error) {
	txnRsp := &metaModel.TxnResponse{
		Header: &metaModel.ResponseHeader{
			ClusterID: "mock_cluster",
		},
		Responses: make([]metaModel.ResponseOp, 0, len(t.ops)),
	}

	// we lock the MetaMock to simulate the SERIALIZABLE isolation
	t.m.Lock()
	defer t.m.Unlock()

	for _, op := range t.ops {
		rsp, err := t.m.doNoLock(t.c, op)
		if err != nil {
			return nil, err
		}
		switch {
		case op.IsGet():
			txnRsp.Responses = append(txnRsp.Responses, metaModel.ResponseOp{
				Response: &metaModel.ResponseOpResponseGet{
					ResponseGet: rsp.Get(),
				},
			})
		case op.IsPut():
			txnRsp.Responses = append(txnRsp.Responses, metaModel.ResponseOp{
				Response: &metaModel.ResponseOpResponsePut{
					ResponsePut: rsp.Put(),
				},
			})
		case op.IsDelete():
			txnRsp.Responses = append(txnRsp.Responses, metaModel.ResponseOp{
				Response: &metaModel.ResponseOpResponseDelete{
					ResponseDelete: rsp.Del(),
				},
			})
		default:
			return nil, &mockError{
				caused: errors.ErrMetaOptionInvalid.Wrap(fmt.Errorf("unrecognized op type:%d", op.T)),
			}
		}
	}

	return txnRsp, nil
}

// MetaMock uses a simple in memory kv storage to implement metaModel.Client
// and metaModel.KV interface. MetaMock is used in unit test.
// not support Option yet
type MetaMock struct {
	sync.Mutex
	store    map[string]string
	revision int64
}

// NewMetaMock creates a new MetaMock instance
func NewMetaMock() *MetaMock {
	return &MetaMock{
		store: make(map[string]string),
	}
}

// Delete implements metaModel.KV.Delete
func (m *MetaMock) Delete(ctx context.Context, key string, opts ...metaModel.OpOption) (*metaModel.DeleteResponse, metaModel.Error) {
	m.Lock()
	defer m.Unlock()

	return m.deleteNoLock(ctx, key, opts...)
}

func (m *MetaMock) deleteNoLock(_ context.Context, key string, _ ...metaModel.OpOption) (*metaModel.DeleteResponse, metaModel.Error) {
	delete(m.store, key)
	m.revision++
	return &metaModel.DeleteResponse{
		Header: &metaModel.ResponseHeader{
			ClusterID: "mock_cluster",
		},
	}, nil
}

// Put implements metaModel.KV.Put
func (m *MetaMock) Put(ctx context.Context, key, value string) (*metaModel.PutResponse, metaModel.Error) {
	m.Lock()
	defer m.Unlock()

	return m.putNoLock(ctx, key, value)
}

func (m *MetaMock) putNoLock(_ context.Context, key, value string) (*metaModel.PutResponse, metaModel.Error) {
	m.store[key] = value
	m.revision++
	return &metaModel.PutResponse{
		Header: &metaModel.ResponseHeader{
			ClusterID: "mock_cluster",
		},
	}, nil
}

// Get implements metaModel.KV.Get
func (m *MetaMock) Get(ctx context.Context, key string, opts ...metaModel.OpOption) (*metaModel.GetResponse, metaModel.Error) {
	m.Lock()
	defer m.Unlock()

	return m.getNoLock(ctx, key, opts...)
}

func (m *MetaMock) getNoLock(_ context.Context, key string, _ ...metaModel.OpOption) (*metaModel.GetResponse, metaModel.Error) {
	ret := &metaModel.GetResponse{
		Header: &metaModel.ResponseHeader{
			ClusterID: "mock_cluster",
		},
	}
	for k, v := range m.store {
		if !strings.HasPrefix(k, key) {
			continue
		}
		ret.Kvs = append(ret.Kvs, &metaModel.KeyValue{
			Key:   []byte(k),
			Value: []byte(v),
		})
	}
	return ret, nil
}

// Do implements Do of KVExt
func (m *MetaMock) Do(ctx context.Context, op metaModel.Op) (metaModel.OpResponse, metaModel.Error) {
	m.Lock()
	defer m.Unlock()

	return m.doNoLock(ctx, op)
}

func (m *MetaMock) doNoLock(ctx context.Context, op metaModel.Op) (metaModel.OpResponse, metaModel.Error) {
	switch {
	case op.IsGet():
		rsp, err := m.getNoLock(ctx, string(op.KeyBytes()))
		if err != nil {
			return metaModel.OpResponse{}, err
		}
		return rsp.OpResponse(), nil
	case op.IsDelete():
		rsp, err := m.deleteNoLock(ctx, string(op.KeyBytes()))
		if err != nil {
			return metaModel.OpResponse{}, err
		}
		return rsp.OpResponse(), nil
	case op.IsPut():
		rsp, err := m.putNoLock(ctx, string(op.KeyBytes()), string(op.ValueBytes()))
		if err != nil {
			return metaModel.OpResponse{}, err
		}
		return rsp.OpResponse(), nil
	default:
	}

	return metaModel.OpResponse{}, &mockError{
		caused: errors.ErrMetaOptionInvalid.Wrap(fmt.Errorf("unrecognized op type:%d", op.T)),
	}
}

// Txn implements metaModel.KV.Txn
func (m *MetaMock) Txn(ctx context.Context) metaModel.Txn {
	return &mockTxn{
		m: m,
		c: ctx,
	}
}

// Close implements pkg/meta/metaModel.Close
func (m *MetaMock) Close() error {
	return nil
}

// GenEpoch implements pkg/meta/metaModel.Client.GenEpoch
func (m *MetaMock) GenEpoch(ctx context.Context) (int64, error) {
	m.Lock()
	defer m.Unlock()

	m.revision++
	return m.revision, nil
}

type mockError struct {
	caused error
}

func (e *mockError) IsRetryable() bool {
	return false
}

func (e *mockError) Error() string {
	return e.caused.Error()
}
