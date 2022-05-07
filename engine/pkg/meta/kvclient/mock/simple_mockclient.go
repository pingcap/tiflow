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

	cerrors "github.com/pingcap/tiflow/engine/pkg/errors"
	metaclient "github.com/pingcap/tiflow/engine/pkg/meta/metaclient"
)

type mockTxn struct {
	c   context.Context
	m   *MetaMock
	ops []metaclient.Op
}

func (t *mockTxn) Do(ops ...metaclient.Op) metaclient.Txn {
	t.ops = append(t.ops, ops...)
	return t
}

func (t *mockTxn) Commit() (*metaclient.TxnResponse, metaclient.Error) {
	txnRsp := &metaclient.TxnResponse{
		Header: &metaclient.ResponseHeader{
			ClusterID: "mock_cluster",
		},
		Responses: make([]metaclient.ResponseOp, 0, len(t.ops)),
	}

	// we lock the MetaMock to simulate the SERIALIZABLE isolation
	t.m.Lock()
	defer t.m.Unlock()

	for _, op := range t.ops {
		rsp, err := t.m.DoNoLock(t.c, op)
		if err != nil {
			return nil, err
		}
		switch {
		case op.IsGet():
			txnRsp.Responses = append(txnRsp.Responses, metaclient.ResponseOp{
				Response: &metaclient.ResponseOpResponseGet{
					ResponseGet: rsp.Get(),
				},
			})
		case op.IsPut():
			txnRsp.Responses = append(txnRsp.Responses, metaclient.ResponseOp{
				Response: &metaclient.ResponseOpResponsePut{
					ResponsePut: rsp.Put(),
				},
			})
		case op.IsDelete():
			txnRsp.Responses = append(txnRsp.Responses, metaclient.ResponseOp{
				Response: &metaclient.ResponseOpResponseDelete{
					ResponseDelete: rsp.Del(),
				},
			})
		default:
			return nil, &mockError{
				caused: cerrors.ErrMetaOptionInvalid.Wrap(fmt.Errorf("unrecognized op type:%d", op.T)),
			}
		}
	}

	return txnRsp, nil
}

// not support Option yet
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

func (m *MetaMock) Delete(ctx context.Context, key string, opts ...metaclient.OpOption) (*metaclient.DeleteResponse, metaclient.Error) {
	m.Lock()
	defer m.Unlock()

	return m.DeleteNoLock(ctx, key, opts...)
}

func (m *MetaMock) DeleteNoLock(ctx context.Context, key string, opts ...metaclient.OpOption) (*metaclient.DeleteResponse, metaclient.Error) {
	delete(m.store, key)
	m.revision++
	return &metaclient.DeleteResponse{
		Header: &metaclient.ResponseHeader{
			ClusterID: "mock_cluster",
		},
	}, nil
}

func (m *MetaMock) Put(ctx context.Context, key, value string) (*metaclient.PutResponse, metaclient.Error) {
	m.Lock()
	defer m.Unlock()

	return m.PutNoLock(ctx, key, value)
}

func (m *MetaMock) PutNoLock(ctx context.Context, key, value string) (*metaclient.PutResponse, metaclient.Error) {
	m.store[key] = value
	m.revision++
	return &metaclient.PutResponse{
		Header: &metaclient.ResponseHeader{
			ClusterID: "mock_cluster",
		},
	}, nil
}

func (m *MetaMock) Get(ctx context.Context, key string, opts ...metaclient.OpOption) (*metaclient.GetResponse, metaclient.Error) {
	m.Lock()
	defer m.Unlock()

	return m.GetNoLock(ctx, key, opts...)
}

func (m *MetaMock) GetNoLock(ctx context.Context, key string, opts ...metaclient.OpOption) (*metaclient.GetResponse, metaclient.Error) {
	ret := &metaclient.GetResponse{
		Header: &metaclient.ResponseHeader{
			ClusterID: "mock_cluster",
		},
	}
	for k, v := range m.store {
		if !strings.HasPrefix(k, key) {
			continue
		}
		ret.Kvs = append(ret.Kvs, &metaclient.KeyValue{
			Key:   []byte(k),
			Value: []byte(v),
		})
	}
	return ret, nil
}

func (m *MetaMock) Do(ctx context.Context, op metaclient.Op) (metaclient.OpResponse, metaclient.Error) {
	m.Lock()
	defer m.Unlock()

	return m.DoNoLock(ctx, op)
}

func (m *MetaMock) DoNoLock(ctx context.Context, op metaclient.Op) (metaclient.OpResponse, metaclient.Error) {
	switch {
	case op.IsGet():
		rsp, err := m.GetNoLock(ctx, string(op.KeyBytes()))
		if err != nil {
			return metaclient.OpResponse{}, err
		}
		return rsp.OpResponse(), nil
	case op.IsDelete():
		rsp, err := m.DeleteNoLock(ctx, string(op.KeyBytes()))
		if err != nil {
			return metaclient.OpResponse{}, err
		}
		return rsp.OpResponse(), nil
	case op.IsPut():
		rsp, err := m.PutNoLock(ctx, string(op.KeyBytes()), string(op.ValueBytes()))
		if err != nil {
			return metaclient.OpResponse{}, err
		}
		return rsp.OpResponse(), nil
	default:
	}

	return metaclient.OpResponse{}, &mockError{
		caused: cerrors.ErrMetaOptionInvalid.Wrap(fmt.Errorf("unrecognized op type:%d", op.T)),
	}
}

func (m *MetaMock) Txn(ctx context.Context) metaclient.Txn {
	return &mockTxn{
		m: m,
		c: ctx,
	}
}

func (m *MetaMock) Close() error {
	return nil
}

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
