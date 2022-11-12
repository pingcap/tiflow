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

package etcdkv

import (
	"context"
	"fmt"
	"sync"

	metaModel "github.com/pingcap/tiflow/engine/pkg/meta/model"
	"github.com/pingcap/tiflow/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Defines fake key/value pair which is used in aliveness check or epoch generation
const (
	FakeKey   = "/fake-key"
	FakeValue = "/fake-value"
)

// We will make follow abstracts and implement for KVClient
//				NewEtcdKVClient(KVClient)
//					/			\
//				prefixKV	    |
//					|			|
//					KV		   Client
//					\			/
//					   etcdIml(KVClient)

// etcdKVClientImpl is the etcd implement of KVClient interface
// Since we always get the latest data, we will set etcd-server with autocompact parameters
// -auto-compaction-mode: revision
// -auto-compaction-retention: 100
type etcdKVClientImpl struct {
	cli *clientv3.Client
}

// NewEtcdKVClientImpl creates a new etcdKVClientImpl instance
func NewEtcdKVClientImpl(cli *clientv3.Client) (*etcdKVClientImpl, error) {
	c := &etcdKVClientImpl{
		cli: cli,
	}
	return c, nil
}

func (c *etcdKVClientImpl) getEtcdOptions(op metaModel.Op) []clientv3.OpOption {
	etcdOps := make([]clientv3.OpOption, 0, 1)
	switch {
	case op.IsOptsWithPrefix():
		etcdOps = append(etcdOps, clientv3.WithPrefix())
	case op.IsOptsWithFromKey():
		etcdOps = append(etcdOps, clientv3.WithFromKey())
	case op.IsOptsWithRange():
		etcdOps = append(etcdOps, clientv3.WithRange(string(op.RangeBytes())))
	}

	return etcdOps
}

func (c *etcdKVClientImpl) getEtcdOp(op metaModel.Op) clientv3.Op {
	opts := c.getEtcdOptions(op)
	switch {
	case op.IsGet():
		return clientv3.OpGet(string(op.KeyBytes()), opts...)
	case op.IsPut():
		return clientv3.OpPut(string(op.KeyBytes()), string(op.ValueBytes()), opts...)
	case op.IsDelete():
		return clientv3.OpDelete(string(op.KeyBytes()), opts...)
	case op.IsTxn():
		ops := op.Txn()
		etcdOps := make([]clientv3.Op, 0, len(ops))
		for _, sop := range ops {
			etcdOps = append(etcdOps, c.getEtcdOp(sop))
		}
		return clientv3.OpTxn(nil, etcdOps, nil)
	}

	panic("unknown op type")
}

func (c *etcdKVClientImpl) Put(ctx context.Context, key, val string) (*metaModel.PutResponse, metaModel.Error) {
	op := metaModel.OpPut(key, val)
	etcdResp, err := c.cli.Do(ctx, c.getEtcdOp(op))
	if err != nil {
		return nil, etcdErrorFromOpFail(err)
	}

	putRsp := etcdResp.Put()
	return makePutResp(putRsp), nil
}

func (c *etcdKVClientImpl) Get(ctx context.Context, key string, opts ...metaModel.OpOption) (*metaModel.GetResponse, metaModel.Error) {
	op := metaModel.OpGet(key, opts...)
	if err := op.CheckValidOp(); err != nil {
		return nil, &etcdError{
			displayed: errors.ErrMetaOptionInvalid.Wrap(err),
		}
	}

	etcdResp, err := c.cli.Do(ctx, c.getEtcdOp(op))
	if err != nil {
		return nil, etcdErrorFromOpFail(err)
	}

	getRsp := etcdResp.Get()
	return makeGetResp(getRsp), nil
}

func (c *etcdKVClientImpl) Delete(ctx context.Context, key string, opts ...metaModel.OpOption) (*metaModel.DeleteResponse, metaModel.Error) {
	op := metaModel.OpDelete(key, opts...)
	if err := op.CheckValidOp(); err != nil {
		return nil, &etcdError{
			displayed: errors.ErrMetaOptionInvalid.Wrap(err),
		}
	}

	etcdResp, err := c.cli.Do(ctx, c.getEtcdOp(op))
	if err != nil {
		return nil, etcdErrorFromOpFail(err)
	}

	delRsp := etcdResp.Del()
	return makeDeleteResp(delRsp), nil
}

func (c *etcdKVClientImpl) Do(ctx context.Context, op metaModel.Op) (metaModel.OpResponse, metaModel.Error) {
	if err := op.CheckValidOp(); err != nil {
		return metaModel.OpResponse{}, &etcdError{
			displayed: errors.ErrMetaOptionInvalid.Wrap(err),
		}
	}

	etcdResp, err := c.cli.Do(ctx, c.getEtcdOp(op))
	if err != nil {
		return metaModel.OpResponse{}, etcdErrorFromOpFail(err)
	}

	switch {
	case op.IsGet():
		rsp := etcdResp.Get()
		getRsp := makeGetResp(rsp)
		return getRsp.OpResponse(), nil
	case op.IsPut():
		rsp := etcdResp.Put()
		putRsp := makePutResp(rsp)
		return putRsp.OpResponse(), nil
	case op.IsDelete():
		rsp := etcdResp.Del()
		delRsp := makeDeleteResp(rsp)
		return delRsp.OpResponse(), nil
	case op.IsTxn():
		rsp := etcdResp.Txn()
		txnRsp := makeTxnResp(rsp)
		return txnRsp.OpResponse(), nil
	default:
	}

	return metaModel.OpResponse{}, &etcdError{
		displayed: errors.ErrMetaOptionInvalid.Wrap(fmt.Errorf("unrecognized op type:%d", op.T)),
	}
}

type etcdTxn struct {
	clientv3.Txn

	mu  sync.Mutex
	kv  *etcdKVClientImpl
	ops []clientv3.Op
	// cache error to make chain operation work
	Err       *etcdError
	committed bool
}

func (c *etcdKVClientImpl) Txn(ctx context.Context) metaModel.Txn {
	return &etcdTxn{
		Txn: c.cli.Txn(ctx),
		kv:  c,
		ops: make([]clientv3.Op, 0, 3),
	}
}

func (c *etcdKVClientImpl) Close() error {
	return nil
}

func (c *etcdKVClientImpl) GenEpoch(ctx context.Context) (int64, error) {
	resp, err := c.cli.Put(ctx, FakeKey, FakeValue)
	if err != nil {
		return 0, etcdErrorFromOpFail(errors.WrapError(errors.ErrMasterEtcdEpochFail, err))
	}

	return resp.Header.Revision, nil
}

func (t *etcdTxn) Do(ops ...metaModel.Op) metaModel.Txn {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.Err != nil {
		return t
	}
	if t.committed {
		t.Err = &etcdError{
			displayed: errors.ErrMetaCommittedTxn.GenWithStackByArgs(),
		}
		return t
	}

	etcdOps := make([]clientv3.Op, 0, len(ops))
	for _, op := range ops {
		if op.IsTxn() {
			t.Err = &etcdError{
				displayed: errors.ErrMetaNestedTxn.GenWithStackByArgs(),
			}
			return t
		}
		etcdOps = append(etcdOps, t.kv.getEtcdOp(op))
	}

	t.ops = append(t.ops, etcdOps...)
	return t
}

func (t *etcdTxn) Commit() (*metaModel.TxnResponse, metaModel.Error) {
	t.mu.Lock()
	if t.Err != nil {
		t.mu.Unlock()
		return nil, t.Err
	}
	if t.committed {
		t.Err = &etcdError{
			displayed: errors.ErrMetaCommittedTxn.GenWithStackByArgs(),
		}
		t.mu.Unlock()
		return nil, t.Err
	}
	t.committed = true
	t.mu.Unlock()

	t.Txn.Then(t.ops...)
	etcdResp, err := t.Txn.Commit()
	if err != nil {
		return nil, etcdErrorFromOpFail(err)
	}

	return makeTxnResp(etcdResp), nil
}
