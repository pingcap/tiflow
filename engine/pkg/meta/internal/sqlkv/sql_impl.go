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

package sqlkv

import (
	"context"
	"sync"

	cerrors "github.com/pingcap/tiflow/engine/pkg/errors"
	"github.com/pingcap/tiflow/engine/pkg/meta/metaclient"
	ormModel "github.com/pingcap/tiflow/engine/pkg/orm/model"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// sqlImpl is the mysql-compatible implement for KVClient
type sqlImpl struct {
	// gorm claim to be thread safe
	db *gorm.DB
}

func NewSQLImpl(db *gorm.DB) (*sqlImpl, error) {
	return &sqlImpl{
		db: db,
	}, nil
}

func (c *sqlImpl) Close() error {
	return nil
}

// GetEpoch implements GenEpoch interface of Client
// NOTE: epoch_logic table SHOULD HAVE BEEN INITIALIZED
func (c *sqlImpl) GenEpoch(ctx context.Context) (int64, error) {
	return ormModel.GenEpoch(ctx, c.db)
}

// Put implements Put interface of KV
func (c *sqlImpl) Put(ctx context.Context, key, val string) (*metaclient.PutResponse, metaclient.Error) {
	op := metaclient.OpPut(key, val)
	return c.doPut(ctx, c.db, &op)
}

func (c *sqlImpl) doPut(ctx context.Context, db *gorm.DB, op *metaclient.Op) (*metaclient.PutResponse, metaclient.Error) {
	if err := c.db.WithContext(ctx).Clauses(clause.OnConflict{
		UpdateAll: true,
	}).Create(&MetaKV{
		JobID: c.jobID,
		Key:   op.KeyBytes(),
		Value: op.ValueBytes(),
	}).Error; err != nil {
		return nil, sqlErrorFromOpFail(err)
	}

	return &metaclient.PutResponse{
		Header: &metaclient.ResponseHeader{},
	}, nil
}

// Get implements Get interface of KV
func (c *sqlImpl) Get(ctx context.Context, key string, opts ...metaclient.OpOption) (*metaclient.GetResponse, metaclient.Error) {
	op := metaclient.OpGet(key, opts...)
	return c.doGet(ctx, c.db, &op)
}

func (c *sqlImpl) doGet(ctx context.Context, db *gorm.DB, op *metaclient.Op) (*metaclient.GetResponse, metaclient.Error) {
	if err := op.CheckValidOp(); err != nil {
		return nil, &sqlError{
			displayed: cerrors.ErrMetaOptionInvalid.Wrap(err),
		}
	}

	var (
		kvs        []*metaclient.KeyValue
		kv         metaclient.KeyValue
		err        error
		isPointGet bool
		key        = op.KeyBytes()
	)

	db = db.WithContext(ctx)
	switch {
	case op.IsOptsWithRange():
		err = db.Where("key >= ? AND key < ?", key, op.RangeBytes()).Find(&kvs).Error
	case op.IsOptsWithPrefix():
		err = db.Where("key like ?%", key).Find(&kvs).Error
	case op.IsOptsWithFromKey():
		err = db.Where("key >= ?", key).Find(&kvs).Error
	default:
		err = db.Where("key = ?", key).First(&kv).Error
		isPointGet = true
	}
	if err != nil {
		return nil, sqlErrorFromOpFail(err)
	}

	if isPointGet {
		kvs = make([]*metaclient.KeyValue, 0, 1)
		kvs = append(kvs, &kv)
	}

	return &metaclient.GetResponse{
		Header: &metaclient.ResponseHeader{},
		Kvs:    kvs,
	}, nil
}

// Delete implements Delete interface of KV
func (c *sqlImpl) Delete(ctx context.Context, key string, opts ...metaclient.OpOption) (*metaclient.DeleteResponse, metaclient.Error) {
	op := metaclient.OpDelete(key, opts...)
	return c.doDelete(ctx, c.db, &op)
}

func (c *sqlImpl) doDelete(ctx context.Context, db *gorm.DB, op *metaclient.Op) (*metaclient.DeleteResponse, metaclient.Error) {
	if err := op.CheckValidOp(); err != nil {
		return nil, &sqlError{
			displayed: cerrors.ErrMetaOptionInvalid.Wrap(err),
		}
	}

	var (
		err error
		key = op.KeyBytes()
	)

	db = db.WithContext(ctx)
	switch {
	case op.IsOptsWithRange():
		err = db.Where("key >= ? AND key < ?", key,
			op.RangeBytes()).Delete(&metaclient.KeyValue{}).Error
	case op.IsOptsWithPrefix():
		err = db.Where("key like ?%", key).Delete(&metaclient.KeyValue{}).Error
	case op.IsOptsWithFromKey():
		err = db.Where("key >= ?", key).Delete(&metaclient.KeyValue{}).Error
	default:
		err = db.Where("key = ?", key).Delete(&metaclient.KeyValue{}).Error
	}
	if err != nil {
		return nil, sqlErrorFromOpFail(err)
	}

	return &metaclient.DeleteResponse{
		Header: &metaclient.ResponseHeader{},
	}, nil
}

type sqlTxn struct {
	mu sync.Mutex

	ctx  context.Context
	impl *sqlImpl
	ops  []metaclient.Op
	// cache error to make chain operation work
	Err       *sqlError
	committed bool
}

// Txn implements Txn interface of KV
func (c *sqlImpl) Txn(ctx context.Context) metaclient.Txn {
	return &sqlTxn{
		ctx:  ctx,
		impl: c,
		ops:  make([]metaclient.Op, 0, 2),
	}
}

// Do implements Do interface of Txn
func (t *sqlTxn) Do(ops ...metaclient.Op) metaclient.Txn {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.Err != nil {
		return t
	}

	if t.committed {
		t.Err = &sqlError{
			displayed: cerrors.ErrMetaCommittedTxn.GenWithStackByArgs("txn had been committed"),
		}
		return t
	}

	t.ops = append(t.ops, ops...)
	return t
}

// Commit implements Commit interface of Txn
func (t *sqlTxn) Commit() (*metaclient.TxnResponse, metaclient.Error) {
	t.mu.Lock()
	if t.Err != nil {
		t.mu.Unlock()
		return nil, t.Err
	}
	if t.committed {
		t.Err = &sqlError{
			displayed: cerrors.ErrMetaCommittedTxn.GenWithStackByArgs("txn had been committed"),
		}
		t.mu.Unlock()
		return nil, t.Err
	}
	t.committed = true
	t.mu.Unlock()

	var txnRsp metaclient.TxnResponse
	txnRsp.Responses = make([]metaclient.ResponseOp, 0, len(t.ops))
	err := t.impl.db.Transaction(func(tx *gorm.DB) error {
		for _, op := range t.ops {
			switch {
			case op.IsGet():
				rsp, err := t.impl.doGet(t.ctx, tx, &op)
				if err != nil {
					return err // rollback
				}
				txnRsp.Responses = append(txnRsp.Responses, makeGetResponseOp(rsp))
			case op.IsPut():
				rsp, err := t.impl.doPut(t.ctx, tx, &op)
				if err != nil {
					return err
				}
				txnRsp.Responses = append(txnRsp.Responses, makePutResponseOp(rsp))
			case op.IsDelete():
				rsp, err := t.impl.doDelete(t.ctx, tx, &op)
				if err != nil {
					return err
				}
				txnRsp.Responses = append(txnRsp.Responses, makeDelResponseOp(rsp))
			case op.IsTxn():
				return &sqlError{
					displayed: cerrors.ErrMetaNestedTxn.GenWithStackByArgs("unsupported nested txn"),
				}
			default:
				return &sqlError{
					displayed: cerrors.ErrMetaOpFail.GenWithStackByArgs("unknown op type"),
				}
			}
		}

		return nil // commit
	})
	if err != nil {
		err2, ok := err.(*sqlError)
		if ok {
			return nil, err2
		}

		return nil, sqlErrorFromOpFail(err2)
	}

	return &txnRsp, nil
}
