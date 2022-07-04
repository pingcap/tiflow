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

package meta

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/tiflow/engine/pkg/meta/mock"
	metaModel "github.com/pingcap/tiflow/engine/pkg/meta/model"
)

// Backend KV store data:
//|		KEY		|		VALUE		|		TTL		|	REVISION	|
//|		apple	|		 red		|		15		|		1		|
//|		orange  |		orange		|		0		|		10		|
//|		data	|		flow		|		0		|		5		|
//|		ticdc	|		kv		|		0		|		5		|
//|		dm		|		DDL		|		0		|		18		|

// nolint:deadcode, ineffassign
func test(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	cli := mock.NewMockKVClient(ctrl)
	defer cli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	// nolint:typecheck
	var (
		putRsp *metaModel.PutResponse
		getRsp *metaModel.GetResponse
		delRsp *metaModel.DeleteResponse
		txnRsp *metaModel.TxnResponse
		err    error
	)

	//
	// Basic Put/Get/Delete
	//
	putRsp, err = cli.Put(ctx, "TiDB", "DistDB")
	// expect err == nil

	// always get latest revision data
	getRsp, err = cli.Get(ctx, "TiDB")
	// expect err == nil
	// expect len(getRsp.Kvs) == 1
	kv := getRsp.Kvs[0]
	var _ *metaModel.KeyValue = kv
	// expect kv.Key == []byte("TiDB")
	// expect kv.Value == []byte("DistDB")

	delRsp, err = cli.Delete(ctx, "TiDB")
	// expect err == nil
	getRsp, err = cli.Get(ctx, "TiDB")
	// expect err == nil
	// expect len(getRsp.Kvs) == 0

	//
	//	Options: Key Range/From Key/Key Prefix attributes
	//
	// Key Range, forbit Put using metaModel.WithRange
	// current data:
	//		apple  red
	//		orange orange
	//		ticdc  kv
	//		dm	   DDL
	getRsp, err = cli.Get(ctx, "ap", metaModel.WithRange("zz"))
	// expect len(getRsp.Kvs) == 4(apple. orange, ticdc, dm)
	getRsp, err = cli.Get(ctx, "ap", metaModel.WithRange("apple2"))
	// expect len(getRsp.Kvs) == 1(apple)
	delRsp, err = cli.Delete(ctx, "dzst", metaModel.WithRange("panda"))
	// delete key orange

	// From Key, forbit Put using metaModel.WithFromKey
	// current data:
	//		apple  red
	//		ticdc  kv
	//		dm	   DDL
	getRsp, err = cli.Get(ctx, "data", metaModel.WithFromKey())
	// expect len(getRsp.Kvs) == 2(ticdc, dm)
	delRsp, err = cli.Delete(ctx, "tian", metaModel.WithFromKey())
	// delete key ticdc

	// Key Prefix, forbit Put using metaModel.WithPrefix
	// current data:
	//		apple  red
	//		apple2  green
	//		ticdc  kv
	//		dm	   DDL
	getRsp, err = cli.Get(ctx, "apple", metaModel.WithPrefix())
	// expect len(getRsp.Kvs) == 2(apple, apple2)
	delRsp, err = cli.Delete(ctx, "apple", metaModel.WithPrefix())
	// delete key apple, apple2

	//
	// Txn, forbit nested txn
	//
	// current data:
	//		apple  red
	//		apple2  green
	//		ticdc  kv
	//		dm	   DDL
	getOp := metaModel.OpGet("apple3", metaModel.WithRange("zz"))
	_ = getRsp
	putOp := metaModel.OpPut("apple3", "t3")
	_ = putRsp
	delOp := metaModel.OpDelete("apple3", metaModel.WithRange("ti"))
	_ = delRsp
	_ = err
	txn := cli.Txn(ctx)
	txnRsp, err = txn.Do(getOp).Do(putOp).Do(delOp).Commit()
	_ = txnRsp
	// When succeed, txnRsp will contain a getRsp, a putRsp and a delRsp
	// txnRsp.ResponseOp[0].GetResponseGet()
	// txnRsp.ResponseOp[1].GetResponsePut()
	// txnRsp.ResponseOp[2].GetResponseDelete()
	// When failed, all ops will take no effect.

	epoch, err := cli.GenEpoch(ctx)
	_ = epoch
	// expect epoch is always an increasing int64
}
