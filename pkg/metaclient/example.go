package metaclient

import (
	"context"
	"time"
)

// Backend KV store data:
//|		KEY		|		VALUE		|		TTL		|	REVISION	|
//|		apple	|		 red		|		15		|		1		|
//|		orange  |		orange		|		0		|		10		|
//|		data	|		flow		|		0		|		5		|
//|		ticdc	|		kv		|		0		|		5		|
//|		dm		|		DDL		|		0		|		18		|

// nolint:deadcode, ineffassign
func Test() {
	endpoint := "http://127.0.0.1:3769"
	cli := NewMockKVClient(endpoint)
	defer cli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	// nolint:typecheck
	var (
		putRsp *PutResponse
		getRsp *GetResponse
		delRsp *DeleteResponse
		txnRsp *TxnResponse
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
	var _ *KeyValue = kv
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
	// Key Range, forbit Put using WithRange
	// current data:
	//		apple  red
	//		orange orange
	//		ticdc  kv
	//		dm	   DDL
	getRsp, err = cli.Get(ctx, "ap", WithRange("zz"))
	// expect len(getRsp.Kvs) == 4(apple. orange, ticdc, dm)
	getRsp, err = cli.Get(ctx, "ap", WithRange("apple2"))
	// expect len(getRsp.Kvs) == 1(apple)
	delRsp, err = cli.Delete(ctx, "dzst", WithRange("panda"))
	// delete key orange

	// From Key, forbit Put using WithFromKey
	// current data:
	//		apple  red
	//		ticdc  kv
	//		dm	   DDL
	getRsp, err = cli.Get(ctx, "data", WithFromKey())
	// expect len(getRsp.Kvs) == 2(ticdc, dm)
	delRsp, err = cli.Delete(ctx, "tian", WithFromKey())
	// delete key ticdc

	// Key Prefix, forbit Put using WithPrefix
	// current data:
	//		apple  red
	//		apple2  green
	//		ticdc  kv
	//		dm	   DDL
	getRsp, err = cli.Get(ctx, "apple", WithPrefix())
	// expect len(getRsp.Kvs) == 2(apple, apple2)
	delRsp, err = cli.Delete(ctx, "apple", WithPrefix())
	// delete key apple, apple2

	//
	// Txn, forbit nested txn
	//
	// current data:
	//		apple  red
	//		apple2  green
	//		ticdc  kv
	//		dm	   DDL
	getOp := OpGet("apple3", WithRange("zz"))
	_ = getRsp
	putOp := OpPut("apple3", "t3")
	_ = putRsp
	delOp := OpDelete("apple3", WithRange("ti"))
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
}
