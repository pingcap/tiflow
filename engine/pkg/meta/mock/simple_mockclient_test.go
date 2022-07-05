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
	"sync"
	"testing"
	"time"

	metaModel "github.com/pingcap/tiflow/engine/pkg/meta/model"
	"github.com/stretchr/testify/require"
)

type kv struct {
	key   string
	value string
}

type prepare struct {
	kvs []kv
}

type optype int

const (
	tNone optype = iota
	tGet
	tPut
	tDel
	tTxn
)

type query struct {
	key  string
	opts []metaModel.OpOption
	err  error
	// for txn: we only use expected
	expected []kv
}

type action struct {
	t optype
	// do action
	do   kv
	opts []metaModel.OpOption
	// query action
	q query
}

func prepareData(ctx context.Context, t *testing.T, cli metaModel.KVClient, p prepare) {
	if p.kvs != nil {
		for _, kv := range p.kvs {
			prsp, perr := cli.Put(ctx, kv.key, kv.value)
			require.Nil(t, perr)
			require.NotNil(t, prsp)
		}
	}
}

func testAction(ctx context.Context, t *testing.T, cli metaModel.KVClient, acts []action) {
	for _, act := range acts {
		switch act.t {
		case tGet:
			rsp, err := cli.Get(ctx, act.do.key, act.opts...)
			require.Nil(t, err)
			require.NotNil(t, rsp)
		case tPut:
			rsp, err := cli.Put(ctx, act.do.key, act.do.value)
			require.Nil(t, err)
			require.NotNil(t, rsp)
		case tDel:
			rsp, err := cli.Delete(ctx, act.do.key, act.opts...)
			require.Nil(t, err)
			require.NotNil(t, rsp)
		case tTxn:
			require.FailNow(t, "unexpected txn action")
		case tNone:
			// do nothing
		default:
			require.FailNow(t, "unexpected action type")
		}

		rsp, err := cli.Get(ctx, act.q.key, act.q.opts...)
		if act.q.err != nil {
			require.Error(t, err)
			continue
		}
		require.Nil(t, err)
		require.NotNil(t, rsp)
		expected := act.q.expected
		require.Len(t, rsp.Kvs, len(expected))
		for i, kv := range rsp.Kvs {
			require.Equal(t, string(kv.Key), expected[i].key)
			require.Equal(t, string(kv.Value), expected[i].value)
		}
	}
}

func TestMockBasicKV(t *testing.T) {
	t.Parallel()

	cli := NewMetaMock()
	defer cli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	input := prepare{
		kvs: []kv{},
	}
	actions := []action{
		{
			t: tNone,
			q: query{
				key:      "hello",
				opts:     []metaModel.OpOption{},
				expected: []kv{},
			},
		},
		{
			t:    tPut,
			do:   kv{"hello", "world"},
			opts: []metaModel.OpOption{},
			q: query{
				key:  "hello",
				opts: []metaModel.OpOption{},
				expected: []kv{
					{"hello", "world"},
				},
			},
		},
		{
			t:    tDel,
			do:   kv{"hello", ""},
			opts: []metaModel.OpOption{},
			q: query{
				key:      "hello",
				opts:     []metaModel.OpOption{},
				expected: []kv{},
			},
		},
		{
			t:    tPut,
			do:   kv{"hello", "new world"},
			opts: []metaModel.OpOption{},
			q: query{
				key:  "hello",
				opts: []metaModel.OpOption{},
				expected: []kv{
					{"hello", "new world"},
				},
			},
		},
	}

	// prepare data and test query
	prepareData(ctx, t, cli, input)
	testAction(ctx, t, cli, actions)

	cli.Close()
}

func testGenerator(t *testing.T, kvcli metaModel.KVClient) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	firstEpoch, err := kvcli.GenEpoch(ctx)
	require.Nil(t, err)
	require.GreaterOrEqual(t, firstEpoch, int64(0))

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			epoch, err := kvcli.GenEpoch(ctx)
			require.Nil(t, err)
			require.GreaterOrEqual(t, epoch, int64(0))
			oldEpoch := epoch

			epoch, err = kvcli.GenEpoch(ctx)
			require.Nil(t, err)
			require.Greater(t, epoch, oldEpoch)
		}()
	}

	wg.Wait()
	lastEpoch, err := kvcli.GenEpoch(ctx)
	require.Nil(t, err)
	require.Equal(t, int64(201), lastEpoch-firstEpoch)
}

func TestGenEpoch(t *testing.T) {
	t.Parallel()

	cli := NewMetaMock()
	defer cli.Close()
	testGenerator(t, cli)
}

func TestMockTxn(t *testing.T) {
	t.Parallel()

	cli := NewMetaMock()
	defer cli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// prepare data
	_, err := cli.Put(ctx, "key1", "value1")
	require.Nil(t, err)
	_, err = cli.Put(ctx, "key2", "value2")
	require.Nil(t, err)

	txn := cli.Txn(ctx)
	txn.Do(metaModel.OpGet("key1"))
	txn.Do(metaModel.OpPut("key3", "value3"))
	txn.Do(metaModel.OpDelete("key2"))
	txn.Do(metaModel.OpGet("key2"))
	txnRsp, err := txn.Commit()
	require.Nil(t, err)
	require.Len(t, txnRsp.Responses, 4)

	getRsp := txnRsp.Responses[0].GetResponseGet()
	require.Len(t, getRsp.Kvs, 1)
	require.Equal(t, "key1", string(getRsp.Kvs[0].Key))
	require.Equal(t, "value1", string(getRsp.Kvs[0].Value))

	putRsp := txnRsp.Responses[1].GetResponsePut()
	require.NotNil(t, putRsp)

	delRsp := txnRsp.Responses[2].GetResponseDelete()
	require.NotNil(t, delRsp)

	getRsp = txnRsp.Responses[3].GetResponseGet()
	require.Len(t, getRsp.Kvs, 0)

	rsp, err := cli.Txn(ctx).Do(metaModel.OpTxn([]metaModel.Op{metaModel.EmptyOp})).Commit()
	require.Nil(t, rsp)
	require.Error(t, err)

	rsp, err = cli.Txn(ctx).Do(metaModel.EmptyOp).Commit()
	require.Nil(t, rsp)
	require.Error(t, err)
}
