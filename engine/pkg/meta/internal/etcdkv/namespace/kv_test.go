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

package namespace

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/tiflow/engine/pkg/meta/mock"
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

type txnAction struct {
	acts []action
	// return error
	err error
}

func prepareData(ctx context.Context, t *testing.T, cli metaModel.KV, p prepare) {
	if p.kvs != nil {
		for _, kv := range p.kvs {
			prsp, perr := cli.Put(ctx, kv.key, kv.value)
			require.Nil(t, perr)
			require.NotNil(t, prsp)
		}
	}
}

func testAction(ctx context.Context, t *testing.T, cli metaModel.KV, acts []action) {
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

func testTxnAction(ctx context.Context, t *testing.T, cli metaModel.KV, txns []txnAction) {
	for _, txn := range txns {
		ops := make([]metaModel.Op, 0, len(txn.acts))
		for _, act := range txn.acts {
			switch act.t {
			case tGet:
				ops = append(ops, metaModel.OpGet(act.do.key, act.opts...))
			case tPut:
				ops = append(ops, metaModel.OpPut(act.do.key, act.do.value))
			case tDel:
				ops = append(ops, metaModel.OpDelete(act.do.key, act.opts...))
			default:
				require.FailNow(t, "unexpected action type")
			}
		}
		tx := cli.Txn(ctx)
		tx.Do(ops...)
		rsp, err := tx.Commit()
		// test txn rsp
		if txn.err != nil {
			require.Error(t, err)
			continue
		}
		require.Nil(t, err)
		require.Len(t, rsp.Responses, len(txn.acts))
		for i, r := range rsp.Responses {
			act := txn.acts[i]
			switch act.t {
			case tGet:
				rr := r.GetResponseGet()
				require.NotNil(t, rr)
				expected := act.q.expected
				require.Len(t, rr.Kvs, len(expected))
				for i, kv := range rr.Kvs {
					require.Equal(t, string(kv.Key), expected[i].key)
					require.Equal(t, string(kv.Value), expected[i].value)
				}
			case tPut:
				rr := r.GetResponsePut()
				require.NotNil(t, rr)
			case tDel:
				rr := r.GetResponseDelete()
				require.NotNil(t, rr)
			default:
				require.FailNow(t, "unexpected action type")
			}
		}
	}
}

func TestPrefixBasicKV(t *testing.T) {
	t.Parallel()

	mock := mock.NewMetaMock()
	defer mock.Close()
	cli := NewPrefixKV(mock, "test")
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
}

func TestTxn(t *testing.T) {
	t.Parallel()

	mock := mock.NewMetaMock()
	defer mock.Close()
	cli := NewPrefixKV(mock, "test")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	input := prepare{
		kvs: []kv{
			{"hello1", "world1"},
			{"hello2", "world2"},
			{"interesting", "world"},
			{"dataflow", "engine"},
			{"TiDB", "component"},
		},
	}
	txns := []txnAction{
		{
			acts: []action{
				{
					t:    tGet,
					do:   kv{"hello1", ""},
					opts: []metaModel.OpOption{},
					q: query{
						expected: []kv{
							{"hello1", "world1"},
						},
					},
				},
				{
					t:    tPut,
					do:   kv{"hello3", "world3"},
					opts: []metaModel.OpOption{},
					q: query{
						expected: []kv{},
					},
				},
				{
					t:    tPut,
					do:   kv{"dataflow2", "engine2"},
					opts: []metaModel.OpOption{},
					q: query{
						expected: []kv{},
					},
				},
				{
					t:    tDel,
					do:   kv{"dataflow3", ""},
					opts: []metaModel.OpOption{},
					q: query{
						expected: []kv{},
					},
				},
			},
		},
	}

	prepareData(ctx, t, cli, input)
	testTxnAction(ctx, t, cli, txns)
}
