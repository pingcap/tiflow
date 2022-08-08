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
	"sync"
	"testing"
	"time"

	metaModel "github.com/pingcap/tiflow/engine/pkg/meta/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.etcd.io/etcd/server/v3/embed"
)

type SuiteTestEtcd struct {
	// Include basic suite logic.
	suite.Suite
	e         *embed.Etcd
	endpoints string
}

// The SetupSuite method will be run by testify once, at the very
// start of the testing suite, before any tests are run.
func (suite *SuiteTestEtcd) SetupSuite() {
	svr, endpoints, err := RetryMockBackendEtcd()
	require.NoError(suite.T(), err)

	suite.e = svr
	suite.endpoints = endpoints
}

// The TearDownSuite method will be run by testify once, at the very
// end of the testing suite, after all tests have been run.
func (suite *SuiteTestEtcd) TearDownSuite() {
	CloseEmbededEtcd(suite.e)
}

func clearKeySpace(ctx context.Context, cli metaModel.KVClient) {
	cli.Delete(ctx, "", metaModel.WithFromKey())
}

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

func prepareData(ctx context.Context, t *testing.T, cli metaModel.KVClient, p prepare) {
	clearKeySpace(ctx, cli)
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

func testTxnAction(ctx context.Context, t *testing.T, cli metaModel.KVClient, txns []txnAction) {
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

func (suite *SuiteTestEtcd) TestBasicKV() {
	conf := &metaModel.StoreConfig{
		Endpoints: []string{suite.endpoints},
	}
	t := suite.T()
	cc, err := NewEtcdClient(conf)
	require.Nil(t, err)
	cli, err := NewEtcdKVClientImpl(cc)
	require.Nil(t, err)
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

func (suite *SuiteTestEtcd) TestKeyRangeOption() {
	conf := &metaModel.StoreConfig{
		Endpoints: []string{suite.endpoints},
	}
	t := suite.T()
	cc, err := NewEtcdClient(conf)
	require.Nil(t, err)
	cli, err := NewEtcdKVClientImpl(cc)
	require.Nil(t, err)
	defer cli.Close()
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
	actions := []action{
		{
			t: tNone,
			q: query{
				key:  "hello",
				opts: []metaModel.OpOption{metaModel.WithRange("s")},
				expected: []kv{
					{"hello1", "world1"},
					{"hello2", "world2"},
					{"interesting", "world"},
				},
			},
		},
		{
			t: tNone,
			q: query{
				key:      "hello2",
				opts:     []metaModel.OpOption{metaModel.WithRange("Z")},
				expected: []kv{},
			},
		},
		{
			t: tNone,
			q: query{
				key:  "hello",
				opts: []metaModel.OpOption{metaModel.WithPrefix()},
				expected: []kv{
					{"hello1", "world1"},
					{"hello2", "world2"},
				},
			},
		},
		{
			t: tNone,
			q: query{
				key:  "Hello",
				opts: []metaModel.OpOption{metaModel.WithFromKey()},
				expected: []kv{
					{"TiDB", "component"},
					{"dataflow", "engine"},
					{"hello1", "world1"},
					{"hello2", "world2"},
					{"interesting", "world"},
				},
			},
		},
		{
			t:    tDel,
			do:   kv{"hello", ""},
			opts: []metaModel.OpOption{metaModel.WithPrefix()},
			q: query{
				key:  "",
				opts: []metaModel.OpOption{metaModel.WithFromKey()},
				expected: []kv{
					{"TiDB", "component"},
					{"dataflow", "engine"},
					{"interesting", "world"},
				},
			},
		},
		{
			t:    tDel,
			do:   kv{"AZ", ""},
			opts: []metaModel.OpOption{metaModel.WithRange("Titan")},
			q: query{
				key:  "",
				opts: []metaModel.OpOption{metaModel.WithFromKey()},
				expected: []kv{
					{"dataflow", "engine"},
					{"interesting", "world"},
				},
			},
		},
		{
			t:    tDel,
			do:   kv{"egg", ""},
			opts: []metaModel.OpOption{metaModel.WithFromKey()},
			q: query{
				key:  "",
				opts: []metaModel.OpOption{metaModel.WithFromKey()},
				expected: []kv{
					{"dataflow", "engine"},
				},
			},
		},
	}

	// test get key range(WithRange/WithPrefix/WithFromKey)
	prepareData(ctx, t, cli, input)
	testAction(ctx, t, cli, actions)

	cli.Close()
}

func (suite *SuiteTestEtcd) TestTxn() {
	conf := &metaModel.StoreConfig{
		Endpoints: []string{suite.endpoints},
		StoreType: metaModel.StoreTypeEtcd,
	}
	t := suite.T()
	cc, err := NewEtcdClient(conf)
	require.Nil(t, err)
	cli, err := NewEtcdKVClientImpl(cc)
	require.Nil(t, err)
	defer cli.Close()
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
			// etcd forbits same key op intersect(put/delete) in txn to avoid quadratic blowup??
			err: errors.ErrMetaOpFail, // [TODO] check the detail error
			acts: []action{
				{
					t:    tGet,
					do:   kv{"hello", ""},
					opts: []metaModel.OpOption{},
				},
				{
					t:    tGet,
					do:   kv{"hello", ""},
					opts: []metaModel.OpOption{metaModel.WithPrefix()},
				},
				{
					t:    tPut,
					do:   kv{"hello", "world"},
					opts: []metaModel.OpOption{},
				},
				{
					t:    tDel,
					do:   kv{"hello", ""},
					opts: []metaModel.OpOption{},
				},
				{
					t:    tGet,
					do:   kv{"hello", ""},
					opts: []metaModel.OpOption{metaModel.WithFromKey()},
				},
			},
		},
		{
			acts: []action{
				{
					t:    tGet,
					do:   kv{"hello", ""},
					opts: []metaModel.OpOption{},
					q: query{
						expected: []kv{},
					},
				},
				{
					t:    tGet,
					do:   kv{"hell", ""},
					opts: []metaModel.OpOption{metaModel.WithPrefix()},
					q: query{
						expected: []kv{
							{"hello1", "world1"},
							{"hello2", "world2"},
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
				{
					t:    tDel,
					do:   kv{"int", ""},
					opts: []metaModel.OpOption{metaModel.WithPrefix()},
					q: query{
						expected: []kv{},
					},
				},
				{
					t:    tGet,
					do:   kv{"", ""},
					opts: []metaModel.OpOption{metaModel.WithFromKey()},
					q: query{
						expected: []kv{
							{"TiDB", "component"},
							{"dataflow", "engine"},
							{"dataflow2", "engine2"},
							{"hello1", "world1"},
							{"hello2", "world2"},
							{"hello3", "world3"},
						},
					},
				},
			},
		},
	}

	prepareData(ctx, t, cli, input)
	testTxnAction(ctx, t, cli, txns)

	cli.Close()
}

func (suite *SuiteTestEtcd) TestGenEpoch() {
	conf := &metaModel.StoreConfig{
		Endpoints: []string{suite.endpoints},
	}
	t := suite.T()
	cc, err := NewEtcdClient(conf)
	require.Nil(t, err)
	cli, err := NewEtcdKVClientImpl(cc)
	require.Nil(t, err)
	defer cli.Close()
	testGenerator(t, cli)
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

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestEtcdSuite(t *testing.T) {
	suite.Run(t, new(SuiteTestEtcd))
}
