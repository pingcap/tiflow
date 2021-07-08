// Copyright 2021 PingCAP, Inc.
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

package orchestrator

import (
	"context"
	"strconv"
	"sync"
	"time"

	"go.etcd.io/etcd/clientv3/concurrency"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/orchestrator/util"
	"github.com/pingcap/ticdc/pkg/util/testleak"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap/zapcore"
)

var _ = check.SerialSuites(&etcdBigTxnSuite{})

type etcdBigTxnSuite struct {
}

type bigTxnReactor struct {
	state     *commonReactorState
	tickCount int
}

const (
	testBigTxnOpCount = 256
)

func (r *bigTxnReactor) Tick(ctx context.Context, state ReactorState) (nextState ReactorState, err error) {
	r.state = state.(*commonReactorState)
	if r.tickCount == 1 {
		return r.state, cerrors.ErrReactorFinished.GenWithStackByArgs()
	}
	r.tickCount++
	for i := 0; i < testBigTxnOpCount; i++ {
		r.state.AppendPatch(util.NewEtcdKey(testEtcdKeyPrefix+"/big_txn/"+strconv.Itoa(i)), func(old []byte) (newValue []byte, changed bool, err error) {
			return append(old, []byte("def")...), true, nil
		})
	}
	return r.state, nil
}

func initKV(ctx context.Context, cli *etcd.Client) error {
	for i := 0; i < testBigTxnOpCount; i++ {
		_, err := cli.Put(ctx, testEtcdKeyPrefix+"/big_txn/"+strconv.Itoa(i), "abc")
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func assertNoLockRemains(ctx context.Context, cli *etcd.Client, c *check.C) {
	resp, err := cli.Get(ctx, testEtcdKeyPrefix+"/big_txn"+etcdBigTxnLockPrefix, clientv3.WithPrefix())
	c.Assert(err, check.IsNil)
	c.Assert(resp.Count, check.Equals, int64(0))

	resp, err = cli.Get(ctx, testEtcdKeyPrefix+"/big_txn"+etcdBigTxnMetaPrefix, clientv3.WithPrefix())
	c.Assert(err, check.IsNil)
	c.Assert(resp.Count, check.Equals, int64(0))
}

func assertRolledForward(ctx context.Context, cli *etcd.Client, c *check.C) {
	for i := 0; i < testBigTxnOpCount; i++ {
		resp, err := cli.Get(ctx, testEtcdKeyPrefix+"/big_txn/"+strconv.Itoa(i))
		c.Assert(err, check.IsNil)
		c.Assert(resp.Count, check.Equals, int64(1))
		c.Assert(resp.Kvs[0].Value, check.BytesEquals, []byte("abcdef"))
	}
}

func assertRolledBack(ctx context.Context, cli *etcd.Client, c *check.C) {
	for i := 0; i < testBigTxnOpCount; i++ {
		resp, err := cli.Get(ctx, testEtcdKeyPrefix+"/big_txn/"+strconv.Itoa(i))
		c.Assert(err, check.IsNil)
		c.Assert(resp.Count, check.Equals, int64(1))
		c.Assert(resp.Kvs[0].Value, check.BytesEquals, []byte("abc"))
	}
}

type noopReactor struct{}

func (r *noopReactor) Tick(ctx context.Context, state ReactorState) (nextState ReactorState, err error) {
	return state, nil
}

func mockCleanUpByOthers(ctx context.Context, cli *etcd.Client, c *check.C) {
	defer cli.Unwrap().Close() //nolint:errcheck
	err := failpoint.Enable("github.com/pingcap/ticdc/pkg/orchestrator/forceRollForwardByOthers", "return(true)")
	c.Assert(err, check.IsNil)
	defer failpoint.Disable("github.com/pingcap/ticdc/pkg/orchestrator/forceRollForwardByOthers") //nolint:errcheck

	reactor, err := NewEtcdWorker(cli, testEtcdKeyPrefix+"/big_txn", &noopReactor{}, &commonReactorState{
		state: map[string]string{},
	})
	c.Assert(err, check.IsNil)

	ctx, cancel := context.WithTimeout(ctx, time.Second*4)
	defer cancel()
	err = reactor.Run(ctx, nil, time.Millisecond*100)
	c.Assert(err, check.ErrorMatches, ".*deadline.*")
}

func (s *etcdBigTxnSuite) TestBigTxnBasic(c *check.C) {
	defer testleak.AfterTest(c)()
	log.SetLevel(zapcore.DebugLevel)
	defer log.SetLevel(zapcore.InfoLevel)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancel()

	newClient, closer := setUpTest(c)
	defer closer()

	cli := newClient()
	defer cli.Unwrap().Close()

	err := initKV(ctx, cli)
	c.Assert(err, check.IsNil)

	prefix := testEtcdKeyPrefix + "/big_txn"
	initState := &commonReactorState{
		state: make(map[string]string),
	}
	reactor, err := NewEtcdWorker(cli, prefix, &bigTxnReactor{}, initState)
	c.Assert(err, check.IsNil)

	err = reactor.Run(ctx, nil, time.Millisecond*100)
	c.Assert(err, check.IsNil)

	assertNoLockRemains(ctx, cli, c)
	assertRolledForward(ctx, cli, c)
}

func (s *etcdBigTxnSuite) TestBigTxnErrorAfterPutMeta(c *check.C) {
	defer testleak.AfterTest(c)()
	log.SetLevel(zapcore.DebugLevel)
	defer log.SetLevel(zapcore.InfoLevel)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancel()

	newClient, closer := setUpTest(c)
	defer closer()

	cli := newClient()
	defer cli.Unwrap().Close()

	err := initKV(ctx, cli)
	c.Assert(err, check.IsNil)

	prefix := testEtcdKeyPrefix + "/big_txn"
	initState := &commonReactorState{
		state: make(map[string]string),
	}
	reactor, err := NewEtcdWorker(cli, prefix, &bigTxnReactor{}, initState)
	c.Assert(err, check.IsNil)

	err = failpoint.Enable("github.com/pingcap/ticdc/pkg/orchestrator/etcdBigTxnFailAfterPutMeta", "return(true)")
	c.Assert(err, check.IsNil)
	defer failpoint.Disable("github.com/pingcap/ticdc/pkg/orchestrator/etcdBigTxnFailAfterPutMeta") //nolint:errcheck
	err = reactor.Run(ctx, nil, time.Millisecond*100)
	c.Assert(err, check.ErrorMatches, ".*ErrEtcdMockCrash.*")

	assertNoLockRemains(ctx, cli, c)
	assertRolledBack(ctx, cli, c)
}

func (s *etcdBigTxnSuite) TestBigTxnErrorAfterPrewrite(c *check.C) {
	defer testleak.AfterTest(c)()
	log.SetLevel(zapcore.DebugLevel)
	defer log.SetLevel(zapcore.InfoLevel)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancel()

	newClient, closer := setUpTest(c)
	defer closer()

	cli := newClient()
	defer cli.Unwrap().Close()

	err := initKV(ctx, cli)
	c.Assert(err, check.IsNil)

	prefix := testEtcdKeyPrefix + "/big_txn"
	initState := &commonReactorState{
		state: make(map[string]string),
	}
	reactor, err := NewEtcdWorker(cli, prefix, &bigTxnReactor{}, initState)
	c.Assert(err, check.IsNil)

	err = failpoint.Enable("github.com/pingcap/ticdc/pkg/orchestrator/etcdBigTxnFailAfterPrewrite", "return(true)")
	c.Assert(err, check.IsNil)
	defer failpoint.Disable("github.com/pingcap/ticdc/pkg/orchestrator/etcdBigTxnFailAfterPrewrite") //nolint:errcheck
	err = reactor.Run(ctx, nil, time.Millisecond*100)
	c.Assert(err, check.ErrorMatches, ".*ErrEtcdMockCrash.*")

	assertNoLockRemains(ctx, cli, c)
	assertRolledBack(ctx, cli, c)
}

func (s *etcdBigTxnSuite) TestBigTxnErrorBeforeCommit(c *check.C) {
	defer testleak.AfterTest(c)()
	log.SetLevel(zapcore.DebugLevel)
	defer log.SetLevel(zapcore.InfoLevel)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancel()

	newClient, closer := setUpTest(c)
	defer closer()

	cli := newClient()
	defer cli.Unwrap().Close()

	err := initKV(ctx, cli)
	c.Assert(err, check.IsNil)

	prefix := testEtcdKeyPrefix + "/big_txn"
	initState := &commonReactorState{
		state: make(map[string]string),
	}
	reactor, err := NewEtcdWorker(cli, prefix, &bigTxnReactor{}, initState)
	c.Assert(err, check.IsNil)

	err = failpoint.Enable("github.com/pingcap/ticdc/pkg/orchestrator/etcdBigTxnFailBeforeCommit", "return(true)")
	c.Assert(err, check.IsNil)
	defer failpoint.Disable("github.com/pingcap/ticdc/pkg/orchestrator/etcdBigTxnFailBeforeCommit") //nolint:errcheck
	err = reactor.Run(ctx, nil, time.Millisecond*100)
	c.Assert(err, check.ErrorMatches, ".*ErrEtcdMockCrash.*")

	assertNoLockRemains(ctx, cli, c)
	assertRolledBack(ctx, cli, c)
}

func (s *etcdBigTxnSuite) TestBigTxnErrorAfterCommit(c *check.C) {
	defer testleak.AfterTest(c)()
	log.SetLevel(zapcore.DebugLevel)
	defer log.SetLevel(zapcore.InfoLevel)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancel()

	newClient, closer := setUpTest(c)
	defer closer()

	cli := newClient()
	defer cli.Unwrap().Close()

	err := initKV(ctx, cli)
	c.Assert(err, check.IsNil)

	prefix := testEtcdKeyPrefix + "/big_txn"
	initState := &commonReactorState{
		state: make(map[string]string),
	}
	reactor, err := NewEtcdWorker(cli, prefix, &bigTxnReactor{}, initState)
	c.Assert(err, check.IsNil)

	err = failpoint.Enable("github.com/pingcap/ticdc/pkg/orchestrator/etcdBigTxnFailAfterCommit", "return(true)")
	c.Assert(err, check.IsNil)
	defer failpoint.Disable("github.com/pingcap/ticdc/pkg/orchestrator/etcdBigTxnFailAfterCommit") //nolint:errcheck
	err = reactor.Run(ctx, nil, time.Millisecond*100)
	c.Assert(err, check.ErrorMatches, ".*ErrEtcdMockCrash.*")

	mockCleanUpByOthers(ctx, newClient(), c)
	assertNoLockRemains(ctx, cli, c)
	assertRolledForward(ctx, cli, c)
}

func (s *etcdBigTxnSuite) TestBigTxnCrashAfterPutMeta(c *check.C) {
	defer testleak.AfterTest(c)()
	log.SetLevel(zapcore.DebugLevel)
	defer log.SetLevel(zapcore.InfoLevel)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancel()

	newClient, closer := setUpTest(c)
	defer closer()

	cli := newClient()
	defer cli.Unwrap().Close()

	err := initKV(ctx, cli)
	c.Assert(err, check.IsNil)

	prefix := testEtcdKeyPrefix + "/big_txn"
	initState := &commonReactorState{
		state: make(map[string]string),
	}
	reactor, err := NewEtcdWorker(cli, prefix, &bigTxnReactor{}, initState)
	c.Assert(err, check.IsNil)

	var wg sync.WaitGroup
	err = failpoint.Enable("github.com/pingcap/ticdc/pkg/orchestrator/etcdBigTxnFailAfterPutMeta", "1*pause")
	c.Assert(err, check.IsNil)
	defer func() {
		failpoint.Disable("github.com/pingcap/ticdc/pkg/orchestrator/etcdBigTxnFailAfterPutMeta") //nolint:errcheck
		wg.Wait()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := reactor.Run(ctx, nil, time.Millisecond*100)
		c.Assert(err, check.ErrorMatches, ".*ErrEtcdMockCrash.*")
	}()

	time.Sleep(time.Second * 2)

	mockCleanUpByOthers(ctx, newClient(), c)
	assertNoLockRemains(ctx, cli, c)
	assertRolledBack(ctx, cli, c)
}

func (s *etcdBigTxnSuite) TestBigTxnCrashAfterPrewrite(c *check.C) {
	defer testleak.AfterTest(c)()
	log.SetLevel(zapcore.DebugLevel)
	defer log.SetLevel(zapcore.InfoLevel)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancel()

	newClient, closer := setUpTest(c)
	defer closer()

	cli := newClient()
	defer cli.Unwrap().Close()

	err := initKV(ctx, cli)
	c.Assert(err, check.IsNil)

	prefix := testEtcdKeyPrefix + "/big_txn"
	initState := &commonReactorState{
		state: make(map[string]string),
	}
	reactor, err := NewEtcdWorker(cli, prefix, &bigTxnReactor{}, initState)
	c.Assert(err, check.IsNil)

	var wg sync.WaitGroup
	err = failpoint.Enable("github.com/pingcap/ticdc/pkg/orchestrator/etcdBigTxnFailAfterPrewrite", "1*pause")
	c.Assert(err, check.IsNil)
	defer func() {
		failpoint.Disable("github.com/pingcap/ticdc/pkg/orchestrator/etcdBigTxnFailAfterPrewrite") //nolint:errcheck
		wg.Wait()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := reactor.Run(ctx, nil, time.Millisecond*100)
		c.Assert(err, check.ErrorMatches, ".*ErrEtcdMockCrash.*")
	}()

	time.Sleep(time.Second * 10)

	mockCleanUpByOthers(ctx, newClient(), c)
	assertNoLockRemains(ctx, cli, c)
	assertRolledBack(ctx, cli, c)
}

func (s *etcdBigTxnSuite) TestBigTxnCrashBeforeCommit(c *check.C) {
	defer testleak.AfterTest(c)()
	log.SetLevel(zapcore.DebugLevel)
	defer log.SetLevel(zapcore.InfoLevel)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancel()

	newClient, closer := setUpTest(c)
	defer closer()

	cli := newClient()
	defer cli.Unwrap().Close()

	err := initKV(ctx, cli)
	c.Assert(err, check.IsNil)

	prefix := testEtcdKeyPrefix + "/big_txn"
	initState := &commonReactorState{
		state: make(map[string]string),
	}
	reactor, err := NewEtcdWorker(cli, prefix, &bigTxnReactor{}, initState)
	c.Assert(err, check.IsNil)

	var wg sync.WaitGroup
	err = failpoint.Enable("github.com/pingcap/ticdc/pkg/orchestrator/etcdBigTxnFailBeforeCommit", "1*pause")
	c.Assert(err, check.IsNil)
	defer func() {
		failpoint.Disable("github.com/pingcap/ticdc/pkg/orchestrator/etcdBigTxnFailBeforeCommit") //nolint:errcheck
		wg.Wait()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := reactor.Run(ctx, nil, time.Millisecond*100)
		c.Assert(err, check.ErrorMatches, ".*ErrEtcdMockCrash.*")
	}()

	time.Sleep(time.Second * 20)

	mockCleanUpByOthers(ctx, newClient(), c)
	assertNoLockRemains(ctx, cli, c)
	assertRolledBack(ctx, cli, c)
}

func (s *etcdBigTxnSuite) TestBigTxnConflictWithSameClient(c *check.C) {
	defer testleak.AfterTest(c)()
	log.SetLevel(zapcore.DebugLevel)
	defer log.SetLevel(zapcore.InfoLevel)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancel()

	newClient, closer := setUpTest(c)
	defer closer()

	cli := newClient()
	defer cli.Unwrap().Close()

	err := initKV(ctx, cli)
	c.Assert(err, check.IsNil)

	prefix := testEtcdKeyPrefix + "/big_txn"
	worker1, err := NewEtcdWorker(cli, prefix, &bigTxnReactor{}, &commonReactorState{
		state: make(map[string]string),
	})
	c.Assert(err, check.IsNil)

	worker2, err := NewEtcdWorker(cli, prefix, &bigTxnReactor{}, &commonReactorState{
		state: make(map[string]string),
	})
	c.Assert(err, check.IsNil)

	err = failpoint.Enable("github.com/pingcap/ticdc/pkg/orchestrator/etcdBigTxnPauseAfterPutMeta", "2*sleep(1000)")
	c.Assert(err, check.IsNil)
	defer failpoint.Disable("github.com/pingcap/ticdc/pkg/orchestrator/etcdBigTxnPauseAfterPutMeta") //nolint:errcheck

	session, err := concurrency.NewSession(cli.Unwrap())
	c.Assert(err, check.IsNil)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		err := worker1.Run(ctx, session, time.Millisecond*100)
		c.Assert(err, check.IsNil)
	}()

	go func() {
		defer wg.Done()
		err := worker2.Run(ctx, session, time.Millisecond*100)
		c.Assert(err, check.IsNil)
	}()

	wg.Wait()

	assertNoLockRemains(ctx, cli, c)

	for i := 0; i < testBigTxnOpCount; i++ {
		resp, err := cli.Get(ctx, testEtcdKeyPrefix+"/big_txn/"+strconv.Itoa(i))
		c.Assert(err, check.IsNil)
		c.Assert(resp.Count, check.Equals, int64(1))
		c.Assert(resp.Kvs[0].Value, check.BytesEquals, []byte("abcdefdef"))
	}
}
