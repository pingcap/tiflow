// Copyright 2020 PingCAP, Inc.
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
	"encoding/json"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/orchestrator/util"
	"github.com/pingcap/ticdc/pkg/util/testleak"
	"github.com/prometheus/client_golang/prometheus"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	testEtcdKeyPrefix    = "/cdc_etcd_worker_test"
	numGroups            = 10
	numValuesPerGroup    = 5
	totalTicksPerReactor = 1000
)

func Test(t *testing.T) { check.TestingT(t) }

var _ = check.Suite(&etcdWorkerSuite{})

type etcdWorkerSuite struct {
}

type simpleReactor struct {
	state     *simpleReactorState
	tickCount int
	id        int
}

func (s *simpleReactor) Tick(_ context.Context, state ReactorState) (nextState ReactorState, err error) {
	if s.tickCount >= totalTicksPerReactor {
		return s.state, cerrors.ErrReactorFinished
	}
	s.tickCount++

	newState := state.(*simpleReactorState)
	if newState == nil {
		return s.state, nil
	}
	s.state = newState

	if s.id == 0 {
		sum := s.state.sum
		for _, delta := range s.state.deltas {
			sum = sum - delta.old
			sum = sum + delta.new
		}

		// check for consistency
		expectedSum := 0
		for i := range s.state.values {
			for j := range s.state.values[i] {
				expectedSum += s.state.values[i][j]
			}
		}
		if sum != expectedSum {
			log.Panic("state is inconsistent", zap.Int("expected-sum", sum), zap.Int("actual-sum", s.state.sum))
		}

		s.state.SetSum(sum)
	} else {
		i2 := s.id - 1
		for i := range s.state.values {
			s.state.Inc(i, i2)
		}
	}

	s.state.deltas = s.state.deltas[:0]

	return s.state, nil
}

type delta struct {
	old int
	new int
	i1  int
	i2  int
}

type simpleReactorState struct {
	values  [][]int
	sum     int
	deltas  []*delta
	patches []*DataPatch
}

var keyParseRegexp = regexp.MustCompile(regexp.QuoteMeta(testEtcdKeyPrefix) + `/(.+)`)

func (s *simpleReactorState) Get(i1, i2 int) int {
	return s.values[i1][i2]
}

func (s *simpleReactorState) Inc(i1, i2 int) {
	patch := &DataPatch{
		Key: util.NewEtcdKey(testEtcdKeyPrefix + "/" + strconv.Itoa(i1)),
		Fun: func(old []byte) ([]byte, error) {
			var oldJSON []int
			err := json.Unmarshal(old, &oldJSON)
			if err != nil {
				return nil, errors.Trace(err)
			}

			oldJSON[i2]++
			return json.Marshal(oldJSON)
		},
	}

	s.patches = append(s.patches, patch)
}

func (s *simpleReactorState) SetSum(sum int) {
	patch := &DataPatch{
		Key: util.NewEtcdKey(testEtcdKeyPrefix + "/sum"),
		Fun: func(_ []byte) ([]byte, error) {
			return []byte(strconv.Itoa(sum)), nil
		},
	}

	s.patches = append(s.patches, patch)
}

func (s *simpleReactorState) Update(key util.EtcdKey, value []byte, isInit bool) error {
	subMatches := keyParseRegexp.FindSubmatch(key.Bytes())
	if len(subMatches) != 2 {
		log.Panic("illegal Etcd key", zap.ByteString("key", key.Bytes()))
	}

	if string(subMatches[1]) == "sum" {
		newSum, err := strconv.Atoi(string(value))
		if err != nil {
			log.Panic("illegal sum", zap.Error(err))
		}
		s.sum = newSum
		return nil
	}

	index, err := strconv.Atoi(string(subMatches[1]))
	if err != nil {
		log.Panic("illegal index", zap.Error(err))
	}

	var newValues []int
	err = json.Unmarshal(value, &newValues)
	if err != nil {
		log.Panic("illegal value", zap.Error(err))
	}

	for i2, v := range s.values[index] {
		if v != newValues[i2] {
			s.deltas = append(s.deltas, &delta{
				old: v,
				new: newValues[i2],
				i1:  index,
				i2:  i2,
			})
		}
	}

	s.values[index] = newValues
	return nil
}

func (s *simpleReactorState) GetPatches() []*DataPatch {
	ret := s.patches
	s.patches = nil
	return ret
}

func setUpTest(c *check.C) (func() *etcd.Client, func()) {
	dir := c.MkDir()
	url, server, err := etcd.SetupEmbedEtcd(dir)
	c.Assert(err, check.IsNil)
	endpoints := []string{url.String()}
	return func() *etcd.Client {
			rawCli, err := clientv3.NewFromURLs(endpoints)
			c.Check(err, check.IsNil)
			return etcd.Wrap(rawCli, map[string]prometheus.Counter{})
		}, func() {
			server.Close()
		}
}

func (s *etcdWorkerSuite) TestEtcdSum(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancel()

	newClient, closer := setUpTest(c)
	defer closer()

	cli := newClient()
	defer func() {
		_ = cli.Unwrap().Close()
	}()

	_, err := cli.Put(ctx, testEtcdKeyPrefix+"/sum", "0")
	c.Check(err, check.IsNil)

	initArray := make([]int, numValuesPerGroup)
	jsonStr, err := json.Marshal(initArray)
	c.Check(err, check.IsNil)

	for i := 0; i < numGroups; i++ {
		_, err := cli.Put(ctx, testEtcdKeyPrefix+"/"+strconv.Itoa(i), string(jsonStr))
		c.Check(err, check.IsNil)
	}

	errg, ctx := errgroup.WithContext(ctx)
	for i := 0; i < numValuesPerGroup+1; i++ {
		finalI := i
		errg.Go(func() error {
			values := make([][]int, numGroups)
			for j := range values {
				values[j] = make([]int, numValuesPerGroup)
			}

			reactor := &simpleReactor{
				state: nil,
				id:    finalI,
			}

			initState := &simpleReactorState{
				values:  values,
				sum:     0,
				deltas:  nil,
				patches: nil,
			}

			cli := newClient()
			defer func() {
				_ = cli.Unwrap().Close()
			}()

			etcdWorker, err := NewEtcdWorker(cli, testEtcdKeyPrefix, reactor, initState)
			if err != nil {
				return errors.Trace(err)
			}

			return errors.Trace(etcdWorker.Run(ctx, nil, 10*time.Millisecond))
		})
	}

	err = errg.Wait()
	if err != nil && (errors.Cause(err) == context.DeadlineExceeded || errors.Cause(err) == context.Canceled) {
		return
	}
	c.Check(err, check.IsNil)
}

type intReactorState struct {
	val       int
	isUpdated bool
}

func (s *intReactorState) Update(key util.EtcdKey, value []byte, isInit bool) error {
	var err error
	s.val, err = strconv.Atoi(string(value))
	if err != nil {
		log.Panic("intReactorState", zap.Error(err))
	}
	s.isUpdated = !isInit
	return nil
}

func (s *intReactorState) GetPatches() []*DataPatch {
	return []*DataPatch{}
}

type linearizabilityReactor struct {
	state    *intReactorState
	expected int
}

func (r *linearizabilityReactor) Tick(ctx context.Context, state ReactorState) (nextState ReactorState, err error) {
	r.state = state.(*intReactorState)
	if r.state.isUpdated {
		if r.state.val != r.expected {
			log.Panic("linearizability check failed", zap.Int("expected", r.expected), zap.Int("actual", r.state.val))
		}
		r.expected++
	}
	if r.state.val == 1999 {
		return r.state, cerrors.ErrReactorFinished
	}
	r.state.isUpdated = false
	return r.state, nil
}

func (s *etcdWorkerSuite) TestLinearizability(c *check.C) {
	defer testleak.AfterTest(c)()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancel()

	newClient, closer := setUpTest(c)
	defer closer()

	cli0 := newClient()
	cli := newClient()
	for i := 0; i < 1000; i++ {
		_, err := cli.Put(ctx, testEtcdKeyPrefix+"/lin", strconv.Itoa(i))
		c.Assert(err, check.IsNil)
	}

	reactor, err := NewEtcdWorker(cli0, testEtcdKeyPrefix+"/lin", &linearizabilityReactor{
		state:    nil,
		expected: 999,
	}, &intReactorState{
		val:       0,
		isUpdated: false,
	})
	c.Assert(err, check.IsNil)
	errg := &errgroup.Group{}
	errg.Go(func() error {
		return reactor.Run(ctx, nil, 10*time.Millisecond)
	})

	time.Sleep(500 * time.Millisecond)
	for i := 999; i < 2000; i++ {
		_, err := cli.Put(ctx, testEtcdKeyPrefix+"/lin", strconv.Itoa(i))
		c.Assert(err, check.IsNil)
	}

	err = errg.Wait()
	c.Assert(err, check.IsNil)

	err = cli.Unwrap().Close()
	c.Assert(err, check.IsNil)
	err = cli0.Unwrap().Close()
	c.Assert(err, check.IsNil)
}

type commonReactorState struct {
	state          map[string]string
	pendingPatches []*DataPatch
}

func (s *commonReactorState) Update(key util.EtcdKey, value []byte, isInit bool) error {
	s.state[key.String()] = string(value)
	return nil
}

func (s *commonReactorState) AppendPatch(key util.EtcdKey, fun PatchFunc) {
	s.pendingPatches = append(s.pendingPatches, &DataPatch{
		Key: key,
		Fun: fun,
	})
}

func (s *commonReactorState) GetPatches() []*DataPatch {
	pendingPatches := s.pendingPatches
	s.pendingPatches = nil
	return pendingPatches
}

type finishedReactor struct {
	state   *commonReactorState
	tickNum int
	prefix  string
}

func (r *finishedReactor) Tick(ctx context.Context, state ReactorState) (nextState ReactorState, err error) {
	r.state = state.(*commonReactorState)
	if r.tickNum < 2 {
		r.state.AppendPatch(util.NewEtcdKey(r.prefix+"/key1"), func(old []byte) (newValue []byte, err error) {
			newValue = append(old, []byte("abc")...)
			return
		})
		r.state.AppendPatch(util.NewEtcdKey(r.prefix+"/key2"), func(old []byte) (newValue []byte, err error) {
			newValue = append(old, []byte("123")...)
			return
		})
		r.tickNum++
		return r.state, nil
	}
	r.state.AppendPatch(util.NewEtcdKey(r.prefix+"/key1"), func(old []byte) (newValue []byte, err error) {
		newValue = append(old, []byte("fin")...)
		return
	})
	r.state.AppendPatch(util.NewEtcdKey(r.prefix+"/key2"), func(old []byte) (newValue []byte, err error) {
		return nil, nil
	})
	return r.state, cerrors.ErrReactorFinished
}

func (s *etcdWorkerSuite) TestFinished(c *check.C) {
	defer testleak.AfterTest(c)()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancel()

	newClient, closer := setUpTest(c)
	defer closer()

	cli := newClient()
	prefix := testEtcdKeyPrefix + "/finished"
	reactor, err := NewEtcdWorker(cli, prefix, &finishedReactor{
		prefix: prefix,
	}, &commonReactorState{
		state: make(map[string]string),
	})
	c.Assert(err, check.IsNil)
	err = reactor.Run(ctx, nil, 10*time.Millisecond)
	c.Assert(err, check.IsNil)
	resp, err := cli.Get(ctx, prefix+"/key1")
	c.Assert(err, check.IsNil)
	c.Assert(string(resp.Kvs[0].Key), check.Equals, "/cdc_etcd_worker_test/finished/key1")
	c.Assert(string(resp.Kvs[0].Value), check.Equals, "abcabcfin")
	resp, err = cli.Get(ctx, prefix+"/key2")
	c.Assert(err, check.IsNil)
	c.Assert(resp.Kvs, check.HasLen, 0)
	err = cli.Unwrap().Close()
	c.Assert(err, check.IsNil)
}

type coverReactor struct {
	state   *commonReactorState
	tickNum int
	prefix  string
}

func (r *coverReactor) Tick(ctx context.Context, state ReactorState) (nextState ReactorState, err error) {
	r.state = state.(*commonReactorState)
	if r.tickNum < 2 {
		r.state.AppendPatch(util.NewEtcdKey(r.prefix+"/key1"), func(old []byte) (newValue []byte, err error) {
			newValue = append(old, []byte("abc")...)
			return
		})
		r.state.AppendPatch(util.NewEtcdKey(r.prefix+"/key2"), func(old []byte) (newValue []byte, err error) {
			newValue = append(old, []byte("123")...)
			return
		})
		r.state.AppendPatch(util.NewEtcdKey(r.prefix+"/key1"), func(old []byte) (newValue []byte, err error) {
			newValue = append(old, []byte("cba")...)
			return
		})
		r.state.AppendPatch(util.NewEtcdKey(r.prefix+"/key2"), func(old []byte) (newValue []byte, err error) {
			newValue = append(old, []byte("321")...)
			return
		})
		r.tickNum++
		return r.state, nil
	}
	r.state.AppendPatch(util.NewEtcdKey(r.prefix+"/key1"), func(old []byte) (newValue []byte, err error) {
		newValue = append(old, []byte("fin")...)
		return
	})
	r.state.AppendPatch(util.NewEtcdKey(r.prefix+"/key1"), func(old []byte) (newValue []byte, err error) {
		newValue = append(old, []byte("fin")...)
		return
	})
	r.state.AppendPatch(util.NewEtcdKey(r.prefix+"/key2"), func(old []byte) (newValue []byte, err error) {
		return nil, nil
	})
	r.state.AppendPatch(util.NewEtcdKey(r.prefix+"/key2"), func(old []byte) (newValue []byte, err error) {
		newValue = append(old, []byte("fin")...)
		return
	})
	return r.state, cerrors.ErrReactorFinished
}

func (s *etcdWorkerSuite) TestCover(c *check.C) {
	defer testleak.AfterTest(c)()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancel()

	newClient, closer := setUpTest(c)
	defer closer()

	cli := newClient()
	prefix := testEtcdKeyPrefix + "/cover"
	reactor, err := NewEtcdWorker(cli, prefix, &coverReactor{
		prefix: prefix,
	}, &commonReactorState{
		state: make(map[string]string),
	})
	c.Assert(err, check.IsNil)
	err = reactor.Run(ctx, nil, 10*time.Millisecond)
	c.Assert(err, check.IsNil)
	resp, err := cli.Get(ctx, prefix+"/key1")
	c.Assert(err, check.IsNil)
	c.Assert(string(resp.Kvs[0].Key), check.Equals, "/cdc_etcd_worker_test/cover/key1")
	c.Assert(string(resp.Kvs[0].Value), check.Equals, "abccbaabccbafinfin")
	resp, err = cli.Get(ctx, prefix+"/key2")
	c.Assert(err, check.IsNil)
	c.Assert(string(resp.Kvs[0].Key), check.Equals, "/cdc_etcd_worker_test/cover/key2")
	c.Assert(string(resp.Kvs[0].Value), check.Equals, "fin")
	err = cli.Unwrap().Close()
	c.Assert(err, check.IsNil)
}

type emptyTxnReactor struct {
	state   *commonReactorState
	tickNum int
	prefix  string
	cli     *etcd.Client
}

func (r *emptyTxnReactor) Tick(ctx context.Context, state ReactorState) (nextState ReactorState, err error) {
	r.state = state.(*commonReactorState)
	if r.tickNum == 0 {
		r.state.AppendPatch(util.NewEtcdKey(r.prefix+"/key1"), func(old []byte) (newValue []byte, err error) {
			return []byte("abc"), nil
		})
		r.state.AppendPatch(util.NewEtcdKey(r.prefix+"/key2"), func(old []byte) (newValue []byte, err error) {
			return []byte("123"), nil
		})
		r.state.AppendPatch(util.NewEtcdKey(r.prefix+"/key1"), func(old []byte) (newValue []byte, err error) {
			return nil, nil
		})
		r.tickNum++
		return r.state, nil
	}
	if r.tickNum == 1 {
		// Simulating other client writes
		_, err := r.cli.Put(ctx, "/key3", "123")
		if err != nil {
			return nil, errors.Trace(err)
		}

		r.state.AppendPatch(util.NewEtcdKey(r.prefix+"/key2"), func(old []byte) (newValue []byte, err error) {
			return []byte("123"), nil
		})
		r.state.AppendPatch(util.NewEtcdKey(r.prefix+"/key1"), func(old []byte) (newValue []byte, err error) {
			return nil, nil
		})
		r.tickNum++
		return r.state, nil
	}
	r.state.AppendPatch(util.NewEtcdKey(r.prefix+"/key1"), func(old []byte) (newValue []byte, err error) {
		return nil, nil
	})
	r.state.AppendPatch(util.NewEtcdKey(r.prefix+"/key2"), func(old []byte) (newValue []byte, err error) {
		return []byte("123"), nil
	})
	return r.state, cerrors.ErrReactorFinished
}

func (s *etcdWorkerSuite) TestEmptyTxn(c *check.C) {
	defer testleak.AfterTest(c)()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancel()

	newClient, closer := setUpTest(c)
	defer closer()

	cli := newClient()
	prefix := testEtcdKeyPrefix + "/empty_txn"
	reactor, err := NewEtcdWorker(cli, prefix, &emptyTxnReactor{
		prefix: prefix,
		cli:    cli,
	}, &commonReactorState{
		state: make(map[string]string),
	})
	c.Assert(err, check.IsNil)
	err = reactor.Run(ctx, nil, 10*time.Millisecond)
	c.Assert(err, check.IsNil)
	resp, err := cli.Get(ctx, prefix+"/key1")
	c.Assert(err, check.IsNil)
	c.Assert(resp.Kvs, check.HasLen, 0)
	resp, err = cli.Get(ctx, prefix+"/key2")
	c.Assert(err, check.IsNil)
	c.Assert(string(resp.Kvs[0].Key), check.Equals, "/cdc_etcd_worker_test/empty_txn/key2")
	c.Assert(string(resp.Kvs[0].Value), check.Equals, "123")
	err = cli.Unwrap().Close()
	c.Assert(err, check.IsNil)
}

type emptyOrNilReactor struct {
	state   *commonReactorState
	tickNum int
	prefix  string
}

func (r *emptyOrNilReactor) Tick(ctx context.Context, state ReactorState) (nextState ReactorState, err error) {
	r.state = state.(*commonReactorState)
	if r.tickNum == 0 {
		r.state.AppendPatch(util.NewEtcdKey(r.prefix+"/key1"), func(old []byte) (newValue []byte, err error) {
			return []byte(""), nil
		})
		r.state.AppendPatch(util.NewEtcdKey(r.prefix+"/key2"), func(old []byte) (newValue []byte, err error) {
			return nil, nil
		})
		r.tickNum++
		return r.state, nil
	}
	if r.tickNum == 1 {
		r.state.AppendPatch(util.NewEtcdKey(r.prefix+"/key1"), func(old []byte) (newValue []byte, err error) {
			return nil, nil
		})
		r.state.AppendPatch(util.NewEtcdKey(r.prefix+"/key2"), func(old []byte) (newValue []byte, err error) {
			return []byte(""), nil
		})
		r.tickNum++
		return r.state, nil
	}
	r.state.AppendPatch(util.NewEtcdKey(r.prefix+"/key1"), func(old []byte) (newValue []byte, err error) {
		return []byte(""), nil
	})
	r.state.AppendPatch(util.NewEtcdKey(r.prefix+"/key2"), func(old []byte) (newValue []byte, err error) {
		return nil, nil
	})
	return r.state, cerrors.ErrReactorFinished
}

func (s *etcdWorkerSuite) TestEmptyOrNil(c *check.C) {
	defer testleak.AfterTest(c)()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancel()

	newClient, closer := setUpTest(c)
	defer closer()

	cli := newClient()
	prefix := testEtcdKeyPrefix + "/emptyOrNil"
	reactor, err := NewEtcdWorker(cli, prefix, &emptyOrNilReactor{
		prefix: prefix,
	}, &commonReactorState{
		state: make(map[string]string),
	})
	c.Assert(err, check.IsNil)
	err = reactor.Run(ctx, nil, 10*time.Millisecond)
	c.Assert(err, check.IsNil)
	resp, err := cli.Get(ctx, prefix+"/key1")
	c.Assert(err, check.IsNil)
	c.Assert(string(resp.Kvs[0].Key), check.Equals, "/cdc_etcd_worker_test/emptyOrNil/key1")
	c.Assert(string(resp.Kvs[0].Value), check.Equals, "")
	resp, err = cli.Get(ctx, prefix+"/key2")
	c.Assert(err, check.IsNil)
	c.Assert(resp.Kvs, check.HasLen, 0)
	err = cli.Unwrap().Close()
	c.Assert(err, check.IsNil)
}

func (s *etcdWorkerSuite) TestMergePatches(c *check.C) {
	defer testleak.AfterTest(c)()
	testCases := []struct {
		state   map[util.EtcdKey][]byte
		patches []*DataPatch
	}{
		{
			state:   map[util.EtcdKey][]byte{},
			patches: []*DataPatch{},
		},
		{
			state: map[util.EtcdKey][]byte{
				util.NewEtcdKey("key1"): []byte("aa"),
			},
			patches: []*DataPatch{
				{
					Key: util.NewEtcdKey("key1"),
					Fun: func(old []byte) (newValue []byte, err error) {
						newValue = append(old, []byte("bb")...)
						return
					},
				},
				{
					Key: util.NewEtcdKey("key1"),
					Fun: func(old []byte) (newValue []byte, err error) {
						newValue = append(old, []byte("cc")...)
						return
					},
				},
				{
					Key: util.NewEtcdKey("key1"),
					Fun: func(old []byte) (newValue []byte, err error) {
						newValue = append(old, []byte("dd")...)
						return
					},
				},
			},
		},
		{
			state: map[util.EtcdKey][]byte{
				util.NewEtcdKey("key1"): []byte("aa"),
			},
			patches: []*DataPatch{
				{
					Key: util.NewEtcdKey("key1"),
					Fun: func(old []byte) (newValue []byte, err error) {
						newValue = append(old, []byte("bb")...)
						return
					},
				},
				{
					Key: util.NewEtcdKey("key2"),
					Fun: func(old []byte) (newValue []byte, err error) {
						newValue = append(old, []byte("cc")...)
						return
					},
				},
				{
					Key: util.NewEtcdKey("key1"),
					Fun: func(old []byte) (newValue []byte, err error) {
						newValue = append(old, []byte("dd")...)
						return
					},
				},
				{
					Key: util.NewEtcdKey("key2"),
					Fun: func(old []byte) (newValue []byte, err error) {
						newValue = append(old, []byte("ee")...)
						return
					},
				},
			},
		},
		{
			state: map[util.EtcdKey][]byte{
				util.NewEtcdKey("key1"): []byte("aa"),
			},
			patches: []*DataPatch{
				{
					Key: util.NewEtcdKey("key1"),
					Fun: func(old []byte) (newValue []byte, err error) {
						newValue = append(old, []byte("bb")...)
						err = cerrors.ErrEtcdIgnore
						return
					},
				},
				{
					Key: util.NewEtcdKey("key2"),
					Fun: func(old []byte) (newValue []byte, err error) {
						newValue = append(old, []byte("cc")...)
						return
					},
				},
				{
					Key: util.NewEtcdKey("key1"),
					Fun: func(old []byte) (newValue []byte, err error) {
						newValue = append(old, []byte("dd")...)
						return
					},
				},
				{
					Key: util.NewEtcdKey("key2"),
					Fun: func(old []byte) (newValue []byte, err error) {
						newValue = append(old, []byte("ee")...)
						err = cerrors.ErrEtcdIgnore
						return
					},
				},
			},
		},
	}

	applyPatches := func(state map[util.EtcdKey][]byte, patches []*DataPatch) map[util.EtcdKey][]byte {
		// clone state map
		clonedState := make(map[util.EtcdKey][]byte, len(state))
		for k, v := range state {
			clonedState[k] = v
		}
		// apply patches
		for _, p := range patches {
			newValue, err := p.Fun(clonedState[p.Key])
			if cerrors.ErrEtcdIgnore.Equal(errors.Cause(err)) {
				continue
			}
			c.Assert(err, check.IsNil)
			clonedState[p.Key] = newValue
		}
		return clonedState
	}
	for _, tc := range testCases {
		mergedPatches := mergePatch(tc.patches)
		c.Assert(applyPatches(tc.state, mergedPatches), check.DeepEquals, applyPatches(tc.state, tc.patches))
	}
}

func (s *etcdWorkerSuite) TestEtcdValueEqual(c *check.C) {
	defer testleak.AfterTest(c)()
	c.Assert(etcdValueEqual(nil, nil), check.IsTrue)
	c.Assert(etcdValueEqual(nil, []byte{}), check.IsFalse)
	c.Assert(etcdValueEqual([]byte{}, nil), check.IsFalse)
	c.Assert(etcdValueEqual([]byte{}, []byte{}), check.IsTrue)
	c.Assert(etcdValueEqual([]byte{11}, []byte{11}), check.IsTrue)
	c.Assert(etcdValueEqual([]byte{11}, []byte{12}), check.IsFalse)
}
