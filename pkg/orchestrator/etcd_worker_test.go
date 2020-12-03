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
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/prometheus/client_golang/prometheus"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	testEtcdKeyPrefix    = "/cdc_etcd_worker_test"
	numGroups            = 10
	numValuesPerGroup    = 5
	totalTicksPerReactor = 100
)

func Test(t *testing.T) { check.TestingT(t) }

var _ = check.Suite(&etcdWorkerSuite{})

type etcdWorkerSuite struct {
	etcdServer *embed.Etcd
	endpoints  []string
}

type simpleReactor struct {
	state     *simpleReactorState
	tickCount int
	id        int
}

func (s *simpleReactor) Tick(_ context.Context, state ReactorState) (nextState ReactorState, err error) {
	if s.tickCount >= totalTicksPerReactor {
		return nil, ErrReactorFinished
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

var (
	keyParseRegexp = regexp.MustCompile(`/(.+)`)
)

func (s *simpleReactorState) Get(i1, i2 int) int {
	return s.values[i1][i2]
}

func (s *simpleReactorState) Inc(i1, i2 int) {
	patch := &DataPatch{
		Key: []byte("/" + strconv.Itoa(i1)),
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
		Key: []byte("/sum"),
		Fun: func(_ []byte) ([]byte, error) {
			return []byte(strconv.Itoa(sum)), nil
		},
	}

	s.patches = append(s.patches, patch)
}

func (s *simpleReactorState) Update(key []byte, value []byte) {
	subMatches := keyParseRegexp.FindSubmatch(key)
	if len(subMatches) != 2 {
		log.Panic("illegal Etcd key", zap.ByteString("key", key))
	}

	if string(subMatches[1]) == "sum" {
		newSum, err := strconv.Atoi(string(value))
		if err != nil {
			log.Panic("illegal sum", zap.Error(err))
		}
		s.sum = newSum
		return
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
}

func (s *simpleReactorState) GetPatches() []*DataPatch {
	ret := s.patches
	s.patches = nil
	return ret
}

func (s *etcdWorkerSuite) SetUpTest(c *check.C) {
	dir := c.MkDir()
	url, etcdServer, err := etcd.SetupEmbedEtcd(dir)
	c.Assert(err, check.IsNil)
	s.etcdServer = etcdServer
	s.endpoints = []string{url.String()}

	log.Debug("Set up embedded etcd", zap.Strings("endpoints", s.endpoints))
}

func (s *etcdWorkerSuite) TestEtcdSum(c *check.C) {
	newClient := func() *etcd.Client {
		rawCli, err := clientv3.NewFromURLs(s.endpoints)
		c.Check(err, check.IsNil)
		return etcd.Wrap(rawCli, map[string]prometheus.Counter{})
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancel()

	cli := newClient()
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

			etcdWorker, err := NewEtcdWorker(newClient(), testEtcdKeyPrefix, reactor, initState)
			if err != nil {
				return errors.Trace(err)
			}

			return errors.Trace(etcdWorker.Run(ctx, 10*time.Millisecond))
		})
	}

	err = errg.Wait()
	if err != nil && (errors.Cause(err) == context.DeadlineExceeded || errors.Cause(err) == context.Canceled) {
		return
	}
	c.Check(err, check.IsNil)

}
