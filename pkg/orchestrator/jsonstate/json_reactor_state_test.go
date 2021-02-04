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

package jsonstate

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/log"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/orchestrator"
	"github.com/pingcap/ticdc/pkg/util/testleak"
	"github.com/prometheus/client_golang/prometheus"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	testEtcdKeyPrefix = "/cdc_etcd_worker_test"
)

func Test(t *testing.T) { check.TestingT(t) }

var _ = check.Suite(&jsonReactorStateSuite{})

type jsonReactorStateSuite struct {
}

type simpleJSONRecord struct {
	A int `json:"a"`
	B int `json:"b"`
	C int `json:"c"`
}

type simpleJSONReactor struct {
	state  *JSONReactorState
	oldVal int
	id     int
}

func (r *simpleJSONReactor) Tick(_ context.Context, state orchestrator.ReactorState) (nextState orchestrator.ReactorState, err error) {
	if r.oldVal >= 100 {
		return r.state, cerrors.ErrReactorFinished
	}
	newState := state.(*JSONReactorState)
	r.state = newState

	snapshot := r.state.Inner().(*simpleJSONRecord)
	oldVal := 0
	switch r.id {
	case 0:
		oldVal = snapshot.A
		r.state.AddUpdateFunc(func(data interface{}) (newData interface{}, err error) {
			data.(*simpleJSONRecord).A++
			return data, nil
		})
	case 1:
		oldVal = snapshot.B
		r.state.AddUpdateFunc(func(data interface{}) (newData interface{}, err error) {
			data.(*simpleJSONRecord).B++
			return data, nil
		})
	case 2:
		oldVal = snapshot.C
		r.state.AddUpdateFunc(func(data interface{}) (newData interface{}, err error) {
			data.(*simpleJSONRecord).C++
			return data, nil
		})
	}
	if r.oldVal != oldVal {
		log.Panic("validation failed", zap.Int("id", r.id), zap.Int("expected", r.oldVal), zap.Int("actual", oldVal))
	}
	r.oldVal++
	return r.state, nil
}

func (s *jsonReactorStateSuite) TestSimpleJSONRecord(c *check.C) {
	defer testleak.AfterTest(c)()
	dir := c.MkDir()
	url, etcdServer, err := etcd.SetupEmbedEtcd(dir)
	c.Assert(err, check.IsNil)
	defer etcdServer.Close()

	newClient := func() *etcd.Client {
		rawCli, err := clientv3.NewFromURLs([]string{url.String()})
		c.Check(err, check.IsNil)
		return etcd.Wrap(rawCli, map[string]prometheus.Counter{})
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*1)
	defer cancel()

	cli := newClient()
	_, err = cli.Put(ctx, testEtcdKeyPrefix+"/json", `{"a": 0, "b": 0, "c": 0}`)
	c.Assert(err, check.IsNil)

	errg, ctx := errgroup.WithContext(ctx)
	for i := 0; i < 3; i++ {
		reactor := &simpleJSONReactor{
			state:  nil,
			oldVal: 0,
			id:     i,
		}

		initState, err := NewJSONReactorState(testEtcdKeyPrefix+"/json", &simpleJSONRecord{})
		c.Assert(err, check.IsNil)

		etcdWorker, err := orchestrator.NewEtcdWorker(newClient(), testEtcdKeyPrefix, reactor, initState)
		c.Assert(err, check.IsNil)

		errg.Go(func() error {
			err := etcdWorker.Run(ctx, nil, 10*time.Millisecond)
			if err != nil {
				log.Error("etcdWorker returned error", zap.Error(err))
			}
			return err
		})
	}

	err = errg.Wait()
	c.Assert(err, check.IsNil)
}

func (s *jsonReactorStateSuite) TestNotPointerError(c *check.C) {
	defer testleak.AfterTest(c)()

	_, err := NewJSONReactorState("/json", simpleJSONRecord{})
	c.Assert(err, check.NotNil)
}
