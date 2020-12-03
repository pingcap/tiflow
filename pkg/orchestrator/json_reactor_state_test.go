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
	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus"
	"go.etcd.io/etcd/clientv3"
	"golang.org/x/sync/errgroup"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/pkg/etcd"
	"go.uber.org/zap"
)

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

func (r *simpleJSONReactor) Tick(_ context.Context, state ReactorState) (nextState ReactorState, err error) {
	if r.oldVal >= 100 {
		return nil, ErrReactorFinished
	}
	newState := state.(*JSONReactorState)
	r.state = newState

	snapshot := r.state.Inner().(*simpleJSONRecord)
	oldVal := 0
	switch r.id {
	case 0:
		oldVal = snapshot.A
		snapshot.A = r.oldVal + 1
	case 1:
		oldVal = snapshot.B
		snapshot.B = r.oldVal + 1
	case 2:
		oldVal = snapshot.C
		snapshot.C = r.oldVal + 1
	}
	if r.oldVal != oldVal {
		log.Panic("validation failed", zap.Int("id", r.id), zap.Int("expected", r.oldVal), zap.Int("actual", oldVal))
	}
	r.oldVal++
	return r.state, nil
}

func (s *jsonReactorStateSuite) TestSimpleJSONRecord(c *check.C) {
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

		initState, err := NewJSONReactorState("/json", &simpleJSONRecord{})
		c.Assert(err, check.IsNil)

		etcdWorker, err := NewEtcdWorker(newClient(), testEtcdKeyPrefix, reactor, initState)
		c.Assert(err, check.IsNil)

		errg.Go(func() error {
			err := etcdWorker.Run(ctx, 10*time.Millisecond)
			if err != nil {
				log.Error("etcdWorker returned error", zap.Error(err))
			}
			return err
		})
	}

	err = errg.Wait()
	c.Assert(err, check.IsNil)
}
