// Copyright 2019 PingCAP, Inc.
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

package master

import (
	"context"
	"time"
)

func (t *testMaster) TestAgentPool() {
	t.testPool()
	t.testEmit()
}

func (t *testMaster) testPool() {
	var (
		rate  = 10
		burst = 100
	)
	// test limit
	ap := NewAgentPool(&RateLimitConfig{rate: float64(rate), burst: burst})
	go ap.Start(context.Background())
	pc := make(chan *Agent)

	go func() {
		for i := 0; i < rate+burst; i++ {
			pc <- ap.Apply(context.Background(), i)
		}
	}()

	for i := 0; i < burst; i++ {
		agent := <-pc
		t.Require().Equal(i, agent.ID)
	}
	select {
	case <-pc:
		t.T().Error("should not get agent now")
	default:
	}

	for i := 0; i < rate; i++ {
		select {
		case agent := <-pc:
			t.Require().Equal(i+burst, agent.ID)
		case <-time.After(time.Millisecond * 200):
			// add 100ms time drift here
			t.T().Error("get agent timeout")
		}
	}
}

func (t *testMaster) testEmit() {
	type testWorkerType int

	var (
		id                    = "worker-01"
		worker testWorkerType = 1
	)

	ap := NewAgentPool(&RateLimitConfig{rate: DefaultRate, burst: DefaultBurst})
	go ap.Start(context.Background())

	ap.Emit(context.Background(), 1, func(args ...interface{}) {
		if len(args) != 2 {
			t.T().Fatalf("args count is not 2, args %v", args)
		}

		id1, ok := args[0].(string)
		if !ok {
			t.T().Fatalf("args[0] is not id, args %+v", args)
		}
		if id != id1 {
			t.T().Fatalf("args[0] is expected id, args[0] %s vs %s", id1, id)
		}

		worker1, ok := args[1].(testWorkerType)
		if !ok {
			t.T().Fatalf("args[1] is not worker client, args %+v", args)
		}
		if worker1 != worker {
			t.T().Fatalf("args[1] is not expected worker, args[1] %v vs %v", worker1, worker)
		}
	}, func(args ...interface{}) {}, []interface{}{id, worker}...)

	counter := 0
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ap.Emit(ctx, 1, func(args ...interface{}) {
		t.T().FailNow()
	}, func(args ...interface{}) {
		if len(args) != 1 {
			t.T().Fatalf("args count is not 1, args %v", args)
		}
		pCounter, ok := args[0].(*int)
		if !ok {
			t.T().Fatalf("args[0] is not *int, args %+v", args)
		}
		*pCounter++
	}, []interface{}{&counter}...)
	t.Require().Equal(1, counter)
}
