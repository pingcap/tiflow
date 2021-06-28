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

package workerpool

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/pkg/util/testleak"
	"golang.org/x/sync/errgroup"
)

type contextSuite struct{}

var _ = check.Suite(&contextSuite{})

// TestDoneCase1 tests cancelling the first context in the merge.
func (s *contextSuite) TestDoneCase1(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx, cancel0 := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel0()

	ctx1, cancel1 := context.WithCancel(ctx)
	defer cancel1()
	ctx2, cancel2 := context.WithCancel(ctx)
	defer cancel2()

	merged, cancelM := MergeContexts(ctx1, ctx2)
	defer cancelM()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		<-merged.Done()
		wg.Done()
	}()

	cancel1()
	wg.Wait()
}

// TestDoneCase2 tests cancelling the second context in the merge.
func (s *contextSuite) TestDoneCase2(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx, cancel0 := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel0()

	ctx1, cancel1 := context.WithCancel(ctx)
	defer cancel1()
	ctx2, cancel2 := context.WithCancel(ctx)
	defer cancel2()

	merged, cancelM := MergeContexts(ctx1, ctx2)
	defer cancelM()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		<-merged.Done()
		wg.Done()
	}()

	cancel2()
	wg.Wait()
}

// TestDoneCaseCancel tests cancelling the merged context.
func (s *contextSuite) TestDoneCaseCancel(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx, cancel0 := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel0()

	ctx1, cancel1 := context.WithCancel(ctx)
	defer cancel1()
	ctx2, cancel2 := context.WithCancel(ctx)
	defer cancel2()

	merged, cancelM := MergeContexts(ctx1, ctx2)
	defer cancelM()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		<-merged.Done()
		wg.Done()
	}()

	cancelM()
	wg.Wait()
}

func (s *contextSuite) TestDoneContention(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx, cancel0 := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel0()

	errg, ctx := errgroup.WithContext(ctx)

	ctx1, cancel1 := context.WithCancel(ctx)
	defer cancel1()
	ctx2, cancel2 := context.WithCancel(ctx)
	defer cancel2()

	merged, cancelM := MergeContexts(ctx1, ctx2)
	defer cancelM()

	for i := 0; i < 32; i++ {
		errg.Go(func() error {
			<-merged.Done()
			c.Assert(ctx.Err(), check.IsNil)
			return nil
		})
	}

	time.Sleep(time.Second * 1)
	cancel1()

	err := errg.Wait()
	c.Assert(err, check.IsNil)
}

type mockContext struct {
	deadline time.Time
	err      error
	values   map[string]string
}

func (m *mockContext) Deadline() (deadline time.Time, ok bool) {
	if m.deadline.IsZero() {
		return time.Time{}, false
	}
	return m.deadline, true
}

func (m *mockContext) Done() <-chan struct{} {
	panic("not used")
}

func (m *mockContext) Err() error {
	return m.err
}

func (m *mockContext) Value(key interface{}) interface{} {
	if value, ok := m.values[key.(string)]; ok {
		return value
	}
	return nil
}

func (s *contextSuite) TestErr(c *check.C) {
	defer testleak.AfterTest(c)()

	ctx1 := &mockContext{}
	ctx2 := &mockContext{}

	mContext, cancelM := MergeContexts(ctx1, ctx2)
	defer cancelM()

	c.Assert(mContext.Err(), check.IsNil)

	ctx1.err = errors.New("error1")
	c.Assert(mContext.Err(), check.ErrorMatches, "error1")

	ctx2.err = errors.New("error2")
	c.Assert(mContext.Err(), check.ErrorMatches, "error1")

	ctx1.err = nil
	c.Assert(mContext.Err(), check.ErrorMatches, "error2")
}

func (s *contextSuite) TestDeadline(c *check.C) {
	defer testleak.AfterTest(c)()

	ctx1 := &mockContext{}
	ctx2 := &mockContext{}

	mContext, cancelM := MergeContexts(ctx1, ctx2)
	defer cancelM()

	_, ok := mContext.Deadline()
	c.Assert(ok, check.IsFalse)

	startTime := time.Now()

	ctx1.deadline = startTime.Add(time.Minute * 1)
	ddl, ok := mContext.Deadline()
	c.Assert(ok, check.IsTrue)
	c.Assert(ddl, check.Equals, startTime.Add(time.Minute*1))

	ctx2.deadline = startTime.Add(time.Minute * 2)
	ddl, ok = mContext.Deadline()
	c.Assert(ok, check.IsTrue)
	c.Assert(ddl, check.Equals, startTime.Add(time.Minute*1))

	ctx1.deadline = startTime.Add(time.Minute * 3)
	ddl, ok = mContext.Deadline()
	c.Assert(ok, check.IsTrue)
	c.Assert(ddl, check.Equals, startTime.Add(time.Minute*2))

	ctx1.deadline = time.Time{}
	ddl, ok = mContext.Deadline()
	c.Assert(ok, check.IsTrue)
	c.Assert(ddl, check.Equals, startTime.Add(time.Minute*2))
}

func (s *contextSuite) TestValues(c *check.C) {
	defer testleak.AfterTest(c)()

	ctx1 := &mockContext{
		values: map[string]string{"a": "1a", "c": "1c"},
	}
	ctx2 := &mockContext{
		values: map[string]string{"b": "2b", "c": "2c"},
	}

	mContext, cancelM := MergeContexts(ctx1, ctx2)
	defer cancelM()

	c.Assert(mContext.Value("a"), check.Equals, "1a")
	c.Assert(mContext.Value("b"), check.Equals, "2b")
	c.Assert(mContext.Value("c"), check.Equals, "1c")
	c.Assert(mContext.Value("d"), check.IsNil)
}
