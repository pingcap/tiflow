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

package worker

import (
	"testing"
	"time"

	"github.com/pingcap/tiflow/engine/executor/worker/internal"
	"github.com/pingcap/tiflow/engine/framework/taskutil"
	"github.com/pingcap/tiflow/engine/pkg/clock"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockWrappedTaskAdder struct {
	mock.Mock
}

func (a *mockWrappedTaskAdder) addWrappedTask(task *internal.RunnableContainer) error {
	args := a.Called(task)
	return args.Error(0)
}

type taskCommitterTestSuite struct {
	Committer *TaskCommitter
	Clock     *clock.Mock
	Runner    *mockWrappedTaskAdder
}

func newTaskCommitterTestSuite() *taskCommitterTestSuite {
	clk := clock.NewMock()
	clk.Set(time.Now())
	runner := &mockWrappedTaskAdder{}
	committer := newTaskCommitterWithClock(runner, preDispatchTTLForTest, clk)
	return &taskCommitterTestSuite{
		Committer: committer,
		Clock:     clk,
		Runner:    runner,
	}
}

func (s *taskCommitterTestSuite) Close() {
	s.Committer.Close()
}

const preDispatchTTLForTest = 10 * time.Second

func TestTaskCommitterSuccessCase(t *testing.T) {
	suite := newTaskCommitterTestSuite()

	submitTime := time.Now()
	suite.Clock.Set(submitTime)
	task := newDummyWorker("task-1")
	ok := suite.Committer.PreDispatchTask("request-1", taskutil.WrapWorker(task))
	require.True(t, ok)

	suite.Runner.On("addWrappedTask",
		mock.MatchedBy(func(arg *internal.RunnableContainer) bool {
			return arg.ID() == "task-1" && arg.Info().SubmitTime == clock.ToMono(submitTime)
		})).Return(nil)
	ok, err := suite.Committer.ConfirmDispatchTask("request-1", "task-1")
	require.True(t, ok)
	require.NoError(t, err)
	suite.Runner.AssertExpectations(t)

	suite.Close()
}

func TestTaskCommitterNoConfirmUntilTTL(t *testing.T) {
	suite := newTaskCommitterTestSuite()

	task := newDummyWorker("task-1")
	ok := suite.Committer.PreDispatchTask("request-1", taskutil.WrapWorker(task))
	require.True(t, ok)

	oldCount := suite.Committer.cleanUpCount()
	suite.Clock.Add(preDispatchTTLForTest)
	require.Eventually(t, func() bool {
		suite.Clock.Add(time.Second)
		return suite.Committer.cleanUpCount() > oldCount
	}, 1*time.Second, 10*time.Millisecond)

	ok, err := suite.Committer.ConfirmDispatchTask("request-1", "task-1")
	require.False(t, ok)
	require.NoError(t, err)

	suite.Close()
}

func TestTaskCommitterSameTaskIDOverwrites(t *testing.T) {
	suite := newTaskCommitterTestSuite()

	task := newDummyWorker("task-1")
	submitTime1 := time.Now()
	suite.Clock.Set(submitTime1)
	ok := suite.Committer.PreDispatchTask("request-1", taskutil.WrapWorker(task))
	require.True(t, ok)

	anotherTask := newDummyWorker("task-1")
	submitTime2 := submitTime1.Add(time.Second)
	suite.Clock.Set(submitTime2)
	ok = suite.Committer.PreDispatchTask("request-2", taskutil.WrapWorker(anotherTask))
	require.True(t, ok)

	ok, err := suite.Committer.ConfirmDispatchTask("request-1", "task-1")
	require.False(t, ok)
	require.NoError(t, err)

	suite.Runner.On("addWrappedTask",
		mock.MatchedBy(func(arg *internal.RunnableContainer) bool {
			return arg.ID() == "task-1" && arg.Info().SubmitTime == clock.ToMono(submitTime2)
		})).Return(nil)
	ok, err = suite.Committer.ConfirmDispatchTask("request-2", "task-1")
	require.True(t, ok)
	require.NoError(t, err)
	suite.Runner.AssertExpectations(t)

	suite.Close()
}

func TestTaskCommitterDuplicatePreDispatch(t *testing.T) {
	suite := newTaskCommitterTestSuite()

	task := newDummyWorker("task-1")
	ok := suite.Committer.PreDispatchTask("request-1", taskutil.WrapWorker(task))
	require.True(t, ok)

	ok = suite.Committer.PreDispatchTask("request-1", taskutil.WrapWorker(task))
	require.False(t, ok)

	suite.Close()
}

func TestTaskCommitterFailToSubmit(t *testing.T) {
	suite := newTaskCommitterTestSuite()

	submitTime := time.Now()
	suite.Clock.Set(submitTime)
	task := newDummyWorker("task-1")
	ok := suite.Committer.PreDispatchTask("request-1", taskutil.WrapWorker(task))
	require.True(t, ok)

	suite.Runner.On("addWrappedTask", mock.Anything).
		Return(errors.ErrRuntimeIncomingQueueFull.GenWithStackByArgs())
	ok, err := suite.Committer.ConfirmDispatchTask("request-1", "task-1")
	require.False(t, ok)
	require.Error(t, err)
	require.Regexp(t, ".*ErrRuntimeIncomingQueueFull.*", err)
	suite.Runner.AssertExpectations(t)

	suite.Close()
}
