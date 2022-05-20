package worker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/hanfei1991/microcosm/executor/worker/internal"
	"github.com/hanfei1991/microcosm/pkg/clock"
	derror "github.com/hanfei1991/microcosm/pkg/errors"
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

func newTaskCommitterTestSuite(ttl time.Duration) *taskCommitterTestSuite {
	clk := clock.NewMock()
	clk.Set(time.Now())
	runner := &mockWrappedTaskAdder{}
	committer := newTaskCommitterWithClock(runner, ttl, clk)
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
	suite := newTaskCommitterTestSuite(preDispatchTTLForTest)

	submitTime := time.Now()
	suite.Clock.Set(submitTime)
	task := newDummyWorker("task-1")
	ok := suite.Committer.PreDispatchTask("request-1", task)
	require.True(t, ok)

	suite.Runner.On("addWrappedTask",
		mock.MatchedBy(func(arg *internal.RunnableContainer) bool {
			return arg.ID() == "task-1" && arg.Info().SubmitTime == submitTime
		})).Return(nil)
	ok, err := suite.Committer.ConfirmDispatchTask("request-1", "task-1")
	require.True(t, ok)
	require.NoError(t, err)
	suite.Runner.AssertExpectations(t)

	suite.Close()
}

func TestTaskCommitterNoConfirmUntilTTL(t *testing.T) {
	suite := newTaskCommitterTestSuite(preDispatchTTLForTest)

	task := newDummyWorker("task-1")
	ok := suite.Committer.PreDispatchTask("request-1", task)
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
	suite := newTaskCommitterTestSuite(preDispatchTTLForTest)

	task := newDummyWorker("task-1")
	submitTime1 := time.Now()
	suite.Clock.Set(submitTime1)
	ok := suite.Committer.PreDispatchTask("request-1", task)
	require.True(t, ok)

	anotherTask := newDummyWorker("task-1")
	submitTime2 := submitTime1.Add(time.Second)
	suite.Clock.Set(submitTime2)
	ok = suite.Committer.PreDispatchTask("request-2", anotherTask)
	require.True(t, ok)

	ok, err := suite.Committer.ConfirmDispatchTask("request-1", "task-1")
	require.False(t, ok)
	require.NoError(t, err)

	suite.Runner.On("addWrappedTask",
		mock.MatchedBy(func(arg *internal.RunnableContainer) bool {
			return arg.ID() == "task-1" && arg.Info().SubmitTime == submitTime2
		})).Return(nil)
	ok, err = suite.Committer.ConfirmDispatchTask("request-2", "task-1")
	require.True(t, ok)
	require.NoError(t, err)
	suite.Runner.AssertExpectations(t)

	suite.Close()
}

func TestTaskCommitterDuplicatePreDispatch(t *testing.T) {
	suite := newTaskCommitterTestSuite(preDispatchTTLForTest)

	task := newDummyWorker("task-1")
	ok := suite.Committer.PreDispatchTask("request-1", task)
	require.True(t, ok)

	ok = suite.Committer.PreDispatchTask("request-1", task)
	require.False(t, ok)

	suite.Close()
}

func TestTaskCommitterFailToSubmit(t *testing.T) {
	suite := newTaskCommitterTestSuite(preDispatchTTLForTest)

	submitTime := time.Now()
	suite.Clock.Set(submitTime)
	task := newDummyWorker("task-1")
	ok := suite.Committer.PreDispatchTask("request-1", task)
	require.True(t, ok)

	suite.Runner.On("addWrappedTask", mock.Anything).
		Return(derror.ErrRuntimeIncomingQueueFull.GenWithStackByArgs())
	ok, err := suite.Committer.ConfirmDispatchTask("request-1", "task-1")
	require.False(t, ok)
	require.Error(t, err)
	require.Regexp(t, ".*ErrRuntimeIncomingQueueFull.*", err)
	suite.Runner.AssertExpectations(t)

	suite.Close()
}
