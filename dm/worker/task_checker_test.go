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

package worker

import (
	"testing"
	"time"

	"github.com/pingcap/errors"
	tmysql "github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/backoff"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/unit"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

var (
	unsupportedModifyColumnError = unit.NewProcessError(terror.ErrDBExecuteFailed.Delegate(&tmysql.SQLError{Code: 1105, Message: "unsupported modify column length 20 is less than origin 40", State: tmysql.DefaultMySQLState}))
	unknownProcessError          = unit.NewProcessError(errors.New("error message"))
)

func TestResumeStrategy(t *testing.T) {
	require.Equal(t, resumeStrategy2Str[ResumeSkip], ResumeSkip.String())
	require.Equal(t, "unsupported resume strategy: 10000", ResumeStrategy(10000).String())

	taskName := "test-task"
	now := func(addition time.Duration) time.Time { return time.Now().Add(addition) }
	testCases := []struct {
		status         *pb.SubTaskStatus
		latestResumeFn func(addition time.Duration) time.Time
		addition       time.Duration
		duration       time.Duration
		expected       ResumeStrategy
	}{
		{nil, now, time.Duration(0), 1 * time.Millisecond, ResumeIgnore},
		{&pb.SubTaskStatus{Name: taskName, Stage: pb.Stage_Running}, now, time.Duration(0), 1 * time.Millisecond, ResumeIgnore},
		{&pb.SubTaskStatus{Name: taskName, Stage: pb.Stage_Paused}, now, time.Duration(0), 1 * time.Millisecond, ResumeIgnore},
		{&pb.SubTaskStatus{Name: taskName, Stage: pb.Stage_Paused, Result: &pb.ProcessResult{IsCanceled: true}}, now, time.Duration(0), 1 * time.Millisecond, ResumeIgnore},
		{&pb.SubTaskStatus{Name: taskName, Stage: pb.Stage_Paused, Result: &pb.ProcessResult{IsCanceled: false, Errors: []*pb.ProcessError{unsupportedModifyColumnError}}}, now, time.Duration(0), 1 * time.Millisecond, ResumeNoSense},
		{&pb.SubTaskStatus{Name: taskName, Stage: pb.Stage_Paused, Result: &pb.ProcessResult{IsCanceled: false}}, now, time.Duration(0), 1 * time.Second, ResumeSkip},
		{&pb.SubTaskStatus{Name: taskName, Stage: pb.Stage_Paused, Result: &pb.ProcessResult{IsCanceled: false}}, now, -2 * time.Millisecond, 1 * time.Millisecond, ResumeDispatch},
	}

	tsc := NewRealTaskStatusChecker(config.CheckerConfig{
		CheckEnable:     true,
		CheckInterval:   config.Duration{Duration: config.DefaultCheckInterval},
		BackoffRollback: config.Duration{Duration: config.DefaultBackoffRollback},
		BackoffMin:      config.Duration{Duration: config.DefaultBackoffMin},
		BackoffMax:      config.Duration{Duration: config.DefaultBackoffMax},
		BackoffFactor:   config.DefaultBackoffFactor,
	}, nil)
	for _, tc := range testCases {
		rtsc, ok := tsc.(*realTaskStatusChecker)
		require.True(t, ok)
		bf, _ := backoff.NewBackoff(
			1,
			false,
			tc.duration,
			tc.duration)
		rtsc.subtaskAutoResume[taskName] = &AutoResumeInfo{
			Backoff:          bf,
			LatestResumeTime: tc.latestResumeFn(tc.addition),
		}
		strategy := rtsc.subtaskAutoResume[taskName].CheckResumeSubtask(tc.status, config.DefaultBackoffRollback)
		require.Equal(t, tc.expected, strategy)
	}
}

func TestCheck(t *testing.T) {
	var (
		latestResumeTime time.Time
		latestPausedTime time.Time
		latestBlockTime  time.Time
		taskName         = "test-check-task"
	)

	NewRelayHolder = NewDummyRelayHolder
	dir := t.TempDir()
	cfg := loadSourceConfigWithoutPassword2(t)
	cfg.RelayDir = dir
	cfg.MetaDir = dir
	w, err := NewSourceWorker(cfg, nil, "", "")
	require.NoError(t, err)
	w.closed.Store(false)

	tsc := NewRealTaskStatusChecker(config.CheckerConfig{
		CheckEnable:     true,
		CheckInterval:   config.Duration{Duration: config.DefaultCheckInterval},
		BackoffRollback: config.Duration{Duration: 200 * time.Millisecond},
		BackoffMin:      config.Duration{Duration: 1 * time.Millisecond},
		BackoffMax:      config.Duration{Duration: 1 * time.Second},
		BackoffFactor:   config.DefaultBackoffFactor,
	}, nil)
	require.NoError(t, tsc.Init())
	rtsc, ok := tsc.(*realTaskStatusChecker)
	require.True(t, ok)
	rtsc.w = w

	st := &SubTask{
		cfg:   &config.SubTaskConfig{SourceID: "source", Name: taskName},
		stage: pb.Stage_Running,
		l:     log.With(zap.String("subtask", taskName)),
	}
	require.NoError(t, st.cfg.Adjust(false))
	rtsc.w.subTaskHolder.recordSubTask(st)
	rtsc.check()
	bf := rtsc.subtaskAutoResume[taskName].Backoff

	// test resume with paused task
	st.stage = pb.Stage_Paused
	st.result = &pb.ProcessResult{
		IsCanceled: false,
		Errors:     []*pb.ProcessError{unknownProcessError},
	}
	time.Sleep(1 * time.Millisecond)
	rtsc.check()
	time.Sleep(2 * time.Millisecond)
	rtsc.check()
	time.Sleep(4 * time.Millisecond)
	rtsc.check()
	require.Equal(t, 8*time.Millisecond, bf.Current())

	// test backoff rollback at least once, as well as resume ignore strategy
	st.result = &pb.ProcessResult{IsCanceled: true}
	time.Sleep(200 * time.Millisecond)
	rtsc.check()
	require.True(t, bf.Current() <= 4*time.Millisecond)
	current := bf.Current()

	// test no sense strategy
	st.result = &pb.ProcessResult{
		IsCanceled: false,
		Errors:     []*pb.ProcessError{unsupportedModifyColumnError},
	}
	rtsc.check()
	require.True(t, latestPausedTime.Before(rtsc.subtaskAutoResume[taskName].LatestPausedTime))
	latestBlockTime = rtsc.subtaskAutoResume[taskName].LatestBlockTime
	time.Sleep(200 * time.Millisecond)
	rtsc.check()
	require.Equal(t, latestBlockTime, rtsc.subtaskAutoResume[taskName].LatestBlockTime)
	require.Equal(t, current, bf.Current())

	// test resume skip strategy
	tsc = NewRealTaskStatusChecker(config.CheckerConfig{
		CheckEnable:     true,
		CheckInterval:   config.Duration{Duration: config.DefaultCheckInterval},
		BackoffRollback: config.Duration{Duration: 200 * time.Millisecond},
		BackoffMin:      config.Duration{Duration: 10 * time.Second},
		BackoffMax:      config.Duration{Duration: 100 * time.Second},
		BackoffFactor:   config.DefaultBackoffFactor,
	}, w)
	require.NoError(t, tsc.Init())
	rtsc, ok = tsc.(*realTaskStatusChecker)
	require.True(t, ok)

	st = &SubTask{
		cfg:   &config.SubTaskConfig{Name: taskName},
		stage: pb.Stage_Running,
		l:     log.With(zap.String("subtask", taskName)),
	}
	rtsc.w.subTaskHolder.recordSubTask(st)
	rtsc.check()
	bf = rtsc.subtaskAutoResume[taskName].Backoff

	st.stage = pb.Stage_Paused
	st.result = &pb.ProcessResult{
		IsCanceled: false,
		Errors:     []*pb.ProcessError{unknownProcessError},
	}
	rtsc.check()
	latestResumeTime = rtsc.subtaskAutoResume[taskName].LatestResumeTime
	latestPausedTime = rtsc.subtaskAutoResume[taskName].LatestPausedTime
	require.Equal(t, 10*time.Second, bf.Current())
	for i := 0; i < 10; i++ {
		rtsc.check()
		require.Equal(t, latestResumeTime, rtsc.subtaskAutoResume[taskName].LatestResumeTime)
		require.True(t, latestPausedTime.Before(rtsc.subtaskAutoResume[taskName].LatestPausedTime))
		latestPausedTime = rtsc.subtaskAutoResume[taskName].LatestPausedTime
	}
}

func TestCheckTaskIndependent(t *testing.T) {
	var (
		task1                 = "task1"
		task2                 = "tesk2"
		task1LatestResumeTime time.Time
		task2LatestResumeTime time.Time
		backoffMin            = 5 * time.Millisecond
	)

	NewRelayHolder = NewDummyRelayHolder
	dir := t.TempDir()
	cfg := loadSourceConfigWithoutPassword2(t)
	cfg.RelayDir = dir
	cfg.MetaDir = dir
	w, err := NewSourceWorker(cfg, nil, "", "")
	require.NoError(t, err)
	w.closed.Store(false)

	tsc := NewRealTaskStatusChecker(config.CheckerConfig{
		CheckEnable:     true,
		CheckInterval:   config.Duration{Duration: config.DefaultCheckInterval},
		BackoffRollback: config.Duration{Duration: 200 * time.Millisecond},
		BackoffMin:      config.Duration{Duration: backoffMin},
		BackoffMax:      config.Duration{Duration: 10 * time.Second},
		BackoffFactor:   1.0,
	}, nil)
	require.NoError(t, tsc.Init())
	rtsc, ok := tsc.(*realTaskStatusChecker)
	require.True(t, ok)
	rtsc.w = w

	st1 := &SubTask{
		cfg:   &config.SubTaskConfig{Name: task1},
		stage: pb.Stage_Running,
		l:     log.With(zap.String("subtask", task1)),
	}
	rtsc.w.subTaskHolder.recordSubTask(st1)
	st2 := &SubTask{
		cfg:   &config.SubTaskConfig{Name: task2},
		stage: pb.Stage_Running,
		l:     log.With(zap.String("subtask", task2)),
	}
	rtsc.w.subTaskHolder.recordSubTask(st2)
	rtsc.check()
	require.Len(t, rtsc.subtaskAutoResume, 2)
	for _, times := range rtsc.subtaskAutoResume {
		require.True(t, times.LatestBlockTime.IsZero())
	}

	// test backoff strategies of different tasks do not affect each other
	st1 = &SubTask{
		cfg:   &config.SubTaskConfig{SourceID: "source", Name: task1},
		stage: pb.Stage_Paused,
		result: &pb.ProcessResult{
			IsCanceled: false,
			Errors:     []*pb.ProcessError{unsupportedModifyColumnError},
		},
		l: log.With(zap.String("subtask", task1)),
	}
	require.NoError(t, st1.cfg.Adjust(false))
	rtsc.w.subTaskHolder.recordSubTask(st1)
	st2 = &SubTask{
		cfg:   &config.SubTaskConfig{SourceID: "source", Name: task2},
		stage: pb.Stage_Paused,
		result: &pb.ProcessResult{
			IsCanceled: false,
			Errors:     []*pb.ProcessError{unknownProcessError},
		},
		l: log.With(zap.String("subtask", task2)),
	}
	require.NoError(t, st2.cfg.Adjust(false))
	rtsc.w.subTaskHolder.recordSubTask(st2)

	task1LatestResumeTime = rtsc.subtaskAutoResume[task1].LatestResumeTime
	task2LatestResumeTime = rtsc.subtaskAutoResume[task2].LatestResumeTime
	for i := 0; i < 10; i++ {
		time.Sleep(backoffMin)
		rtsc.check()
		require.Equal(t, task1LatestResumeTime, rtsc.subtaskAutoResume[task1].LatestResumeTime)
		require.True(t, task2LatestResumeTime.Before(rtsc.subtaskAutoResume[task2].LatestResumeTime))
		require.False(t, rtsc.subtaskAutoResume[task1].LatestBlockTime.IsZero())
		require.True(t, rtsc.subtaskAutoResume[task2].LatestBlockTime.IsZero())

		task2LatestResumeTime = rtsc.subtaskAutoResume[task2].LatestResumeTime
	}

	// test task information cleanup in task status checker
	rtsc.w.subTaskHolder.removeSubTask(task1)
	time.Sleep(backoffMin)
	rtsc.check()
	require.True(t, task2LatestResumeTime.Before(rtsc.subtaskAutoResume[task2].LatestResumeTime))
	require.Len(t, rtsc.subtaskAutoResume, 1)
	require.True(t, rtsc.subtaskAutoResume[task2].LatestBlockTime.IsZero())
}
