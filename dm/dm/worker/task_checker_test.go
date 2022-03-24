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
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	tmysql "github.com/pingcap/tidb/parser/mysql"
	"go.uber.org/zap"

	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/dm/dm/unit"
	"github.com/pingcap/tiflow/dm/pkg/backoff"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
)

var _ = check.Suite(&testTaskCheckerSuite{})

type testTaskCheckerSuite struct{}

var (
	unsupportedModifyColumnError = unit.NewProcessError(terror.ErrDBExecuteFailed.Delegate(&tmysql.SQLError{Code: 1105, Message: "unsupported modify column length 20 is less than origin 40", State: tmysql.DefaultMySQLState}))
	unknownProcessError          = unit.NewProcessError(errors.New("error message"))
)

func (s *testTaskCheckerSuite) TestResumeStrategy(c *check.C) {
	c.Assert(ResumeSkip.String(), check.Equals, resumeStrategy2Str[ResumeSkip])
	c.Assert(ResumeStrategy(10000).String(), check.Equals, "unsupported resume strategy: 10000")

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
		c.Assert(ok, check.IsTrue)
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
		c.Assert(strategy, check.Equals, tc.expected)
	}
}

func (s *testTaskCheckerSuite) TestCheck(c *check.C) {
	var (
		latestResumeTime time.Time
		latestPausedTime time.Time
		latestBlockTime  time.Time
		taskName         = "test-check-task"
	)

	NewRelayHolder = NewDummyRelayHolder
	dir := c.MkDir()
	cfg := loadSourceConfigWithoutPassword(c)
	cfg.RelayDir = dir
	cfg.MetaDir = dir
	w, err := NewSourceWorker(cfg, nil, "", "")
	c.Assert(err, check.IsNil)
	w.closed.Store(false)

	tsc := NewRealTaskStatusChecker(config.CheckerConfig{
		CheckEnable:     true,
		CheckInterval:   config.Duration{Duration: config.DefaultCheckInterval},
		BackoffRollback: config.Duration{Duration: 200 * time.Millisecond},
		BackoffMin:      config.Duration{Duration: 1 * time.Millisecond},
		BackoffMax:      config.Duration{Duration: 1 * time.Second},
		BackoffFactor:   config.DefaultBackoffFactor,
	}, nil)
	c.Assert(tsc.Init(), check.IsNil)
	rtsc, ok := tsc.(*realTaskStatusChecker)
	c.Assert(ok, check.IsTrue)
	rtsc.w = w

	st := &SubTask{
		cfg:   &config.SubTaskConfig{SourceID: "source", Name: taskName},
		stage: pb.Stage_Running,
		l:     log.With(zap.String("subtask", taskName)),
	}
	c.Assert(st.cfg.Adjust(false), check.IsNil)
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
	c.Assert(bf.Current(), check.Equals, 8*time.Millisecond)

	// test backoff rollback at least once, as well as resume ignore strategy
	st.result = &pb.ProcessResult{IsCanceled: true}
	time.Sleep(200 * time.Millisecond)
	rtsc.check()
	c.Assert(bf.Current() <= 4*time.Millisecond, check.IsTrue)
	current := bf.Current()

	// test no sense strategy
	st.result = &pb.ProcessResult{
		IsCanceled: false,
		Errors:     []*pb.ProcessError{unsupportedModifyColumnError},
	}
	rtsc.check()
	c.Assert(latestPausedTime.Before(rtsc.subtaskAutoResume[taskName].LatestPausedTime), check.IsTrue)
	latestBlockTime = rtsc.subtaskAutoResume[taskName].LatestBlockTime
	time.Sleep(200 * time.Millisecond)
	rtsc.check()
	c.Assert(rtsc.subtaskAutoResume[taskName].LatestBlockTime, check.Equals, latestBlockTime)
	c.Assert(bf.Current(), check.Equals, current)

	// test resume skip strategy
	tsc = NewRealTaskStatusChecker(config.CheckerConfig{
		CheckEnable:     true,
		CheckInterval:   config.Duration{Duration: config.DefaultCheckInterval},
		BackoffRollback: config.Duration{Duration: 200 * time.Millisecond},
		BackoffMin:      config.Duration{Duration: 10 * time.Second},
		BackoffMax:      config.Duration{Duration: 100 * time.Second},
		BackoffFactor:   config.DefaultBackoffFactor,
	}, w)
	c.Assert(tsc.Init(), check.IsNil)
	rtsc, ok = tsc.(*realTaskStatusChecker)
	c.Assert(ok, check.IsTrue)

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
	c.Assert(bf.Current(), check.Equals, 10*time.Second)
	for i := 0; i < 10; i++ {
		rtsc.check()
		c.Assert(latestResumeTime, check.Equals, rtsc.subtaskAutoResume[taskName].LatestResumeTime)
		c.Assert(latestPausedTime.Before(rtsc.subtaskAutoResume[taskName].LatestPausedTime), check.IsTrue)
		latestPausedTime = rtsc.subtaskAutoResume[taskName].LatestPausedTime
	}
}

func (s *testTaskCheckerSuite) TestCheckTaskIndependent(c *check.C) {
	var (
		task1                 = "task1"
		task2                 = "tesk2"
		task1LatestResumeTime time.Time
		task2LatestResumeTime time.Time
		backoffMin            = 5 * time.Millisecond
	)

	NewRelayHolder = NewDummyRelayHolder
	dir := c.MkDir()
	cfg := loadSourceConfigWithoutPassword(c)
	cfg.RelayDir = dir
	cfg.MetaDir = dir
	w, err := NewSourceWorker(cfg, nil, "", "")
	c.Assert(err, check.IsNil)
	w.closed.Store(false)

	tsc := NewRealTaskStatusChecker(config.CheckerConfig{
		CheckEnable:     true,
		CheckInterval:   config.Duration{Duration: config.DefaultCheckInterval},
		BackoffRollback: config.Duration{Duration: 200 * time.Millisecond},
		BackoffMin:      config.Duration{Duration: backoffMin},
		BackoffMax:      config.Duration{Duration: 10 * time.Second},
		BackoffFactor:   1.0,
	}, nil)
	c.Assert(tsc.Init(), check.IsNil)
	rtsc, ok := tsc.(*realTaskStatusChecker)
	c.Assert(ok, check.IsTrue)
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
	c.Assert(len(rtsc.subtaskAutoResume), check.Equals, 2)
	for _, times := range rtsc.subtaskAutoResume {
		c.Assert(times.LatestBlockTime.IsZero(), check.IsTrue)
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
	c.Assert(st1.cfg.Adjust(false), check.IsNil)
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
	c.Assert(st2.cfg.Adjust(false), check.IsNil)
	rtsc.w.subTaskHolder.recordSubTask(st2)

	task1LatestResumeTime = rtsc.subtaskAutoResume[task1].LatestResumeTime
	task2LatestResumeTime = rtsc.subtaskAutoResume[task2].LatestResumeTime
	for i := 0; i < 10; i++ {
		time.Sleep(backoffMin)
		rtsc.check()
		c.Assert(task1LatestResumeTime, check.Equals, rtsc.subtaskAutoResume[task1].LatestResumeTime)
		c.Assert(task2LatestResumeTime.Before(rtsc.subtaskAutoResume[task2].LatestResumeTime), check.IsTrue)
		c.Assert(rtsc.subtaskAutoResume[task1].LatestBlockTime.IsZero(), check.IsFalse)
		c.Assert(rtsc.subtaskAutoResume[task2].LatestBlockTime.IsZero(), check.IsTrue)

		task2LatestResumeTime = rtsc.subtaskAutoResume[task2].LatestResumeTime
	}

	// test task information cleanup in task status checker
	rtsc.w.subTaskHolder.removeSubTask(task1)
	time.Sleep(backoffMin)
	rtsc.check()
	c.Assert(task2LatestResumeTime.Before(rtsc.subtaskAutoResume[task2].LatestResumeTime), check.IsTrue)
	c.Assert(len(rtsc.subtaskAutoResume), check.Equals, 1)
	c.Assert(rtsc.subtaskAutoResume[task2].LatestBlockTime.IsZero(), check.IsTrue)
}

func (s *testTaskCheckerSuite) TestIsResumableError(c *check.C) {
	testCases := []struct {
		err       error
		resumable bool
	}{
		// only DM new error is checked
		{&tmysql.SQLError{Code: 1105, Message: "unsupported modify column length 20 is less than origin 40", State: tmysql.DefaultMySQLState}, true},
		{&tmysql.SQLError{Code: 1105, Message: "unsupported drop integer primary key", State: tmysql.DefaultMySQLState}, true},
		{terror.ErrDBExecuteFailed.Generate("file test.t3.sql: execute statement failed: USE `test_abc`;: context canceled"), true},
		{terror.ErrDBExecuteFailed.Delegate(&tmysql.SQLError{Code: 1105, Message: "unsupported modify column length 20 is less than origin 40", State: tmysql.DefaultMySQLState}, "alter table t modify col varchar(20)"), false},
		{terror.ErrDBExecuteFailed.Delegate(&tmysql.SQLError{Code: 1105, Message: "unsupported drop integer primary key", State: tmysql.DefaultMySQLState}, "alter table t drop column id"), false},
		{terror.ErrDBExecuteFailed.Delegate(&tmysql.SQLError{Code: 1067, Message: "Invalid default value for 'ct'", State: tmysql.DefaultMySQLState}, "CREATE TABLE `tbl` (`c1` int(11) NOT NULL,`ct` datetime NOT NULL DEFAULT '0000-00-00 00:00:00' COMMENT '创建时间',PRIMARY KEY (`c1`)) ENGINE=InnoDB DEFAULT CHARSET=latin1"), false},
		{terror.ErrDBExecuteFailed.Delegate(errors.New("Error 1062: Duplicate entry '5' for key 'PRIMARY'")), false},
		{terror.ErrDBExecuteFailed.Delegate(errors.New("INSERT INTO `db`.`tbl` (`c1`,`c2`) VALUES (?,?);: Error 1406: Data too long for column 'c2' at row 1")), false},
		// real error is generated by `Delegate` and multiple `Annotatef`, we use `New` to simplify it
		{terror.ErrParserParseRelayLog.New("parse relay log file bin.000018 from offset 555 in dir /home/tidb/deploy/relay_log/d2e831df-b4ec-11e9-9237-0242ac110008.000004: parse relay log file bin.000018 from offset 0 in dir /home/tidb/deploy/relay_log/d2e831df-b4ec-11e9-9237-0242ac110008.000004: parse relay log file /home/tidb/deploy/relay_log/d2e831df-b4ec-11e9-9237-0242ac110008.000004/bin.000018: binlog checksum mismatch, data may be corrupted"), false},
		{terror.ErrParserParseRelayLog.New("parse relay log file bin.000018 from offset 500 in dir /home/tidb/deploy/relay_log/d2e831df-b4ec-11e9-9237-0242ac110008.000004: parse relay log file bin.000018 from offset 0 in dir /home/tidb/deploy/relay_log/d2e831df-b4ec-11e9-9237-0242ac110008.000004: parse relay log file /home/tidb/deploy/relay_log/d2e831df-b4ec-11e9-9237-0242ac110008.000004/bin.000018: get event err EOF, need 1567488104 but got 316323"), false},
		{terror.ErrSyncUnitDDLWrongSequence.Generate("wrong sequence", "right sequence"), false},
		{terror.ErrSyncerShardDDLConflict.Generate("conflict DDL", "conflict"), true},
		// others
		{nil, true},
		{errors.New("unknown error"), true},
		{terror.ErrNotSet.Delegate(&tmysql.SQLError{Code: 1236, Message: "Could not find first log file name in binary log index file", State: tmysql.DefaultMySQLState}), false},
		{terror.ErrNotSet.Delegate(&tmysql.SQLError{Code: 1236, Message: "The slave is connecting using CHANGE MASTER TO MASTER_AUTO_POSITION = 1, but the master has purged binary logs containing GTIDs that the slave requires", State: tmysql.DefaultMySQLState}), false},
	}

	for _, tc := range testCases {
		err := unit.NewProcessError(tc.err)
		c.Assert(isResumableError(err), check.Equals, tc.resumable)
	}
}
