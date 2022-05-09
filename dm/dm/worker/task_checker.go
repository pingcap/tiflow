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
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/failpoint"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/backoff"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/retry"
	"github.com/pingcap/tiflow/dm/pkg/terror"
)

// Backoff related constants
// var (
// 	DefaultCheckInterval           = 5 * time.Second
// 	DefaultBackoffRollback         = 5 * time.Minute
// 	DefaultBackoffMin              = 1 * time.Second
// 	DefaultBackoffMax              = 5 * time.Minute
// 	DefaultBackoffJitter           = true
// 	DefaultBackoffFactor   float64 = 2
// )

// ResumeStrategy represents what we can do when we meet a paused task in task status checker.
type ResumeStrategy int

// resume strategies, in each round of `check`, the checker will apply one of the following strategies
// to a given task based on its `state`, `result` from `SubTaskStatus` and backoff information recored
// in task status checker.
// operation of different strategies:
// ResumeIgnore:
//	1. check duration since latestPausedTime, if larger than backoff rollback, rollback backoff once
// ResumeNoSense:
//	1. update latestPausedTime
//	2. update latestBlockTime
// ResumeSkip:
//	1. update latestPausedTime
// ResumeDispatch:
//	1. update latestPausedTime
//	2. dispatch auto resume task
//	3. if step2 successes, update latestResumeTime, forward backoff
const (
	// When a task is not in paused state, or paused by manually, or we can't get enough information from worker
	// to determine whether this task is paused because of some error, we will apply ResumeIgnore strategy, and
	// do nothing with this task in this check round.
	ResumeIgnore ResumeStrategy = iota + 1
	// When checker detects a paused task can recover synchronization by resume, but its last auto resume
	// duration is less than backoff waiting time, we will apply ResumeSkip strategy, and skip auto resume
	// for this task in this check round.
	ResumeSkip
	// When checker detects a task is paused because of some un-resumable error, such as paused because of
	// executing incompatible DDL to downstream, we will apply ResumeNoSense strategy.
	ResumeNoSense
	// ResumeDispatch means we will dispatch an auto resume operation in this check round for the paused task.
	ResumeDispatch
)

var resumeStrategy2Str = map[ResumeStrategy]string{
	ResumeIgnore:   "ignore task",
	ResumeSkip:     "skip task resume",
	ResumeNoSense:  "resume task makes no sense",
	ResumeDispatch: "dispatch auto resume",
}

// String implements fmt.Stringer interface.
func (bs ResumeStrategy) String() string {
	if s, ok := resumeStrategy2Str[bs]; ok {
		return s
	}
	return fmt.Sprintf("unsupported resume strategy: %d", bs)
}

// TaskStatusChecker is an interface that defines how we manage task status.
type TaskStatusChecker interface {
	// Init initializes the checker
	Init() error
	// Start starts the checker
	Start()
	// Close closes the checker
	Close()
}

// NewTaskStatusChecker is a TaskStatusChecker initializer.
var NewTaskStatusChecker = NewRealTaskStatusChecker

// AutoResumeInfo contains some Time and Backoff that are related to auto resume.
// This structure is exposed for DM as library.
type AutoResumeInfo struct {
	Backoff          *backoff.Backoff
	LatestPausedTime time.Time
	LatestBlockTime  time.Time
	LatestResumeTime time.Time
}

// realTaskStatusChecker is not thread-safe.
// It runs asynchronously against DM-worker, and task information may be updated
// later than DM-worker, but it is acceptable.
type realTaskStatusChecker struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	closed atomic.Bool

	cfg config.CheckerConfig
	l   log.Logger
	w   *SourceWorker

	subtaskAutoResume map[string]*AutoResumeInfo
	relayAutoResume   *AutoResumeInfo
}

// NewRealTaskStatusChecker creates a new realTaskStatusChecker instance.
func NewRealTaskStatusChecker(cfg config.CheckerConfig, w *SourceWorker) TaskStatusChecker {
	tsc := &realTaskStatusChecker{
		cfg:               cfg,
		l:                 log.With(zap.String("component", "task checker")),
		w:                 w,
		subtaskAutoResume: map[string]*AutoResumeInfo{},
	}
	tsc.closed.Store(true)
	return tsc
}

// Init implements TaskStatusChecker.Init.
func (tsc *realTaskStatusChecker) Init() error {
	// just check configuration of backoff here, lazy creates backoff counter,
	// as we can't get task information before dm-worker starts
	_, err := backoff.NewBackoff(tsc.cfg.BackoffFactor, tsc.cfg.BackoffJitter, tsc.cfg.BackoffMin.Duration, tsc.cfg.BackoffMax.Duration)
	return terror.WithClass(err, terror.ClassDMWorker)
}

// Start implements TaskStatusChecker.Start.
func (tsc *realTaskStatusChecker) Start() {
	tsc.wg.Add(1)
	go func() {
		defer tsc.wg.Done()
		tsc.run()
	}()
}

// Close implements TaskStatusChecker.Close.
func (tsc *realTaskStatusChecker) Close() {
	if !tsc.closed.CAS(false, true) {
		return
	}

	if tsc.cancel != nil {
		tsc.cancel()
	}
	tsc.wg.Wait()
}

func (tsc *realTaskStatusChecker) run() {
	// keep running until canceled in `Close`.
	tsc.ctx, tsc.cancel = context.WithCancel(context.Background())
	tsc.closed.Store(false)

	failpoint.Inject("TaskCheckInterval", func(val failpoint.Value) {
		interval, err := time.ParseDuration(val.(string))
		if err != nil {
			tsc.l.Warn("inject failpoint TaskCheckInterval failed", zap.Reflect("value", val), zap.Error(err))
		} else {
			tsc.cfg.CheckInterval = config.Duration{Duration: interval}
			tsc.l.Info("set TaskCheckInterval", zap.String("failpoint", "TaskCheckInterval"), zap.Duration("value", interval))
		}
	})

	ticker := time.NewTicker(tsc.cfg.CheckInterval.Duration)
	defer ticker.Stop()
	for {
		select {
		case <-tsc.ctx.Done():
			tsc.l.Info("worker task checker exits")
			return
		case <-ticker.C:
			tsc.check()
		}
	}
}

// isResumableError checks the error message and returns whether we need to
// resume the task and retry.
func isResumableError(err *pb.ProcessError) bool {
	if err == nil {
		return true
	}

	// not elegant code, because TiDB doesn't expose some error
	for _, msg := range retry.UnsupportedDDLMsgs {
		if strings.Contains(strings.ToLower(err.RawCause), strings.ToLower(msg)) {
			return false
		}
	}
	for _, msg := range retry.UnsupportedDMLMsgs {
		if strings.Contains(strings.ToLower(err.RawCause), strings.ToLower(msg)) {
			return false
		}
	}
	for _, msg := range retry.ReplicationErrMsgs {
		if strings.Contains(strings.ToLower(err.RawCause), strings.ToLower(msg)) {
			return false
		}
	}
	if err.ErrCode == int32(terror.ErrParserParseRelayLog.Code()) {
		for _, msg := range retry.ParseRelayLogErrMsgs {
			if strings.Contains(strings.ToLower(err.Message), strings.ToLower(msg)) {
				return false
			}
		}
	}
	if _, ok := retry.UnresumableErrCodes[err.ErrCode]; ok {
		return false
	}

	return true
}

// CheckResumeSubtask updates info and returns ResumeStrategy for a subtask.
// When ResumeDispatch and the subtask is successfully resumed at caller,
// LatestResumeTime and backoff should be updated.
// This function is exposed for DM as library.
func (i *AutoResumeInfo) CheckResumeSubtask(
	stStatus *pb.SubTaskStatus,
	backoffRollback time.Duration,
) (strategy ResumeStrategy) {
	defer func() {
		i.update(strategy, backoffRollback)
	}()

	// task that is not paused or paused manually, just ignore it
	if stStatus == nil || stStatus.Stage != pb.Stage_Paused || stStatus.Result == nil || stStatus.Result.IsCanceled {
		return ResumeIgnore
	}

	// TODO: use different strategies based on the error detail
	for _, processErr := range stStatus.Result.Errors {
		pErr := processErr
		if !isResumableError(processErr) {
			failpoint.Inject("TaskCheckInterval", func(_ failpoint.Value) {
				log.L().Info("error is not resumable", zap.Stringer("error", pErr))
			})
			return ResumeNoSense
		}
	}

	// auto resume interval does not exceed backoff duration, skip this paused task
	if time.Since(i.LatestResumeTime) < i.Backoff.Current() {
		return ResumeSkip
	}

	return ResumeDispatch
}

func (i *AutoResumeInfo) checkResumeRelay(
	relayStatus *pb.RelayStatus,
	backoffRollback time.Duration,
) (strategy ResumeStrategy) {
	defer func() {
		i.update(strategy, backoffRollback)
	}()

	// relay that is not paused or paused manually, just ignore it
	if relayStatus == nil || relayStatus.Stage != pb.Stage_Paused || relayStatus.Result == nil || relayStatus.Result.IsCanceled {
		return ResumeIgnore
	}

	for _, err := range relayStatus.Result.Errors {
		if _, ok := retry.UnresumableRelayErrCodes[err.ErrCode]; ok {
			return ResumeNoSense
		}
	}

	if time.Since(i.LatestResumeTime) < i.Backoff.Current() {
		return ResumeSkip
	}

	return ResumeDispatch
}

func (i *AutoResumeInfo) update(strategy ResumeStrategy, backoffRollback time.Duration) {
	switch strategy {
	case ResumeIgnore:
		if time.Since(i.LatestPausedTime) > backoffRollback {
			i.Backoff.Rollback()
			// after each rollback, reset this timer
			i.LatestPausedTime = time.Now()
		}
	case ResumeNoSense:
		// this strategy doesn't forward or rollback backoff
		i.LatestPausedTime = time.Now()
		if i.LatestBlockTime.IsZero() {
			i.LatestBlockTime = time.Now()
		}
	case ResumeSkip, ResumeDispatch:
		i.LatestPausedTime = time.Now()
	}
}

func (tsc *realTaskStatusChecker) checkRelayStatus() {
	relayStatus := tsc.w.relayHolder.Status(nil)
	if tsc.relayAutoResume == nil {
		bf, _ := backoff.NewBackoff(
			tsc.cfg.BackoffFactor,
			tsc.cfg.BackoffJitter,
			tsc.cfg.BackoffMin.Duration,
			tsc.cfg.BackoffMax.Duration)
		tsc.relayAutoResume = &AutoResumeInfo{
			Backoff:          bf,
			LatestResumeTime: time.Now(),
			LatestPausedTime: time.Now(),
		}
	}

	strategy := tsc.relayAutoResume.checkResumeRelay(relayStatus, tsc.cfg.BackoffRollback.Duration)
	switch strategy {
	case ResumeNoSense:
		tsc.l.Warn("relay can't auto resume",
			zap.Duration("paused duration", time.Since(tsc.relayAutoResume.LatestBlockTime)))
	case ResumeSkip:
		tsc.l.Warn("backoff skip auto resume relay",
			zap.Time("latestResumeTime", tsc.relayAutoResume.LatestResumeTime),
			zap.Duration("duration", tsc.relayAutoResume.Backoff.Current()))
	case ResumeDispatch:
		err := tsc.w.operateRelay(tsc.ctx, pb.RelayOp_ResumeRelay)
		if err != nil {
			tsc.l.Error("dispatch auto resume relay failed", zap.Error(err))
		} else {
			tsc.l.Info("dispatch auto resume relay")
			tsc.relayAutoResume.LatestResumeTime = time.Now()
			tsc.relayAutoResume.Backoff.BoundaryForward()
		}
	}
}

func (tsc *realTaskStatusChecker) checkTaskStatus() {
	allSubTaskStatus := tsc.w.getAllSubTaskStatus()

	defer func() {
		// cleanup outdated tasks
		for taskName := range tsc.subtaskAutoResume {
			_, ok := allSubTaskStatus[taskName]
			if !ok {
				tsc.l.Debug("remove task from checker", zap.String("task", taskName))
				delete(tsc.subtaskAutoResume, taskName)
			}
		}
	}()

	for taskName, stStatus := range allSubTaskStatus {
		info, ok := tsc.subtaskAutoResume[taskName]
		if !ok {
			bf, _ := backoff.NewBackoff(
				tsc.cfg.BackoffFactor,
				tsc.cfg.BackoffJitter,
				tsc.cfg.BackoffMin.Duration,
				tsc.cfg.BackoffMax.Duration)
			info = &AutoResumeInfo{
				Backoff:          bf,
				LatestPausedTime: time.Now(),
				LatestResumeTime: time.Now(),
			}
			tsc.subtaskAutoResume[taskName] = info
		}
		strategy := info.CheckResumeSubtask(stStatus, tsc.cfg.BackoffRollback.Duration)
		switch strategy {
		case ResumeNoSense:
			tsc.l.Warn("task can't auto resume",
				zap.String("task", taskName),
				zap.Duration("paused duration", time.Since(info.LatestBlockTime)))
		case ResumeSkip:
			tsc.l.Warn("backoff skip auto resume task",
				zap.String("task", taskName),
				zap.Time("latestResumeTime", info.LatestResumeTime),
				zap.Duration("duration", info.Backoff.Current()))
		case ResumeDispatch:
			err := tsc.w.OperateSubTask(taskName, pb.TaskOp_AutoResume)
			if err != nil {
				tsc.l.Error("dispatch auto resume task failed", zap.String("task", taskName), zap.Error(err))
			} else {
				tsc.l.Info("dispatch auto resume task", zap.String("task", taskName))
				info.LatestResumeTime = time.Now()
				info.Backoff.BoundaryForward()
			}
		}
	}
}

func (tsc *realTaskStatusChecker) check() {
	if tsc.w.relayEnabled.Load() {
		tsc.checkRelayStatus()
	}
	tsc.checkTaskStatus()
}
