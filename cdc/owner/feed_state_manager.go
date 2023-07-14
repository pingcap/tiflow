// Copyright 2021 PingCAP, Inc.
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

package owner

import (
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"go.uber.org/zap"
)

const (
	// When errors occurred, and we need to do backoff, we start an exponential backoff
	// with an interval from 10s to 30min (10s, 20s, 40s, 80s, 160s, 320s, 640s, 1280s, 1800s).
	// And the backoff will be stopped after 72 min (about 9 tries) because if we do another 30min backoff,
	// the total duration (72+30=102min) will exceed the MaxElapsedTime (90min).
	// To avoid thunderherd, a random factor is also added.
	defaultBackoffInitInterval        = 10 * time.Second
	defaultBackoffMaxInterval         = 30 * time.Minute
	defaultBackoffMaxElapsedTime      = 90 * time.Minute
	defaultBackoffRandomizationFactor = 0.1
	defaultBackoffMultiplier          = 2.0

	// If all states recorded in window are 'normal', it can be assumed that the changefeed
	// is running steady. And then if we enter a state other than normal at next tick,
	// the backoff must be reset.
	defaultStateWindowSize = 512
)

// feedStateManager manages the ReactorState of a changefeed
// when an error or an admin job occurs, the feedStateManager is responsible for controlling the ReactorState
type feedStateManager struct {
	state           *orchestrator.ChangefeedReactorState
	shouldBeRunning bool
	// Based on shouldBeRunning = false
	// shouldBeRemoved = true means the changefeed is removed
	// shouldBeRemoved = false means the changefeed is paused
	shouldBeRemoved bool

	adminJobQueue   []*model.AdminJob
	stateHistory    [defaultStateWindowSize]model.FeedState
	lastErrorTime   time.Time                   // time of last error for a changefeed
	backoffInterval time.Duration               // the interval for restarting a changefeed in 'error' state
	errBackoff      *backoff.ExponentialBackOff // an exponential backoff for restarting a changefeed
}

// newFeedStateManager creates feedStateManager and initialize the exponential backoff
func newFeedStateManager() *feedStateManager {
	f := new(feedStateManager)

	f.errBackoff = backoff.NewExponentialBackOff()
	f.errBackoff.InitialInterval = defaultBackoffInitInterval
	f.errBackoff.MaxInterval = defaultBackoffMaxInterval
	f.errBackoff.MaxElapsedTime = defaultBackoffMaxElapsedTime
	f.errBackoff.Multiplier = defaultBackoffMultiplier
	f.errBackoff.RandomizationFactor = defaultBackoffRandomizationFactor

	f.resetErrBackoff()
	f.lastErrorTime = time.Unix(0, 0)

	return f
}

// newFeedStateManager4Test creates feedStateManager for test
func newFeedStateManager4Test() *feedStateManager {
	f := new(feedStateManager)

	f.errBackoff = backoff.NewExponentialBackOff()
	f.errBackoff.InitialInterval = 200 * time.Millisecond
	f.errBackoff.MaxInterval = 1600 * time.Millisecond
	f.errBackoff.MaxElapsedTime = 6 * time.Second
	f.errBackoff.Multiplier = 2.0
	f.errBackoff.RandomizationFactor = 0

	f.resetErrBackoff()
	f.lastErrorTime = time.Unix(0, 0)

	return f
}

// resetErrBackoff reset the backoff-related fields
func (m *feedStateManager) resetErrBackoff() {
	m.errBackoff.Reset()
	m.backoffInterval = m.errBackoff.NextBackOff()
}

// isChangefeedStable check if there are states other than 'normal' in this sliding window.
func (m *feedStateManager) isChangefeedStable() bool {
	for _, val := range m.stateHistory {
		if val != model.StateNormal {
			return false
		}
	}

	return true
}

// shiftStateWindow shift the sliding window
func (m *feedStateManager) shiftStateWindow(state model.FeedState) {
	for i := 0; i < defaultStateWindowSize-1; i++ {
		m.stateHistory[i] = m.stateHistory[i+1]
	}

	m.stateHistory[defaultStateWindowSize-1] = state
}

func (m *feedStateManager) Tick(state *orchestrator.ChangefeedReactorState) {
	m.state = state
	m.shouldBeRunning = true
	defer func() {
		if m.shouldBeRunning {
			m.patchState(model.StateNormal)
		} else {
			m.cleanUpInfos()
		}
	}()
	if m.handleAdminJob() {
		// `handleAdminJob` returns true means that some admin jobs are pending
		// skip to the next tick until all the admin jobs is handled
		return
	}
	switch m.state.Info.State {
	case model.StateRemoved:
		m.shouldBeRunning = false
		m.shouldBeRemoved = true
		return
	case model.StateStopped, model.StateFailed, model.StateFinished:
		m.shouldBeRunning = false
		return
	}
	errs := m.errorsReportedByProcessors()
	m.handleError(errs...)
}

func (m *feedStateManager) ShouldRunning() bool {
	return m.shouldBeRunning
}

func (m *feedStateManager) ShouldRemoved() bool {
	return m.shouldBeRemoved
}

func (m *feedStateManager) MarkFinished() {
	if m.state == nil {
		// when state is nil, it means that Tick has never been called
		// skip this and wait for the next tick to finish the changefeed
		return
	}
	m.pushAdminJob(&model.AdminJob{
		CfID: m.state.ID,
		Type: model.AdminFinish,
	})
}

func (m *feedStateManager) PushAdminJob(job *model.AdminJob) {
	switch job.Type {
	case model.AdminStop, model.AdminResume, model.AdminRemove:
	default:
		log.Panic("Can not handle this job",
			zap.String("namespace", m.state.ID.Namespace),
			zap.String("changefeed", m.state.ID.ID),
			zap.String("changefeedState", string(m.state.Info.State)), zap.Any("job", job))
	}
	m.pushAdminJob(job)
}

func (m *feedStateManager) handleAdminJob() (jobsPending bool) {
	job := m.popAdminJob()
	if job == nil || job.CfID != m.state.ID {
		return false
	}
	log.Info("handle admin job",
		zap.String("namespace", m.state.ID.Namespace),
		zap.String("changefeed", m.state.ID.ID), zap.Reflect("job", job))
	switch job.Type {
	case model.AdminStop:
		switch m.state.Info.State {
		case model.StateNormal, model.StateError:
		default:
			log.Warn("can not pause the changefeed in the current state",
				zap.String("namespace", m.state.ID.Namespace),
				zap.String("changefeed", m.state.ID.ID),
				zap.String("changefeedState", string(m.state.Info.State)), zap.Any("job", job))
			return
		}
		m.shouldBeRunning = false
		jobsPending = true
		m.patchState(model.StateStopped)
	case model.AdminRemove:
		switch m.state.Info.State {
		case model.StateNormal, model.StateError, model.StateFailed,
			model.StateStopped, model.StateFinished, model.StateRemoved:
		default:
			log.Warn("can not remove the changefeed in the current state",
				zap.String("namespace", m.state.ID.Namespace),
				zap.String("changefeed", m.state.ID.ID),
				zap.String("changefeedState", string(m.state.Info.State)), zap.Any("job", job))
			return
		}
		m.shouldBeRunning = false
		m.shouldBeRemoved = true
		jobsPending = true

		// remove changefeedInfo
		m.state.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
			return nil, true, nil
		})
		// remove changefeedStatus
		m.state.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
			return nil, true, nil
		})
		checkpointTs := m.state.Info.GetCheckpointTs(m.state.Status)

		log.Info("the changefeed is removed",
			zap.String("namespace", m.state.ID.Namespace),
			zap.String("changefeed", m.state.ID.ID),
			zap.Uint64("checkpointTs", checkpointTs))
	case model.AdminResume:
		switch m.state.Info.State {
		case model.StateFailed, model.StateError, model.StateStopped, model.StateFinished:
		default:
			log.Warn("can not resume the changefeed in the current state",
				zap.String("namespace", m.state.ID.Namespace),
				zap.String("changefeed", m.state.ID.ID),
				zap.String("changefeedState", string(m.state.Info.State)), zap.Any("job", job))
			return
		}
		m.shouldBeRunning = true
		// when the changefeed is manually resumed, we must reset the backoff
		m.resetErrBackoff()
		// The lastErrorTime also needs to be cleared before a fresh run.
		m.lastErrorTime = time.Unix(0, 0)
		jobsPending = true
		m.patchState(model.StateNormal)
		m.state.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
			if info == nil {
				return nil, false, nil
			}
			if info.Error != nil {
				info.Error = nil
				return info, true, nil
			}
			return info, false, nil
		})
	case model.AdminFinish:
		switch m.state.Info.State {
		case model.StateNormal:
		default:
			log.Warn("can not finish the changefeed in the current state",
				zap.String("namespace", m.state.ID.Namespace),
				zap.String("changefeed", m.state.ID.ID),
				zap.String("changefeedState", string(m.state.Info.State)), zap.Any("job", job))
			return
		}
		m.shouldBeRunning = false
		jobsPending = true
		m.patchState(model.StateFinished)
	default:
		log.Warn("Unknown admin job", zap.Any("adminJob", job),
			zap.String("namespace", m.state.ID.Namespace),
			zap.String("changefeed", m.state.ID.ID))
	}
	return
}

func (m *feedStateManager) popAdminJob() *model.AdminJob {
	if len(m.adminJobQueue) == 0 {
		return nil
	}
	job := m.adminJobQueue[0]
	m.adminJobQueue = m.adminJobQueue[1:]
	return job
}

func (m *feedStateManager) pushAdminJob(job *model.AdminJob) {
	m.adminJobQueue = append(m.adminJobQueue, job)
}

func (m *feedStateManager) patchState(feedState model.FeedState) {
	var adminJobType model.AdminJobType
	switch feedState {
	case model.StateNormal:
		adminJobType = model.AdminNone
	case model.StateFinished:
		adminJobType = model.AdminFinish
	case model.StateError, model.StateStopped, model.StateFailed:
		adminJobType = model.AdminStop
	case model.StateRemoved:
		adminJobType = model.AdminRemove
	default:
		log.Panic("Unreachable")
	}
	m.state.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		if status == nil {
			return status, false, nil
		}
		if status.AdminJobType != adminJobType {
			status.AdminJobType = adminJobType
			return status, true, nil
		}
		return status, false, nil
	})
	m.state.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
		changed := false
		if info == nil {
			return nil, changed, nil
		}
		if info.State != feedState {
			info.State = feedState
			changed = true
		}
		if info.AdminJobType != adminJobType {
			info.AdminJobType = adminJobType
			changed = true
		}
		return info, changed, nil
	})
}

func (m *feedStateManager) cleanUpInfos() {
	for captureID := range m.state.TaskPositions {
		m.state.PatchTaskPosition(captureID, func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
			return nil, position != nil, nil
		})
	}
}

func (m *feedStateManager) errorsReportedByProcessors() []*model.RunningError {
	var runningErrors map[string]*model.RunningError
	for captureID, position := range m.state.TaskPositions {
		if position.Error != nil {
			if runningErrors == nil {
				runningErrors = make(map[string]*model.RunningError)
			}
			runningErrors[position.Error.Code] = position.Error
			log.Error("processor report an error",
				zap.String("namespace", m.state.ID.Namespace),
				zap.String("changefeed", m.state.ID.ID),
				zap.String("captureID", captureID), zap.Any("error", position.Error))
			m.state.PatchTaskPosition(captureID, func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
				if position == nil {
					return nil, false, nil
				}
				position.Error = nil
				return position, true, nil
			})
		}
	}
	if runningErrors == nil {
		return nil
	}
	result := make([]*model.RunningError, 0, len(runningErrors))
	for _, err := range runningErrors {
		result = append(result, err)
	}
	return result
}

func (m *feedStateManager) handleError(errs ...*model.RunningError) {
	// if there are a fastFail error in errs, we can just fastFail the changefeed
	// and no need to patch other error to the changefeed info
	for _, err := range errs {
		if cerrors.ChangefeedFastFailErrorCode(errors.RFCErrorCode(err.Code)) {
			m.state.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
				if info == nil {
					return nil, false, nil
				}
				info.Error = err
				return info, true, nil
			})
			m.shouldBeRunning = false
			m.patchState(model.StateFailed)
			return
		}
	}

	//  changefeed state from stopped to failed is allowed
	// but stopped to error or normal is not allowed
	if m.state.Info != nil && m.state.Info.State == model.StateStopped {
		log.Warn("changefeed is stopped, ignore errors",
			zap.String("changefeed", m.state.ID.ID),
			zap.String("namespace", m.state.ID.Namespace),
			zap.Any("errors", errs))
		return
	}

	// we need to patch changefeed unretryable error to the changefeed info,
	// so we have to iterate all errs here to check wether it is a unretryable
	// error in errs
	for _, err := range errs {
		if err.IsChangefeedUnRetryableError() {
			m.state.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
				if info == nil {
					return nil, false, nil
				}
				info.Error = err
				return info, true, nil
			})
			m.shouldBeRunning = false
			m.patchState(model.StateError)
			return
		}
	}

	m.state.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
		if info == nil {
			return nil, false, nil
		}
		for _, err := range errs {
			info.Error = err
		}
		return info, len(errs) > 0, nil
	})

	// If we enter into an abnormal state ('error', 'failed') for this changefeed now
	// but haven't seen abnormal states in a sliding window (512 ticks),
	// it can be assumed that this changefeed meets a sudden change from a stable condition.
	// So we can reset the exponential backoff and re-backoff from the InitialInterval.
	// TODO: this detection policy should be added into unit test.
	if len(errs) > 0 {
		m.lastErrorTime = time.Now()
		if m.isChangefeedStable() {
			m.resetErrBackoff()
		}
	} else {
		if m.state.Info.State == model.StateNormal {
			m.lastErrorTime = time.Unix(0, 0)
		}
	}
	m.shiftStateWindow(m.state.Info.State)

	if m.lastErrorTime == time.Unix(0, 0) {
		return
	}

	if time.Since(m.lastErrorTime) < m.backoffInterval {
		m.shouldBeRunning = false
		m.patchState(model.StateError)
	} else {
		oldBackoffInterval := m.backoffInterval
		m.backoffInterval = m.errBackoff.NextBackOff()
		m.lastErrorTime = time.Unix(0, 0)

		// if the duration since backoff start exceeds MaxElapsedTime,
		// we set the state of changefeed to "failed" and don't let it run again unless it is manually resumed.
		if m.backoffInterval == backoff.Stop {
			log.Warn("changefeed will not be restarted because it has been failing for a long time period",
				zap.Duration("maxElapsedTime", m.errBackoff.MaxElapsedTime))
			m.shouldBeRunning = false
			m.patchState(model.StateError)
		} else {
			log.Info("changefeed restart backoff interval is changed",
				zap.String("namespace", m.state.ID.Namespace),
				zap.String("changefeed", m.state.ID.ID),
				zap.Duration("oldInterval", oldBackoffInterval), zap.Duration("newInterval", m.backoffInterval))
		}
	}
}
