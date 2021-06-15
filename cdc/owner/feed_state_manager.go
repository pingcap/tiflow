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

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"go.uber.org/zap"
)

// feedStateManager manages the feedState of a changefeed
// when the error, admin job happened, the feedStateManager is responsible for controlling the feedState
type feedStateManager struct {
	state           *model.ChangefeedReactorState
	shouldBeRunning bool

	adminJobQueue []*model.AdminJob
}

func (m *feedStateManager) Tick(state *model.ChangefeedReactorState) {
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
	case model.StateStopped, model.StateFailed, model.StateRemoved, model.StateFinished:
		m.shouldBeRunning = false
		return
	}
	errs := m.errorsReportedByProcessors()
	m.HandleError(errs...)
}

func (m *feedStateManager) ShouldRunning() bool {
	return m.shouldBeRunning
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
		log.Panic("Can not handle this job", zap.String("changefeedID", m.state.ID),
			zap.String("changefeedState", string(m.state.Info.State)), zap.Any("job", job))
	}
	m.pushAdminJob(job)
}

func (m *feedStateManager) handleAdminJob() (jobsPending bool) {
	job := m.popAdminJob()
	if job == nil || job.CfID != m.state.ID {
		return false
	}
	log.Info("handle admin job", zap.String("changefeedID", m.state.ID), zap.Reflect("job", job))
	switch job.Type {
	case model.AdminStop:
		switch m.state.Info.State {
		case model.StateNormal, model.StateError:
		default:
			log.Warn("can not pause the changefeed in the current state", zap.String("changefeedID", m.state.ID),
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
			log.Warn("can not remove the changefeed in the current state", zap.String("changefeedID", m.state.ID),
				zap.String("changefeedState", string(m.state.Info.State)), zap.Any("job", job))
			return
		}
		m.shouldBeRunning = false
		jobsPending = true
		m.patchState(model.StateRemoved)
		// remove changefeed info and state
		m.state.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
			return nil, true, nil
		})
		m.state.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
			return nil, true, nil
		})

	case model.AdminResume:
		switch m.state.Info.State {
		case model.StateFailed, model.StateError, model.StateStopped, model.StateFinished:
		default:
			log.Warn("can not resume the changefeed in the current state", zap.String("changefeedID", m.state.ID),
				zap.String("changefeedState", string(m.state.Info.State)), zap.Any("job", job))
			return
		}
		m.shouldBeRunning = true
		jobsPending = true
		m.patchState(model.StateNormal)
		// remove error history to make sure the changefeed can running in next tick
		m.state.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
			if info.Error != nil || len(info.ErrorHis) != 0 {
				info.Error = nil
				info.ErrorHis = nil
				return info, true, nil
			}
			return info, false, nil
		})
	case model.AdminFinish:
		switch m.state.Info.State {
		case model.StateNormal:
		default:
			log.Warn("can not finish the changefeed in the current state", zap.String("changefeedID", m.state.ID),
				zap.String("changefeedState", string(m.state.Info.State)), zap.Any("job", job))
			return
		}
		m.shouldBeRunning = false
		jobsPending = true
		m.patchState(model.StateStopped)
	default:
		log.Warn("Unknown admin job", zap.Any("adminJob", job), zap.String("changefeed", m.state.ID))
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
	for captureID := range m.state.TaskStatuses {
		m.state.PatchTaskStatus(captureID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
			return nil, status != nil, nil
		})
	}
	for captureID := range m.state.TaskPositions {
		m.state.PatchTaskPosition(captureID, func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
			return nil, position != nil, nil
		})
	}
	for captureID := range m.state.Workloads {
		m.state.PatchTaskWorkload(captureID, func(workload model.TaskWorkload) (model.TaskWorkload, bool, error) {
			return nil, workload != nil, nil
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
			log.Error("processor report an error", zap.String("changefeedID", m.state.ID), zap.String("captureID", captureID), zap.Any("error", position.Error))
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

func (m *feedStateManager) HandleError(errs ...*model.RunningError) {
	m.state.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
		for _, err := range errs {
			info.Error = err
			info.ErrorHis = append(info.ErrorHis, time.Now().UnixNano()/1e6)
		}
		needSave := info.CleanUpOutdatedErrorHistory()
		return info, needSave || len(errs) > 0, nil
	})
	var err *model.RunningError
	if len(errs) > 0 {
		err = errs[len(errs)-1]
	}
	// if one of the error stored by changefeed state(error in the last tick) or the error specified by this function(error in the this tick)
	// is a fast-fail error, the changefeed should be failed
	if m.state.Info.HasFastFailError() || (err != nil && cerrors.ChangefeedFastFailErrorCode(errors.RFCErrorCode(err.Code))) {
		m.shouldBeRunning = false
		m.patchState(model.StateFailed)
		return
	}
	// if the number of errors has reached the error threshold, stop the changefeed
	if m.state.Info.ErrorsReachedThreshold() {
		m.shouldBeRunning = false
		m.patchState(model.StateError)
		return
	}
}
