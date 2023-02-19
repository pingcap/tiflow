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
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
<<<<<<< HEAD
=======
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"github.com/pingcap/tiflow/pkg/upstream"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
>>>>>>> 0867f80e5f (cdc: add changefeed epoch to prevent unexpected state (#8268))
	"go.uber.org/zap"
)

// feedStateManager manages the ReactorState of a changefeed
// when an error or an admin job occurs, the feedStateManager is responsible for controlling the ReactorState
type feedStateManager struct {
<<<<<<< HEAD
	state           *model.ChangefeedReactorState
=======
	upstream        *upstream.Upstream
	state           *orchestrator.ChangefeedReactorState
>>>>>>> 0867f80e5f (cdc: add changefeed epoch to prevent unexpected state (#8268))
	shouldBeRunning bool

	adminJobQueue []*model.AdminJob
}

<<<<<<< HEAD
func (m *feedStateManager) Tick(state *model.ChangefeedReactorState) {
=======
// newFeedStateManager creates feedStateManager and initialize the exponential backoff
func newFeedStateManager(up *upstream.Upstream) *feedStateManager {
	f := new(feedStateManager)
	f.upstream = up

	f.errBackoff = backoff.NewExponentialBackOff()
	f.errBackoff.InitialInterval = defaultBackoffInitInterval
	f.errBackoff.MaxInterval = defaultBackoffMaxInterval
	f.errBackoff.Multiplier = defaultBackoffMultiplier
	f.errBackoff.RandomizationFactor = defaultBackoffRandomizationFactor
	// MaxElapsedTime=0 means the backoff never stops
	f.errBackoff.MaxElapsedTime = 0

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

func (m *feedStateManager) Tick(state *orchestrator.ChangefeedReactorState) (adminJobPending bool) {
>>>>>>> 0867f80e5f (cdc: add changefeed epoch to prevent unexpected state (#8268))
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
	m.handleError(errs...)
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

		// remove changefeedInfo
		m.state.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
			return nil, true, nil
		})
		// remove changefeedStatus
		m.state.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
			return nil, true, nil
		})
		checkpointTs := m.state.Info.GetCheckpointTs(m.state.Status)
		log.Info("the changefeed is removed", zap.String("changefeed-id", m.state.ID), zap.Uint64("checkpoint-ts", checkpointTs))

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
		m.patchState(model.StateFinished)
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
	var updateEpoch bool
	var adminJobType model.AdminJobType
	switch feedState {
	case model.StateNormal:
		adminJobType = model.AdminNone
		updateEpoch = false
	case model.StateFinished:
		adminJobType = model.AdminFinish
		updateEpoch = true
	case model.StateError, model.StateStopped, model.StateFailed:
		adminJobType = model.AdminStop
		updateEpoch = true
	case model.StateRemoved:
		adminJobType = model.AdminRemove
		updateEpoch = true
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
		if updateEpoch {
			previous := info.Epoch
			ctx, cancel := context.WithTimeout(context.TODO(), 5*time.Second)
			defer cancel()
			info.Epoch = GenerateChangefeedEpoch(ctx, m.upstream.PDClient)
			changed = true
			log.Info("update changefeed epoch",
				zap.String("namespace", m.state.ID.Namespace),
				zap.String("changefeed", m.state.ID.ID),
				zap.Uint64("perviousEpoch", previous),
				zap.Uint64("currentEpoch", info.Epoch))
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

func (m *feedStateManager) handleError(errs ...*model.RunningError) {
	// if there are a fastFail error in errs, we can just fastFail the changefeed
	// and no need to patch other error to the changefeed info
	for _, err := range errs {
		if cerrors.ChangefeedFastFailErrorCode(errors.RFCErrorCode(err.Code)) {
			m.state.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
				info.Error = err
				info.ErrorHis = append(info.ErrorHis, time.Now().UnixNano()/1e6)
				info.CleanUpOutdatedErrorHistory()
				return info, true, nil
			})
			m.shouldBeRunning = false
			m.patchState(model.StateFailed)
			return
		}
	}

	m.state.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
		for _, err := range errs {
			info.Error = err
			info.ErrorHis = append(info.ErrorHis, time.Now().UnixNano()/1e6)
		}
		changed := info.CleanUpOutdatedErrorHistory()
		return info, changed || len(errs) > 0, nil
	})

	// if the number of errors has reached the error threshold, stop the changefeed
	if m.state.Info.ErrorsReachedThreshold() {
		m.shouldBeRunning = false
		m.patchState(model.StateError)
		return
	}
}

// GenerateChangefeedEpoch generates a unique changefeed epoch.
func GenerateChangefeedEpoch(ctx context.Context, pdClient pd.Client) uint64 {
	phyTs, logical, err := pdClient.GetTS(ctx)
	if err != nil {
		log.Warn("generate epoch using local timestamp due to error", zap.Error(err))
		return uint64(time.Now().UnixNano())
	}
	return oracle.ComposeTS(phyTs, logical)
}
