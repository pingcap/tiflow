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

	"github.com/cenkalti/backoff/v4"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/upstream"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

const (
	// When errors occurred, and we need to do backoff, we start an exponential backoff
	// with an interval from 10s to 30min (10s, 20s, 40s, 80s, 160s, 320s,
	//	 600s, 600s, ...).
	// To avoid thunderherd, a random factor is also added.
	defaultBackoffInitInterval        = 10 * time.Second
	defaultBackoffMaxInterval         = 10 * time.Minute
	defaultBackoffRandomizationFactor = 0.1
	defaultBackoffMultiplier          = 2.0
)

// FeedStateManager manages the life cycle of a changefeed, currently it is responsible for:
// 1. Handle admin jobs
// 2. Handle errors
// 3. Handle warnings
// 4. Control the status of a changefeed
type FeedStateManager interface {
	// PushAdminJob pushed an admin job to the admin job queue
	PushAdminJob(job *model.AdminJob)
	// Tick is the main logic of the FeedStateManager, it will be called periodically
	// resolvedTs is the resolvedTs of the changefeed
	// returns true if there is a pending admin job, if so changefeed should not run the tick logic
	Tick(resolvedTs model.Ts, status *model.ChangeFeedStatus, info *model.ChangeFeedInfo) (adminJobPending bool)
	// HandleError is called an error occurs in Changefeed.Tick
	HandleError(errs ...*model.RunningError)
	// HandleWarning is called a warning occurs in Changefeed.Tick
	HandleWarning(warnings ...*model.RunningError)
	// ShouldRunning returns if the changefeed should be running
	ShouldRunning() bool
	// ShouldRemoved returns if the changefeed should be removed
	ShouldRemoved() bool
	// MarkFinished is call when a changefeed is finished
	MarkFinished()
}

// feedStateManager manages the ReactorState of a changefeed
// when an error or an admin job occurs, the feedStateManager is responsible for controlling the ReactorState
type feedStateManager struct {
	upstream *upstream.Upstream
	state    ChangefeedState

	shouldBeRunning bool
	// Based on shouldBeRunning = false
	// shouldBeRemoved = true means the changefeed is removed
	// shouldBeRemoved = false means the changefeed is paused
	shouldBeRemoved bool

	adminJobQueue                 []*model.AdminJob
	isRetrying                    bool
	lastErrorRetryTime            time.Time                   // time of last error for a changefeed
	lastErrorRetryCheckpointTs    model.Ts                    // checkpoint ts of last retry
	lastWarningReportCheckpointTs model.Ts                    // checkpoint ts of last warning report
	backoffInterval               time.Duration               // the interval for restarting a changefeed in 'error' state
	errBackoff                    *backoff.ExponentialBackOff // an exponential backoff for restarting a changefeed

	// resolvedTs and initCheckpointTs is for checking whether resolved timestamp
	// has been advanced or not.
	resolvedTs           model.Ts
	checkpointTs         model.Ts
	checkpointTsAdvanced time.Time

	changefeedErrorStuckDuration time.Duration
}

// NewFeedStateManager creates feedStateManager and initialize the exponential backoff
func NewFeedStateManager(up *upstream.Upstream,
	state ChangefeedState,
) FeedStateManager {
	m := new(feedStateManager)
	m.upstream = up
	m.state = state

	m.errBackoff = backoff.NewExponentialBackOff()
	m.errBackoff.InitialInterval = defaultBackoffInitInterval
	m.errBackoff.MaxInterval = defaultBackoffMaxInterval
	m.errBackoff.Multiplier = defaultBackoffMultiplier
	m.errBackoff.RandomizationFactor = defaultBackoffRandomizationFactor
	// backoff will stop once the defaultBackoffMaxElapsedTime has elapsed.
	m.errBackoff.MaxElapsedTime = *state.GetChangefeedInfo().Config.ChangefeedErrorStuckDuration
	m.changefeedErrorStuckDuration = *state.GetChangefeedInfo().Config.ChangefeedErrorStuckDuration

	m.resetErrRetry()
	m.isRetrying = false
	return m
}

func (m *feedStateManager) shouldRetry() bool {
	// changefeed should not retry within [m.lastErrorRetryTime, m.lastErrorRetryTime + m.backoffInterval).
	return time.Since(m.lastErrorRetryTime) >= m.backoffInterval
}

func (m *feedStateManager) shouldFailWhenRetry() bool {
	// retry the changefeed
	m.backoffInterval = m.errBackoff.NextBackOff()
	// NextBackOff() will return -1 once the MaxElapsedTime has elapsed,
	// set the changefeed to failed state.
	if m.backoffInterval == m.errBackoff.Stop {
		return true
	}

	m.lastErrorRetryTime = time.Now()
	return false
}

// resetErrRetry reset the error retry related fields
func (m *feedStateManager) resetErrRetry() {
	m.errBackoff.Reset()
	m.backoffInterval = m.errBackoff.NextBackOff()
	m.lastErrorRetryTime = time.Unix(0, 0)
}

func (m *feedStateManager) Tick(resolvedTs model.Ts,
	status *model.ChangeFeedStatus, info *model.ChangeFeedInfo,
) (adminJobPending bool) {
	m.checkAndInitLastRetryCheckpointTs(status)

	if status != nil {
		if m.checkpointTs < status.CheckpointTs {
			m.checkpointTs = status.CheckpointTs
			m.checkpointTsAdvanced = time.Now()
		}
		if m.resolvedTs < resolvedTs {
			m.resolvedTs = resolvedTs
		}
		if m.checkpointTs >= m.resolvedTs {
			m.checkpointTsAdvanced = time.Now()
		}
	}

	m.shouldBeRunning = true
	defer func() {
		if !m.shouldBeRunning {
			m.cleanUp()
		}
	}()

	if m.handleAdminJob() {
		// `handleAdminJob` returns true means that some admin jobs are pending
		// skip to the next tick until all the admin jobs is handled
		adminJobPending = true
		return
	}

	switch info.State {
	case model.StateUnInitialized:
		m.patchState(model.StateNormal)
		return
	case model.StateRemoved:
		m.shouldBeRunning = false
		m.shouldBeRemoved = true
		return
	case model.StateStopped, model.StateFailed, model.StateFinished:
		m.shouldBeRunning = false
		return
	case model.StatePending:
		if !m.shouldRetry() {
			m.shouldBeRunning = false
			return
		}

		if m.shouldFailWhenRetry() {
			log.Error("The changefeed won't be restarted as it has been experiencing failures for "+
				"an extended duration",
				zap.Duration("maxElapsedTime", m.errBackoff.MaxElapsedTime),
				zap.String("namespace", m.state.GetID().Namespace),
				zap.String("changefeed", m.state.GetID().ID),
				zap.Time("lastRetryTime", m.lastErrorRetryTime),
				zap.Uint64("lastRetryCheckpointTs", m.lastErrorRetryCheckpointTs),
			)
			m.shouldBeRunning = false
			m.patchState(model.StateFailed)
			return
		}

		// retry the changefeed
		m.shouldBeRunning = true
		if status != nil {
			m.lastErrorRetryCheckpointTs = m.state.GetChangefeedStatus().CheckpointTs
		}
		m.patchState(model.StateWarning)
		log.Info("changefeed retry backoff interval is elapsed,"+
			"chengefeed will be restarted",
			zap.String("namespace", m.state.GetID().Namespace),
			zap.String("changefeed", m.state.GetID().ID),
			zap.Time("lastErrorRetryTime", m.lastErrorRetryTime),
			zap.Duration("nextRetryInterval", m.backoffInterval))
	case model.StateNormal, model.StateWarning:
		m.checkAndChangeState()
		errs := m.state.TakeProcessorErrors()
		m.HandleError(errs...)
		// only handle warnings when there are no errors
		// otherwise, the warnings will cover the errors
		if len(errs) == 0 {
			// warning are come from processors' sink component
			// they ere not fatal errors, so we don't need to stop the changefeed
			warnings := m.state.TakeProcessorWarnings()
			m.HandleWarning(warnings...)
		}
	}
	return
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
		CfID: m.state.GetID(),
		Type: model.AdminFinish,
	})
}

func (m *feedStateManager) PushAdminJob(job *model.AdminJob) {
	switch job.Type {
	case model.AdminStop, model.AdminResume, model.AdminRemove:
	default:
		log.Panic("Can not handle this job",
			zap.String("namespace", m.state.GetID().Namespace),
			zap.String("changefeed", m.state.GetID().ID),
			zap.String("changefeedState", string(m.state.GetChangefeedInfo().State)), zap.Any("job", job))
	}
	m.pushAdminJob(job)
}

func (m *feedStateManager) handleAdminJob() (jobsPending bool) {
	job := m.popAdminJob()
	if job == nil || job.CfID != m.state.GetID() {
		return false
	}
	log.Info("handle admin job",
		zap.String("namespace", m.state.GetID().Namespace),
		zap.String("changefeed", m.state.GetID().ID),
		zap.Any("job", job))
	switch job.Type {
	case model.AdminStop:
		switch m.state.GetChangefeedInfo().State {
		case model.StateNormal, model.StateWarning, model.StatePending:
		default:
			log.Warn("can not pause the changefeed in the current state",
				zap.String("namespace", m.state.GetID().Namespace),
				zap.String("changefeed", m.state.GetID().ID),
				zap.String("changefeedState", string(m.state.GetChangefeedInfo().State)), zap.Any("job", job))
			return
		}
		m.shouldBeRunning = false
		jobsPending = true
		m.patchState(model.StateStopped)
	case model.AdminRemove:
		m.shouldBeRunning = false
		m.shouldBeRemoved = true
		jobsPending = true
		m.state.RemoveChangefeed()
		checkpointTs := m.state.GetChangefeedInfo().GetCheckpointTs(m.state.GetChangefeedStatus())

		log.Info("the changefeed is removed",
			zap.String("namespace", m.state.GetID().Namespace),
			zap.String("changefeed", m.state.GetID().ID),
			zap.Uint64("checkpointTs", checkpointTs))
	case model.AdminResume:
		switch m.state.GetChangefeedInfo().State {
		case model.StateFailed, model.StateStopped, model.StateFinished:
		default:
			log.Warn("can not resume the changefeed in the current state",
				zap.String("namespace", m.state.GetID().Namespace),
				zap.String("changefeed", m.state.GetID().ID),
				zap.String("changefeedState", string(m.state.GetChangefeedInfo().State)), zap.Any("job", job))
			return
		}
		m.shouldBeRunning = true
		// when the changefeed is manually resumed, we must reset the backoff
		m.resetErrRetry()
		m.isRetrying = false
		jobsPending = true
		m.patchState(model.StateNormal)
		m.state.ResumeChnagefeed(job.OverwriteCheckpointTs)

	case model.AdminFinish:
		switch m.state.GetChangefeedInfo().State {
		case model.StateNormal, model.StateWarning:
		default:
			log.Warn("can not finish the changefeed in the current state",
				zap.String("namespace", m.state.GetID().Namespace),
				zap.String("changefeed", m.state.GetID().ID),
				zap.String("changefeedState", string(m.state.GetChangefeedInfo().State)),
				zap.Any("job", job))
			return
		}
		m.shouldBeRunning = false
		jobsPending = true
		m.patchState(model.StateFinished)
	default:
		log.Warn("Unknown admin job", zap.Any("adminJob", job),
			zap.String("namespace", m.state.GetID().Namespace),
			zap.String("changefeed", m.state.GetID().ID),
		)
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
	case model.StateNormal, model.StateWarning:
		adminJobType = model.AdminNone
		updateEpoch = false
	case model.StateFinished:
		adminJobType = model.AdminFinish
		updateEpoch = true
	case model.StatePending, model.StateStopped, model.StateFailed:
		adminJobType = model.AdminStop
		updateEpoch = true
	case model.StateRemoved:
		adminJobType = model.AdminRemove
		updateEpoch = true
	default:
		log.Panic("Unreachable")
	}
	epoch := uint64(0)
	if updateEpoch {
		if updateEpoch {
			ctx, cancel := context.WithTimeout(context.TODO(), 5*time.Second)
			defer cancel()
			epoch = GenerateChangefeedEpoch(ctx, m.upstream.PDClient)
		}
	}
	m.state.UpdateChangefeedState(feedState, adminJobType, epoch)
}

func (m *feedStateManager) cleanUp() {
	m.state.CleanUpTaskPositions()
	m.checkpointTs = 0
	m.checkpointTsAdvanced = time.Time{}
	m.resolvedTs = 0
}

func (m *feedStateManager) HandleError(errs ...*model.RunningError) {
	if len(errs) == 0 {
		return
	}
	// if there are a fastFail error in errs, we can just fastFail the changefeed
	// and no need to patch other error to the changefeed info
	for _, err := range errs {
		if cerrors.IsChangefeedGCFastFailErrorCode(errors.RFCErrorCode(err.Code)) ||
			err.ShouldFailChangefeed() {
			m.state.SetError(err)
			m.shouldBeRunning = false
			m.patchState(model.StateFailed)
			return
		}
	}

	// Changing changefeed state from stopped to failed is allowed
	// but changing changefeed state from stopped to error or normal is not allowed.
	if m.state.GetChangefeedInfo() != nil && m.state.GetChangefeedInfo().State == model.StateStopped {
		log.Warn("changefeed is stopped, ignore errors",
			zap.String("namespace", m.state.GetID().Namespace),
			zap.String("changefeed", m.state.GetID().ID),
			zap.Any("errors", errs))
		return
	}

	var lastError *model.RunningError
	// find the last non nil error
	// BTW, there shouldn't be any nil error in errs
	// this is just a safe guard
	for i := len(errs) - 1; i >= 0; i-- {
		if errs[i] != nil {
			lastError = errs[i]
			break
		}
	}
	// if any error is occurred in this tick, we should set the changefeed state to warning
	// and stop the changefeed
	if lastError != nil {
		log.Warn("changefeed meets an error", zap.Any("error", lastError))
		m.shouldBeRunning = false
		m.patchState(model.StatePending)

		// patch the last error to changefeed info
		m.state.SetError(lastError)

		// The errBackoff needs to be reset before the first retry.
		if !m.isRetrying {
			m.resetErrRetry()
			m.isRetrying = true
		}
	}
}

func (m *feedStateManager) HandleWarning(errs ...*model.RunningError) {
	if len(errs) == 0 {
		return
	}
	lastError := errs[len(errs)-1]

	if m.state.GetChangefeedStatus() != nil {
		currTime := m.upstream.PDClock.CurrentTime()
		ckptTime := oracle.GetTimeFromTS(m.state.GetChangefeedStatus().CheckpointTs)
		m.lastWarningReportCheckpointTs = m.state.GetChangefeedStatus().CheckpointTs

		checkpointTsStuck := time.Since(m.checkpointTsAdvanced) > m.changefeedErrorStuckDuration
		if checkpointTsStuck {
			log.Info("changefeed retry on warning for a very long time and does not resume, "+
				"it will be failed",
				zap.String("namespace", m.state.GetID().Namespace),
				zap.String("changefeed", m.state.GetID().ID),
				zap.Uint64("checkpointTs", m.state.GetChangefeedStatus().CheckpointTs),
				zap.Duration("checkpointTime", currTime.Sub(ckptTime)),
			)
			code, _ := cerrors.RFCCode(cerrors.ErrChangefeedUnretryable)
			m.HandleError(&model.RunningError{
				Time:    lastError.Time,
				Addr:    lastError.Addr,
				Code:    string(code),
				Message: lastError.Message,
			})
			return
		}
	}

	m.patchState(model.StateWarning)
	m.state.SetWarning(lastError)
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

// checkAndChangeState checks the state of the changefeed and change it if needed.
// if the state of the changefeed is warning and the changefeed's checkpointTs is
// greater than the lastRetryCheckpointTs, it will change the state to normal.
func (m *feedStateManager) checkAndChangeState() {
	if m.state.GetChangefeedInfo() == nil || m.state.GetChangefeedStatus() == nil {
		return
	}
	if m.state.GetChangefeedInfo().State == model.StateWarning &&
		m.state.GetChangefeedStatus().CheckpointTs > m.lastErrorRetryCheckpointTs &&
		m.state.GetChangefeedStatus().CheckpointTs > m.lastWarningReportCheckpointTs {
		log.Info("changefeed is recovered from warning state,"+
			"its checkpointTs is greater than lastRetryCheckpointTs,"+
			"it will be changed to normal state",
			zap.String("namespace", m.state.GetID().Namespace),
			zap.String("changefeed", m.state.GetID().ID),
			zap.Uint64("checkpointTs", m.state.GetChangefeedStatus().CheckpointTs),
			zap.Uint64("lastRetryCheckpointTs", m.lastErrorRetryCheckpointTs))
		m.patchState(model.StateNormal)
		m.isRetrying = false
	}
}

// checkAndInitLastRetryCheckpointTs checks the lastRetryCheckpointTs and init it if needed.
// It the owner is changed, the lastRetryCheckpointTs will be reset to 0, and we should init
// it to the checkpointTs of the changefeed when the changefeed is ticked at the first time.
func (m *feedStateManager) checkAndInitLastRetryCheckpointTs(status *model.ChangeFeedStatus) {
	if status == nil || m.lastErrorRetryCheckpointTs != 0 {
		return
	}
	m.lastWarningReportCheckpointTs = status.CheckpointTs
	m.lastErrorRetryCheckpointTs = status.CheckpointTs
	log.Info("init lastRetryCheckpointTs", zap.Uint64("lastRetryCheckpointTs", m.lastErrorRetryCheckpointTs))
}
