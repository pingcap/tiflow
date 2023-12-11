// Copyright 2023 PingCAP, Inc.
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
	"github.com/pingcap/tiflow/cdc/owner"
	"github.com/pingcap/tiflow/cdcv2/metadata"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/upstream"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

const (
	// When errors occurred, and we need to do backoff, we start an exponential backoff
	// with an interval from 10s to 30min (10s, 20s, 40s, 80s, 160s, 320s,
	//	 600s, 600s, ...).
	// To avoid thunderherd, a random factor is also added.
	defaultBackoffInitInterval        = 10 * time.Second
	defaultBackoffMaxInterval         = 10 * time.Minute
	defaultBackoffMaxElapsedTime      = 30 * time.Minute
	defaultBackoffRandomizationFactor = 0.1
	defaultBackoffMultiplier          = 2.0
)

var _ owner.FeedStateManager = &feedStateManagerImpl{}

type feedStateManagerImpl struct {
	shouldBeRunning bool
	shouldBeRemoved bool
	adminJobQueue   []*model.AdminJob
	id              model.ChangeFeedID
	ownerdb         metadata.OwnerObservation
	upstream        *upstream.Upstream

	// resolvedTs and initCheckpointTs is for checking whether resolved timestamp
	// has been advanced or not.
	resolvedTs   model.Ts
	checkpointTs model.Ts

	checkpointTsAdvanced time.Time

	isRetrying                    bool
	lastErrorRetryTime            time.Time                   // time of last error for a changefeed
	lastErrorRetryCheckpointTs    model.Ts                    // checkpoint ts of last retry
	lastWarningReportCheckpointTs model.Ts                    // checkpoint ts of last warning report
	backoffInterval               time.Duration               // the interval for restarting a changefeed in 'error' state
	errBackoff                    *backoff.ExponentialBackOff // an exponential backoff for restarting a changefeed

	state  *metadata.ChangefeedState
	status *model.ChangeFeedStatus

	changefeedErrorStuckDuration time.Duration
}

func newFeedStateManager(id model.ChangeFeedID, upstream *upstream.Upstream,
	ownerdb metadata.OwnerObservation) *feedStateManagerImpl {
	f := &feedStateManagerImpl{
		adminJobQueue: make([]*model.AdminJob, 0),
		id:            id,
		upstream:      upstream,
		ownerdb:       ownerdb,
	}
	f.errBackoff = backoff.NewExponentialBackOff()
	f.errBackoff.InitialInterval = defaultBackoffInitInterval
	f.errBackoff.MaxInterval = defaultBackoffMaxInterval
	f.errBackoff.Multiplier = defaultBackoffMultiplier
	f.errBackoff.RandomizationFactor = defaultBackoffRandomizationFactor
	// backoff will stop once the defaultBackoffMaxElapsedTime has elapsed.
	f.errBackoff.MaxElapsedTime = defaultBackoffMaxElapsedTime
	f.changefeedErrorStuckDuration = time.Minute * 15
	f.resetErrRetry()
	return f
}

// resetErrRetry reset the error retry related fields
func (f *feedStateManagerImpl) resetErrRetry() {
	f.errBackoff.Reset()
	f.backoffInterval = f.errBackoff.NextBackOff()
	f.lastErrorRetryTime = time.Unix(0, 0)
}

func (f *feedStateManagerImpl) PushAdminJob(job *model.AdminJob) {
	f.pushAdminJob(job)
}

func (f *feedStateManagerImpl) Tick(resolvedTs model.Ts,
	status *model.ChangeFeedStatus,
	info *model.ChangeFeedInfo) (adminJobPending bool) {
	f.checkAndInitLastRetryCheckpointTs(status)

	if status != nil {
		if f.checkpointTs < status.CheckpointTs {
			f.checkpointTs = status.CheckpointTs
			f.checkpointTsAdvanced = time.Now()
		}
		if f.resolvedTs < resolvedTs {
			f.resolvedTs = resolvedTs
		}
		if f.checkpointTs >= f.resolvedTs {
			f.checkpointTsAdvanced = time.Now()
		}
	}

	f.resolvedTs = resolvedTs
	f.shouldBeRunning = true

	if f.handleAdminJob() {
		// `handleAdminJob` returns true means that some admin jobs are pending
		// skip to the next tick until all the admin jobs is handled
		adminJobPending = true
		return
	}

	switch f.state.State {
	case model.StateUnInitialized:
		_ = f.ownerdb.ResumeChangefeed()
		return
	case model.StateRemoved:
		f.shouldBeRunning = false
		f.shouldBeRemoved = true
		return
	case model.StateStopped, model.StateFailed, model.StateFinished:
		f.shouldBeRunning = false
		return
	case model.StatePending:
		if time.Since(f.lastErrorRetryTime) < f.backoffInterval {
			f.shouldBeRunning = false
			return
		}
		// retry the changefeed
		oldBackoffInterval := f.backoffInterval
		f.backoffInterval = f.errBackoff.NextBackOff()
		// NextBackOff() will return -1 once the MaxElapsedTime has elapsed,
		// set the changefeed to failed state.
		if f.backoffInterval == f.errBackoff.Stop {
			log.Error("The changefeed won't be restarted as it has been experiencing failures for "+
				"an extended duration",
				zap.Duration("maxElapsedTime", f.errBackoff.MaxElapsedTime),
				zap.String("namespace", f.id.Namespace),
				zap.String("changefeed", f.id.ID),
				zap.Time("lastRetryTime", f.lastErrorRetryTime),
				zap.Uint64("lastRetryCheckpointTs", f.lastErrorRetryCheckpointTs),
			)
			f.shouldBeRunning = false
			_ = f.ownerdb.SetChangefeedFailed(nil)
			return
		}

		f.lastErrorRetryTime = time.Now()
		if f.status != nil {
			f.lastErrorRetryCheckpointTs = f.status.CheckpointTs
		}
		f.shouldBeRunning = true
		_ = f.ownerdb.SetChangefeedWarning(nil)
		log.Info("changefeed retry backoff interval is elapsed,"+
			"chengefeed will be restarted",
			zap.String("namespace", f.id.Namespace),
			zap.String("changefeed", f.id.ID),
			zap.Time("lastErrorRetryTime", f.lastErrorRetryTime),
			zap.Duration("lastRetryInterval", oldBackoffInterval),
			zap.Duration("nextRetryInterval", f.backoffInterval))
	case model.StateNormal, model.StateWarning:
		f.checkAndChangeState()
		errs := f.errorsReportedByProcessors()
		f.HandleError(errs...)
		// only handle warnings when there are no errors
		// otherwise, the warnings will cover the errors
		if len(errs) == 0 {
			// warning are come from processors' sink component
			// they ere not fatal errors, so we don't need to stop the changefeed
			warnings := f.warningsReportedByProcessors()
			f.HandleWarning(warnings...)
		}
	}
	return
}

// checkAndInitLastRetryCheckpointTs checks the lastRetryCheckpointTs and init it if needed.
// It the owner is changed, the lastRetryCheckpointTs will be reset to 0, and we should init
// it to the checkpointTs of the changefeed when the changefeed is ticked at the first time.
func (f *feedStateManagerImpl) checkAndInitLastRetryCheckpointTs(status *model.ChangeFeedStatus) {
	if status == nil || f.lastErrorRetryCheckpointTs != 0 {
		return
	}
	f.lastWarningReportCheckpointTs = status.CheckpointTs
	f.lastErrorRetryCheckpointTs = status.CheckpointTs
	log.Info("init lastRetryCheckpointTs", zap.Uint64("lastRetryCheckpointTs", f.lastErrorRetryCheckpointTs))
}

func (f *feedStateManagerImpl) errorsReportedByProcessors() []*model.RunningError {
	if f.state == nil || f.state.Error == nil {
		return nil
	}
	result := make([]*model.RunningError, 0, 1)
	result = append(result, f.state.Error)
	return result
}

// checkAndChangeState checks the state of the changefeed and change it if needed.
// if the state of the changefeed is warning and the changefeed's checkpointTs is
// greater than the lastRetryCheckpointTs, it will change the state to normal.
func (f *feedStateManagerImpl) checkAndChangeState() {
	if f.state == nil || f.status == nil {
		return
	}
	if f.state.State == model.StateWarning &&
		f.status.CheckpointTs > f.lastErrorRetryCheckpointTs &&
		f.status.CheckpointTs > f.lastWarningReportCheckpointTs {
		log.Info("changefeed is recovered from warning state,"+
			"its checkpointTs is greater than lastRetryCheckpointTs,"+
			"it will be changed to normal state",
			zap.String("changefeed", f.id.String()),
			zap.Uint64("checkpointTs", f.status.CheckpointTs),
			zap.Uint64("lastRetryCheckpointTs", f.lastErrorRetryCheckpointTs))
		_ = f.ownerdb.ResumeChangefeed()
		f.isRetrying = false
	}
}

func (f *feedStateManagerImpl) warningsReportedByProcessors() []*model.RunningError {
	if f.state == nil || f.state.Warning == nil {
		return nil
	}
	result := make([]*model.RunningError, 0, 1)
	result = append(result, f.state.Warning)
	return result
}

func (f *feedStateManagerImpl) HandleError(errs ...*model.RunningError) {
	if len(errs) == 0 {
		return
	}
	// if there are a fastFail error in errs, we can just fastFail the changefeed
	// and no need to patch other error to the changefeed info
	for _, err := range errs {
		if cerrors.IsChangefeedGCFastFailErrorCode(errors.RFCErrorCode(err.Code)) ||
			err.ShouldFailChangefeed() {
			f.shouldBeRunning = false
			_ = f.ownerdb.SetChangefeedFailed(err)
			return
		}
	}

	// Changing changefeed state from stopped to failed is allowed
	// but changing changefeed state from stopped to error or normal is not allowed.
	if f.state != nil && f.state.State == model.StateStopped {
		log.Warn("changefeed is stopped, ignore errors",
			zap.String("changefeed", f.id.ID),
			zap.String("namespace", f.id.Namespace),
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
		f.shouldBeRunning = false
		_ = f.ownerdb.SetChangefeedPending(lastError)
	}

	// The errBackoff needs to be reset before the first retry.
	if !f.isRetrying {
		f.resetErrRetry()
		f.isRetrying = true
	}
}

func (f *feedStateManagerImpl) HandleWarning(warnings ...*model.RunningError) {
	if len(warnings) == 0 {
		return
	}
	lastError := warnings[len(warnings)-1]

	if f.status != nil {
		currTime := f.upstream.PDClock.CurrentTime()
		ckptTime := oracle.GetTimeFromTS(f.status.CheckpointTs)
		f.lastWarningReportCheckpointTs = f.status.CheckpointTs
		// Conditions:
		// 1. checkpoint lag is large enough;
		// 2. checkpoint hasn't been advanced for a long while;
		// 3. the changefeed has been initialized.
		checkpointTsStuck := time.Since(f.checkpointTsAdvanced) > f.changefeedErrorStuckDuration
		if checkpointTsStuck {
			log.Info("changefeed retry on warning for a very long time and does not resume, "+
				"it will be failed", zap.String("changefeed", f.id.String()),
				zap.Uint64("checkpointTs", f.status.CheckpointTs),
				zap.Duration("checkpointTime", currTime.Sub(ckptTime)),
			)
			code, _ := cerrors.RFCCode(cerrors.ErrChangefeedUnretryable)
			f.HandleError(&model.RunningError{
				Time:    lastError.Time,
				Addr:    lastError.Addr,
				Code:    string(code),
				Message: lastError.Message,
			})
			return
		}
	}

	_ = f.ownerdb.SetChangefeedWarning(lastError)
}

func (f *feedStateManagerImpl) ShouldRunning() bool {
	return f.shouldBeRunning
}

func (f *feedStateManagerImpl) ShouldRemoved() bool {
	return f.shouldBeRemoved
}

func (f *feedStateManagerImpl) MarkFinished() {
	f.pushAdminJob(&model.AdminJob{
		CfID: f.id,
		Type: model.AdminFinish,
	})
}

func (f *feedStateManagerImpl) handleAdminJob() (jobsPending bool) {
	job := f.popAdminJob()
	if job == nil || job.CfID != f.id {
		return false
	}
	log.Info("handle admin job",
		zap.String("namespace", f.id.Namespace),
		zap.String("changefeed", f.id.ID),
		zap.Any("job", job))
	switch job.Type {
	case model.AdminStop:
		f.shouldBeRunning = false
		jobsPending = true
		_ = f.ownerdb.PauseChangefeed()
	case model.AdminRemove:
		f.shouldBeRunning = false
		f.shouldBeRemoved = true
		jobsPending = true

		_ = f.ownerdb.SetChangefeedRemoved()

		log.Info("the changefeed is removed",
			zap.String("namespace", f.id.Namespace),
			zap.String("changefeed", f.id.ID))
	case model.AdminResume:
		f.shouldBeRunning = true
		// when the changefeed is manually resumed, we must reset the backoff
		f.resetErrRetry()
		jobsPending = true
		_ = f.ownerdb.ResumeChangefeed()
	case model.AdminFinish:
		f.shouldBeRunning = false
		jobsPending = true
		_ = f.ownerdb.SetChangefeedFinished()
	default:
		log.Warn("Unknown admin job", zap.Any("adminJob", job),
			zap.String("namespace", f.id.Namespace),
			zap.String("changefeed", f.id.ID))
	}
	return
}

func (f *feedStateManagerImpl) popAdminJob() *model.AdminJob {
	if len(f.adminJobQueue) == 0 {
		return nil
	}
	job := f.adminJobQueue[0]
	f.adminJobQueue = f.adminJobQueue[1:]
	return job
}

func (f *feedStateManagerImpl) pushAdminJob(job *model.AdminJob) {
	f.adminJobQueue = append(f.adminJobQueue, job)
}
