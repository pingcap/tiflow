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
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	cdcContext "github.com/pingcap/ticdc/pkg/context"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/orchestrator"
)

const (
	// CDCServiceSafePointID is the ID of CDC service in pd.UpdateServiceGCSafePoint.
	cdcServiceSafePointID = "ticdc"
	// GCSafepointUpdateInterval is the minimual interval that CDC can update gc safepoint
	gcSafepointUpdateInterval = time.Duration(1 * time.Minute)
)

type ownerJobType int

// All AdminJob types
const (
	ownerJobTypeRebalance ownerJobType = iota
	ownerJobTypeManualSchedule
	ownerJobTypeAdminJob
	ownerJobTypeDebugInfo
)

type ownerJob struct {
	tp           ownerJobType
	changefeedID model.ChangeFeedID

	// for ManualSchedule only
	targetCaptureID model.CaptureID
	// for ManualSchedule only
	tableID model.TableID

	// for Admin Job only
	adminJob *model.AdminJob

	// for debug info only
	httpWriter http.ResponseWriter

	done chan struct{}
}

type Owner struct {
	changefeeds map[model.ChangeFeedID]*changefeed

	gcManager *gcManager

	ownerJobQueue   []*ownerJob
	ownerJobQueueMu sync.Mutex

	close int32

	newChangefeed func(gcManager *gcManager) *changefeed
}

func NewOwner() *Owner {
	return &Owner{
		changefeeds:   make(map[model.ChangeFeedID]*changefeed),
		gcManager:     newGCManager(),
		newChangefeed: newChangefeed,
	}
}

func NewOwner4Test(
	newDDLPuller func(ctx cdcContext.Context, startTs uint64) (DDLPuller, error),
	newSink func(ctx cdcContext.Context) (AsyncSink, error)) *Owner {
	o := NewOwner()
	o.newChangefeed = func(gcManager *gcManager) *changefeed {
		return newChangefeed4Test(gcManager, newDDLPuller, newSink)
	}
	return o
}

func (o *Owner) Tick(stdCtx context.Context, rawState orchestrator.ReactorState) (nextState orchestrator.ReactorState, err error) {
	failpoint.Inject("owner-run-with-error", func() {
		failpoint.Return(nil, errors.New("owner run with injected error"))
	})
	failpoint.Inject("sleep-in-owner-tick", nil)
	ctx := stdCtx.(cdcContext.Context)
	state := rawState.(*model.GlobalReactorState)
	state.CheckCaptureAlive(ctx.GlobalVars().CaptureInfo.ID)
	err = o.gcManager.updateGCSafePoint(ctx, state)
	if err != nil {
		return nil, errors.Trace(err)
	}
	o.handleJob()
	for changefeedID, changefeedState := range state.Changefeeds {
		if changefeedState.Info == nil {
			o.cleanUpChangefeed(changefeedState)
			continue
		}
		ctx = cdcContext.WithChangefeedVars(ctx, &cdcContext.ChangefeedVars{
			ID:   changefeedID,
			Info: changefeedState.Info,
		})
		cfReactor, exist := o.changefeeds[changefeedID]
		if !exist {
			cfReactor = o.newChangefeed(o.gcManager)
			o.changefeeds[changefeedID] = cfReactor
		}
		cfReactor.Tick(ctx, changefeedState, state.Captures)
	}
	if len(o.changefeeds) != len(state.Changefeeds) {
		for changefeedID, cfReactor := range o.changefeeds {
			if _, exist := state.Changefeeds[changefeedID]; exist {
				continue
			}
			cfReactor.Close()
			delete(o.changefeeds, changefeedID)
		}
	}
	if atomic.LoadInt32(&o.close) != 0 {
		for _, cfReactor := range o.changefeeds {
			cfReactor.Close()
		}
		return state, cerror.ErrReactorFinished.GenWithStackByArgs()
	}
	return state, nil
}

func (o *Owner) EnqueueJob(adminJob model.AdminJob) {
	o.pushOwnerJob(&ownerJob{
		tp:           ownerJobTypeAdminJob,
		adminJob:     &adminJob,
		changefeedID: adminJob.CfID,
		done:         make(chan struct{}),
	})
}

func (o *Owner) TriggerRebalance(cfID model.ChangeFeedID) {
	o.pushOwnerJob(&ownerJob{
		tp:           ownerJobTypeRebalance,
		changefeedID: cfID,
		done:         make(chan struct{}),
	})
}

func (o *Owner) ManualSchedule(cfID model.ChangeFeedID, toCapture model.CaptureID, tableID model.TableID) {
	o.pushOwnerJob(&ownerJob{
		tp:              ownerJobTypeManualSchedule,
		changefeedID:    cfID,
		targetCaptureID: toCapture,
		tableID:         tableID,
		done:            make(chan struct{}),
	})
}

func (o *Owner) WriteDebugInfo(w http.ResponseWriter) {
	o.pushOwnerJob(&ownerJob{
		tp:         ownerJobTypeDebugInfo,
		httpWriter: w,
		done:       make(chan struct{}),
	})
}

func (o *Owner) AsyncStop() {
	atomic.StoreInt32(&o.close, 1)
}

func (o *Owner) cleanUpChangefeed(state *model.ChangefeedReactorState) {
	state.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
		return nil, info != nil, nil
	})
	state.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		return nil, status != nil, nil
	})
	for captureID := range state.TaskStatuses {
		state.PatchTaskStatus(captureID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
			return nil, status != nil, nil
		})
	}
	for captureID := range state.TaskPositions {
		state.PatchTaskPosition(captureID, func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
			return nil, position != nil, nil
		})
	}
	for captureID := range state.Workloads {
		state.PatchTaskWorkload(captureID, func(workload model.TaskWorkload) (model.TaskWorkload, bool, error) {
			return nil, workload != nil, nil
		})
	}
}

func (o *Owner) handleJob() {
	jobs := o.takeOnwerJobs()
	for _, job := range jobs {
		changefeedID := job.changefeedID
		cfReactor, exist := o.changefeeds[changefeedID]
		if !exist {
			log.Warn("") // TODO
		}
		switch job.tp {
		case ownerJobTypeAdminJob:
			cfReactor.feedStateManager.PushAdminJob(job.adminJob)
		case ownerJobTypeManualSchedule:
			cfReactor.scheduler.MoveTable(job.tableID, job.targetCaptureID)
		case ownerJobTypeRebalance:
			cfReactor.scheduler.Rebalance()
		case ownerJobTypeDebugInfo:
			panic("unimplemented") // TODO
		}
	}
}

func (o *Owner) takeOnwerJobs() []*ownerJob {
	o.ownerJobQueueMu.Lock()
	defer o.ownerJobQueueMu.Unlock()

	jobs := o.ownerJobQueue
	o.ownerJobQueue = nil
	return jobs
}

func (o *Owner) pushOwnerJob(job *ownerJob) {
	o.ownerJobQueueMu.Lock()
	defer o.ownerJobQueueMu.Unlock()
	o.ownerJobQueue = append(o.ownerJobQueue, job)
}
