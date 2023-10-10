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
	"context"
	"database/sql"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/owner"
	"github.com/pingcap/tiflow/cdc/processor"
	"github.com/pingcap/tiflow/cdc/scheduler"
	"github.com/pingcap/tiflow/cdcv2/metadata"
	"github.com/pingcap/tiflow/pkg/config"
	cdcContext "github.com/pingcap/tiflow/pkg/context"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"github.com/pingcap/tiflow/pkg/upstream"
	"go.uber.org/zap"
)

type OwnerImpl struct {
	upstreamManager    *upstream.Manager
	captureObservation metadata.CaptureObservation
	cfg                *config.SchedulerConfig
	storage            *sql.DB
	//todo: make a struct
	changefeeds       map[model.ChangeFeedID]*changefeedImpl
	changefeedUUIDMap map[metadata.ChangefeedUUID]*changefeedImpl

	liveness *model.Liveness

	ownerJobQueue struct {
		sync.Mutex
		queue []*ownerJob
	}
	changefeedTicked bool
	closed           int32
	captures         map[model.CaptureID]*model.CaptureInfo

	querier metadata.Querier
}

func (o *OwnerImpl) Tick(ctx context.Context,
	state orchestrator.ReactorState) (nextState orchestrator.ReactorState, err error) {
	//TODO implement me
	panic("implement me")
}

func (o *OwnerImpl) EnqueueJob(adminJob model.AdminJob,
	done chan<- error) {
	panic("implement me")
}

func (o *OwnerImpl) RebalanceTables(cfID model.ChangeFeedID,
	done chan<- error) {
	panic("implement me")
}

func (o *OwnerImpl) ScheduleTable(cfID model.ChangeFeedID,
	toCapture model.CaptureID,
	tableID model.TableID, done chan<- error) {
	//TODO implement me
	panic("implement me")
}

func (o *OwnerImpl) DrainCapture(query *scheduler.Query,
	done chan<- error) {
	//TODO implement me
	panic("implement me")
}

func (o *OwnerImpl) WriteDebugInfo(w io.Writer,
	done chan<- error) {
	//TODO implement me
	panic("implement me")
}

func (o *OwnerImpl) Query(query *owner.Query, done chan<- error) {
	//TODO implement me
	panic("implement me")
}

func (o *OwnerImpl) AsyncStop() {
	return
}

func NewOwner(
	liveness *model.Liveness,
	upstreamManager *upstream.Manager,
	cfg *config.SchedulerConfig,
	captureObservation metadata.CaptureObservation,
	querier metadata.Querier,
	storage *sql.DB) *OwnerImpl {
	return &OwnerImpl{
		upstreamManager:    upstreamManager,
		captureObservation: captureObservation,
		cfg:                cfg,
		querier:            querier,
		storage:            storage,
		liveness:           liveness,
	}
}

func (o *OwnerImpl) Run(ctx context.Context) error {
	tick := time.NewTicker(time.Millisecond * 100)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
			// handleJobs() should be called before clusterVersionConsistent(), because
			// when there are different versions of cdc nodes in the cluster,
			// the admin job may not be processed all the time. And http api relies on
			// admin job, which will cause all http api unavailable.
			o.handleJobs(ctx)
			self := o.captureObservation.Self()
			var progress metadata.CaptureProgress = make(map[metadata.ChangefeedUUID]metadata.ChangefeedProgress)
			for _, cf := range o.changefeeds {
				// start owner
				info, err := o.querier.GetChangefeeds(cf.uuid)
				if err != nil {
					log.Warn("changefeed not found when handle a job", zap.Any("job", cf))
					continue
				}
				nInfo := &model.ChangeFeedInfo{
					Config: info[0].Config,
				}
				//only one capture
				cp, bt := cf.Tick(cdcContext.NewContext(ctx, nil),
					nInfo,
					//todo: get changefeed status
					cf.Status,
					map[model.CaptureID]*model.CaptureInfo{self.ID: self},
				)
				cf.Status = &model.ChangeFeedStatus{
					CheckpointTs:      cp,
					MinTableBarrierTs: bt,
				}
				progress[cf.uuid] = metadata.ChangefeedProgress{
					CheckpointTs:      cp,
					MinTableBarrierTs: bt,
				}
			}
			_ = o.captureObservation.Advance(progress)
		case cf := <-o.captureObservation.OwnerChanges():
			switch cf.OwnerState {
			case metadata.SchedRemoving:
				//stop owner
				changefeed, exist := o.changefeedUUIDMap[cf.ChangefeedUUID]
				if !exist {
					log.Warn("changefeed not found when handle a job", zap.Any("job", cf))
					continue
				}
				changefeed.Close(cdcContext.NewContext(ctx, nil))
				delete(o.changefeedUUIDMap, cf.ChangefeedUUID)
				delete(o.changefeeds, changefeed.ID)
				_ = o.captureObservation.PostOwnerRemoved(cf.ChangefeedUUID, cf.TaskPosition)
			case metadata.SchedLaunched:
				// start owner
				info, err := o.querier.GetChangefeeds(cf.ChangefeedUUID)
				if err != nil {
					log.Warn("changefeed not found when handle a job", zap.Any("job", cf))
					continue
				}
				cfInfo := info[0]
				//todo: add upstream
				up, _ := o.upstreamManager.Get(cfInfo.UpstreamID)
				minfo := &model.ChangeFeedInfo{
					SinkURI:   cfInfo.SinkURI,
					Config:    cfInfo.Config,
					Namespace: cfInfo.Namespace,
					ID:        cfInfo.ID,
				}
				mstatus := &model.ChangeFeedStatus{
					CheckpointTs:      cf.TaskPosition.CheckpointTs,
					MinTableBarrierTs: cf.TaskPosition.MinTableBarrierTs,
					AdminJobType:      cf.TaskPosition.AdminJobType,
				}
				cfID := model.ChangeFeedID{
					Namespace: cfInfo.Namespace,
					ID:        cfInfo.ID,
				}
				p := processor.NewProcessor(minfo, mstatus, nil, cfID, up, o.liveness, 0, o.cfg)
				o.changefeedUUIDMap[cf.ChangefeedUUID] = newChangefeed(owner.NewChangefeed(
					cfID,
					minfo,
					mstatus, newFeedStateManager(),
					up, o.cfg,
				), minfo, mstatus, p)
				o.changefeeds[o.changefeedUUIDMap[cf.ChangefeedUUID].ID] = o.changefeedUUIDMap[cf.ChangefeedUUID]
			}
		}
	}
}

func (o *OwnerImpl) handleJobs(ctx context.Context) {
	jobs := o.takeOwnerJobs()
	for _, job := range jobs {
		changefeedID := job.ChangefeedID
		cfReactor, exist := o.changefeeds[changefeedID]
		if !exist && (job.Tp != ownerJobTypeQuery && job.Tp != ownerJobTypeDrainCapture) {
			log.Warn("changefeed not found when handle a job", zap.Any("job", job))
			job.done <- cerror.ErrChangeFeedNotExists.FastGenByArgs(job.ChangefeedID)
			close(job.done)
			continue
		}
		switch job.Tp {
		case ownerJobTypeAdminJob:
			//todo: admin job
			//cfReactor.feedStateManager.PushAdminJob(job.AdminJob)
		case ownerJobTypeScheduleTable:
			// Scheduler is created lazily, it is nil before initialization.
			if cfReactor.changefeed.GetScheduler() != nil {
				cfReactor.changefeed.GetScheduler().MoveTable(job.TableID, job.TargetCaptureID)
			}
		case ownerJobTypeDrainCapture:
			// todo: drain capture
			//o.handleDrainCaptures(ctx, job.scheduleQuery, job.done)
			continue // continue here to prevent close the done channel twice
		case ownerJobTypeRebalance:
			// Scheduler is created lazily, it is nil before initialization.
			if cfReactor.changefeed.GetScheduler() != nil {
				cfReactor.changefeed.GetScheduler().Rebalance()
			}
		case ownerJobTypeQuery:
			job.done <- o.handleQueries(job.query)
		case ownerJobTypeDebugInfo:
			// TODO: implement this function
		}
		close(job.done)
	}
}

func (o *OwnerImpl) handleQueries(query *owner.Query) error {
	switch query.Tp {
	case owner.QueryAllChangeFeedStatuses:
		ret := map[model.ChangeFeedID]*model.ChangeFeedStatusForAPI{}
		for cfID, cfReactor := range o.changefeeds {
			ret[cfID] = &model.ChangeFeedStatusForAPI{}
			if cfReactor.Info == nil {
				continue
			}
			ret[cfID].CheckpointTs = cfReactor.Status.CheckpointTs
		}
		query.Data = ret
	case owner.QueryAllChangeFeedInfo:
		ret := map[model.ChangeFeedID]*model.ChangeFeedInfo{}
		for cfID, cfReactor := range o.changefeeds {
			if cfReactor.Info == nil {
				ret[cfID] = &model.ChangeFeedInfo{}
				continue
			}
			var err error
			ret[cfID], err = cfReactor.Info.Clone()
			if err != nil {
				return errors.Trace(err)
			}
		}
		query.Data = ret
	case owner.QueryAllTaskStatuses:
		cfReactor, ok := o.changefeeds[query.ChangeFeedID]
		if !ok {
			return cerror.ErrChangeFeedNotExists.GenWithStackByArgs(query.ChangeFeedID)
		}
		if cfReactor.Info == nil {
			return cerror.ErrChangeFeedNotExists.GenWithStackByArgs(query.ChangeFeedID)
		}

		var ret map[model.CaptureID]*model.TaskStatus
		provider := cfReactor.GetInfoProvider()
		if provider == nil {
			// The scheduler has not been initialized yet.
			return cerror.ErrChangeFeedNotExists.GenWithStackByArgs(query.ChangeFeedID)
		}

		var err error
		ret, err = provider.GetTaskStatuses()
		if err != nil {
			return errors.Trace(err)
		}
		query.Data = ret
	case owner.QueryProcessors:
		var ret []*model.ProcInfoSnap
		for cfID, cfReactor := range o.changefeeds {
			provider := cfReactor.GetInfoProvider()
			if provider == nil {
				// The scheduler has not been initialized yet.
				continue
			}

			statuses, err := provider.GetTaskStatuses()
			if err != nil {
				return errors.Trace(err)
			}
			for captureID := range statuses {
				ret = append(ret, &model.ProcInfoSnap{
					CfID:      cfID,
					CaptureID: captureID,
				})
			}
		}
		query.Data = ret
	case owner.QueryCaptures:
		var ret []*model.CaptureInfo
		for _, captureInfo := range o.captures {
			ret = append(ret, &model.CaptureInfo{
				ID:            captureInfo.ID,
				AdvertiseAddr: captureInfo.AdvertiseAddr,
				Version:       captureInfo.Version,
			})
		}
		query.Data = ret
	case owner.QueryHealth:
		query.Data = o.isHealthy()
	case owner.QueryOwner:
		_, exist := o.changefeeds[query.ChangeFeedID]
		query.Data = exist
	}
	return nil
}

func (o *OwnerImpl) isHealthy() bool {
	if !o.changefeedTicked {
		// Owner has not yet tick changefeeds, some changefeeds may be not
		// initialized.
		log.Warn("owner is not healthy since changefeeds are not ticked")
		return false
	}
	//if !o.clusterVersionConsistent(o.captures) {
	//	return false
	//}
	for _, changefeed := range o.changefeeds {
		if changefeed.Info == nil {
			log.Warn("isHealthy: changefeed state is nil",
				zap.String("namespace", changefeed.Info.Namespace),
				zap.String("changefeed", changefeed.Info.ID))
			continue
		}
		if changefeed.Info.State != model.StateNormal {
			log.Warn("isHealthy: changefeed not normal",
				zap.String("namespace", changefeed.Info.Namespace),
				zap.String("changefeed", changefeed.Info.ID),
				zap.Any("state", changefeed.Info.State))
			continue
		}

		provider := changefeed.GetInfoProvider()
		if provider == nil || !provider.IsInitialized() {
			// The scheduler has not been initialized yet, it is considered
			// unhealthy, because owner can not schedule tables for now.
			log.Warn("isHealthy: changefeed is not initialized",
				zap.String("namespace", changefeed.Info.Namespace),
				zap.String("changefeed", changefeed.Info.ID))
			return false
		}
	}
	return true
}

func (o *OwnerImpl) takeOwnerJobs() []*ownerJob {
	o.ownerJobQueue.Lock()
	defer o.ownerJobQueue.Unlock()

	jobs := o.ownerJobQueue.queue
	o.ownerJobQueue.queue = nil
	return jobs
}

func (o *OwnerImpl) pushOwnerJob(job *ownerJob) {
	o.ownerJobQueue.Lock()
	defer o.ownerJobQueue.Unlock()
	if atomic.LoadInt32(&o.closed) != 0 {
		log.Info("reject owner job as owner has been closed",
			zap.Int("jobType", int(job.Tp)))
		select {
		case job.done <- cerror.ErrOwnerNotFound.GenWithStackByArgs():
		default:
		}
		close(job.done)
		return
	}
	o.ownerJobQueue.queue = append(o.ownerJobQueue.queue, job)
}

func (o *OwnerImpl) cleanupOwnerJob() {
	log.Info("cleanup owner jobs as owner has been closed")
	jobs := o.takeOwnerJobs()
	for _, job := range jobs {
		select {
		case job.done <- cerror.ErrOwnerNotFound.GenWithStackByArgs():
		default:
		}
		close(job.done)
	}
}

type ownerJobType int

// All OwnerJob types
const (
	ownerJobTypeRebalance ownerJobType = iota
	ownerJobTypeScheduleTable
	ownerJobTypeDrainCapture
	ownerJobTypeAdminJob
	ownerJobTypeDebugInfo
	ownerJobTypeQuery
)

// Export field names for pretty printing.
type ownerJob struct {
	Tp           ownerJobType
	ChangefeedID model.ChangeFeedID

	// for ScheduleTable only
	TargetCaptureID model.CaptureID
	// for ScheduleTable only
	TableID model.TableID

	// for Admin Job only
	AdminJob *model.AdminJob

	// for debug info only
	debugInfoWriter io.Writer

	// for status provider
	query *owner.Query

	// for scheduler related jobs
	scheduleQuery *scheduler.Query

	done chan<- error
}
