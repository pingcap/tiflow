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
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/scheduler"
	"github.com/pingcap/tiflow/pkg/config"
	cdcContext "github.com/pingcap/tiflow/pkg/context"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"github.com/pingcap/tiflow/pkg/upstream"
	"github.com/pingcap/tiflow/pkg/version"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

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

// versionInconsistentLogRate represents the rate of log output when there are
// captures with versions different from that of the owner
const versionInconsistentLogRate = 1

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
	query *Query

	// for scheduler related jobs
	scheduleQuery *scheduler.Query

	done chan<- error
}

// Owner managers TiCDC cluster.
//
// The interface is thread-safe, except for Tick, it's only used by etcd worker.
type Owner interface {
	EnqueueJob(adminJob model.AdminJob, done chan<- error)
	RebalanceTables(cfID model.ChangeFeedID, done chan<- error)
	ScheduleTable(
		cfID model.ChangeFeedID, toCapture model.CaptureID,
		tableID model.TableID, done chan<- error,
	)
	DrainCapture(query *scheduler.Query, done chan<- error)
	WriteDebugInfo(w io.Writer, done chan<- error)
	Query(query *Query, done chan<- error)
	AsyncStop()
	UpdateChangefeedAndUpstream(ctx context.Context,
		upstreamInfo *model.UpstreamInfo,
		changeFeedInfo *model.ChangeFeedInfo,
		changeFeedID model.ChangeFeedID,
	) error
	UpdateChangefeed(ctx context.Context,
		changeFeedInfo *model.ChangeFeedInfo) error
}

type ownerImpl struct {
	changefeeds     map[model.ChangeFeedID]*changefeed
	captures        map[model.CaptureID]*model.CaptureInfo
	upstreamManager *upstream.Manager
	ownerJobQueue   struct {
		sync.Mutex
		queue []*ownerJob
	}
	// logLimiter controls cluster version check log output rate
	logLimiter   *rate.Limiter
	lastTickTime time.Time
	closed       int32

	// changefeedTicked specifies whether changefeeds have been ticked.
	// NOTICE: Do not use it in a method other than tick unexpectedly,
	//         as it is not a thread-safe value.
	changefeedTicked bool

	etcdClient etcd.CDCEtcdClient

	newChangefeed func(
		id model.ChangeFeedID,
		cfInfo *model.ChangeFeedInfo,
		cfStatus *model.ChangeFeedStatus,
		feedStateManager FeedStateManager,
		up *upstream.Upstream,
		cfg *config.SchedulerConfig,
	) *changefeed
	cfg *config.SchedulerConfig
}

var (
	_ orchestrator.Reactor = &ownerImpl{}
	_ Owner                = &ownerImpl{}
)

// NewOwner creates a new Owner
func NewOwner(
	upstreamManager *upstream.Manager,
	cfg *config.SchedulerConfig,
	etcdClient etcd.CDCEtcdClient,
) Owner {
	return &ownerImpl{
		upstreamManager: upstreamManager,
		changefeeds:     make(map[model.ChangeFeedID]*changefeed),
		lastTickTime:    time.Now(),
		newChangefeed:   NewChangefeed,
		logLimiter:      rate.NewLimiter(versionInconsistentLogRate, versionInconsistentLogRate),
		cfg:             cfg,
		etcdClient:      etcdClient,
	}
}

// Tick implements the Reactor interface
func (o *ownerImpl) Tick(stdCtx context.Context, rawState orchestrator.ReactorState) (nextState orchestrator.ReactorState, err error) {
	failpoint.Inject("owner-run-with-error", func() {
		failpoint.Return(nil, errors.New("owner run with injected error"))
	})
	failpoint.Inject("sleep-in-owner-tick", nil)
	state := rawState.(*orchestrator.GlobalReactorState)

	o.captures = state.Captures
	o.updateMetrics()

	// handleJobs() should be called before clusterVersionConsistent(), because
	// when there are different versions of cdc nodes in the cluster,
	// the admin job may not be processed all the time. And http api relies on
	// admin job, which will cause all http api unavailable.
	o.handleJobs(stdCtx)

	// Tick all changefeeds.
	ctx := stdCtx.(cdcContext.Context)
	for changefeedID, changefeedState := range state.Changefeeds {
		// check if we are the changefeed owner to handle this changefeed
		if !o.shouldHandleChangefeed(changefeedState) {
			continue
		}
		if changefeedState.Info == nil {
			o.cleanUpChangefeed(changefeedState)
			if cfReactor, ok := o.changefeeds[changefeedID]; ok {
				cfReactor.isRemoved = true
			}
			continue
		}
		cfReactor, exist := o.changefeeds[changefeedID]
		if !exist {
			up, ok := o.upstreamManager.Get(changefeedState.Info.UpstreamID)
			if !ok {
				upstreamInfo := state.Upstreams[changefeedState.Info.UpstreamID]
				up = o.upstreamManager.AddUpstream(upstreamInfo)
			}
			cfReactor = o.newChangefeed(changefeedID, changefeedState.Info, changefeedState.Status,
				NewFeedStateManager(up, changefeedState),
				up, o.cfg)
			o.changefeeds[changefeedID] = cfReactor
		}
		ctx = cdcContext.WithChangefeedVars(ctx, &cdcContext.ChangefeedVars{
			ID: changefeedID,
		})
		changefeedState.CheckCaptureAlive(ctx.GlobalVars().CaptureInfo.ID)
		captures := o.getChangefeedCaptures(changefeedState, state)
		if !preflightCheck(changefeedState, captures) {
			continue
		}
		checkpointTs, minTableBarrierTs := cfReactor.Tick(ctx, changefeedState.Info, changefeedState.Status, captures)
		updateStatus(changefeedState, checkpointTs, minTableBarrierTs)
	}
	o.changefeedTicked = true

	// Cleanup changefeeds that are not in the state.
	if len(o.changefeeds) != len(state.Changefeeds) {
		for changefeedID, reactor := range o.changefeeds {
			if _, exist := state.Changefeeds[changefeedID]; exist {
				continue
			}
			reactor.Close(ctx)
			delete(o.changefeeds, changefeedID)
		}
	}

	// Close and cleanup all changefeeds.
	if atomic.LoadInt32(&o.closed) != 0 {
		for _, reactor := range o.changefeeds {
			reactor.Close(ctx)
		}
		return state, cerror.ErrReactorFinished.GenWithStackByArgs()
	}

	if err := o.upstreamManager.Tick(stdCtx, state); err != nil {
		return state, errors.Trace(err)
	}
	return state, nil
}

// preflightCheck makes sure that the metadata in Etcd is complete enough to run the tick.
// If the metadata is not complete, such as when the ChangeFeedStatus is nil,
// this function will reconstruct the lost metadata and skip this tick.
func preflightCheck(changefeed *orchestrator.ChangefeedReactorState,
	captures map[model.CaptureID]*model.CaptureInfo,
) (ok bool) {
	ok = true
	if changefeed.Status == nil {
		// complete the changefeed status when it is just created.
		changefeed.PatchStatus(
			func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
				if status == nil {
					status = &model.ChangeFeedStatus{
						// changefeed status is nil when the changefeed has just created.
						CheckpointTs:      changefeed.Info.StartTs,
						MinTableBarrierTs: changefeed.Info.StartTs,
						AdminJobType:      model.AdminNone,
					}
					return status, true, nil
				}
				return status, false, nil
			})
		ok = false
	} else if changefeed.Status.MinTableBarrierTs == 0 {
		// complete the changefeed status when the TiCDC cluster is
		// upgraded from an old version(less than v6.7.0).
		changefeed.PatchStatus(
			func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
				if status != nil {
					if status.MinTableBarrierTs == 0 {
						status.MinTableBarrierTs = status.CheckpointTs
					}
					return status, true, nil
				}
				return status, false, nil
			})
		ok = false
	}

	// clean stale capture task positions
	for captureID := range changefeed.TaskPositions {
		if _, exist := captures[captureID]; !exist {
			changefeed.PatchTaskPosition(captureID, func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
				return nil, position != nil, nil
			})
			ok = false
		}
	}
	if !ok {
		log.Info("changefeed preflight check failed, will skip this tick",
			zap.String("namespace", changefeed.ID.Namespace),
			zap.String("changefeed", changefeed.ID.ID),
			zap.Any("status", changefeed.Status), zap.Bool("ok", ok),
		)
	}

	return
}

func updateStatus(changefeed *orchestrator.ChangefeedReactorState,
	checkpointTs, minTableBarrierTs model.Ts,
) {
	if checkpointTs == 0 || minTableBarrierTs == 0 {
		return
	}
	changefeed.PatchStatus(
		func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
			changed := false
			if status == nil {
				return nil, changed, nil
			}
			if status.CheckpointTs != checkpointTs {
				status.CheckpointTs = checkpointTs
				changed = true
			}
			if status.MinTableBarrierTs != minTableBarrierTs {
				status.MinTableBarrierTs = minTableBarrierTs
				changed = true
			}
			return status, changed, nil
		})
}

// shouldHandleChangefeed returns whether the owner should handle the changefeed.
func (o *ownerImpl) shouldHandleChangefeed(_ *orchestrator.ChangefeedReactorState) bool {
	return true
}

// getChangefeedCaptures returns the captures to run the changefeed.
func (o *ownerImpl) getChangefeedCaptures(_ *orchestrator.ChangefeedReactorState,
	globalStates *orchestrator.GlobalReactorState,
) map[model.CaptureID]*model.CaptureInfo {
	return globalStates.Captures
}

// EnqueueJob enqueues an admin job into an internal queue,
// and the Owner will handle the job in the next tick
// `done` must be buffered to prevent blocking owner.
func (o *ownerImpl) EnqueueJob(adminJob model.AdminJob, done chan<- error) {
	o.pushOwnerJob(&ownerJob{
		Tp:           ownerJobTypeAdminJob,
		AdminJob:     &adminJob,
		ChangefeedID: adminJob.CfID,
		done:         done,
	})
}

// RebalanceTables triggers a rebalance for the specified changefeed
// `done` must be buffered to prevent blocking owner.
func (o *ownerImpl) RebalanceTables(cfID model.ChangeFeedID, done chan<- error) {
	o.pushOwnerJob(&ownerJob{
		Tp:           ownerJobTypeRebalance,
		ChangefeedID: cfID,
		done:         done,
	})
}

// ScheduleTable moves a table from a capture to another capture
// `done` must be buffered to prevent blocking owner.
func (o *ownerImpl) ScheduleTable(
	cfID model.ChangeFeedID, toCapture model.CaptureID, tableID model.TableID,
	done chan<- error,
) {
	o.pushOwnerJob(&ownerJob{
		Tp:              ownerJobTypeScheduleTable,
		ChangefeedID:    cfID,
		TargetCaptureID: toCapture,
		TableID:         tableID,
		done:            done,
	})
}

// DrainCapture removes all tables at the target capture
// `done` must be buffered to prevent blocking owner.
func (o *ownerImpl) DrainCapture(query *scheduler.Query, done chan<- error) {
	o.pushOwnerJob(&ownerJob{
		Tp:            ownerJobTypeDrainCapture,
		scheduleQuery: query,
		done:          done,
	})
}

// WriteDebugInfo writes debug info into the specified http writer
func (o *ownerImpl) WriteDebugInfo(w io.Writer, done chan<- error) {
	o.pushOwnerJob(&ownerJob{
		Tp:              ownerJobTypeDebugInfo,
		debugInfoWriter: w,
		done:            done,
	})
}

// Query queries owner internal information.
func (o *ownerImpl) Query(query *Query, done chan<- error) {
	o.pushOwnerJob(&ownerJob{
		Tp:    ownerJobTypeQuery,
		query: query,
		done:  done,
	})
}

// AsyncStop stops the owner asynchronously
func (o *ownerImpl) AsyncStop() {
	atomic.StoreInt32(&o.closed, 1)
	// Must be called after setting closed.
	o.cleanupOwnerJob()
	o.cleanStaleMetrics()
}

func (o *ownerImpl) UpdateChangefeedAndUpstream(ctx context.Context,
	upstreamInfo *model.UpstreamInfo,
	changeFeedInfo *model.ChangeFeedInfo,
	changeFeedID model.ChangeFeedID,
) error {
	return o.etcdClient.UpdateChangefeedAndUpstream(ctx, upstreamInfo, changeFeedInfo, changeFeedID)
}

func (o *ownerImpl) UpdateChangefeed(ctx context.Context,
	changeFeedInfo *model.ChangeFeedInfo,
) error {
	return o.etcdClient.SaveChangeFeedInfo(ctx, changeFeedInfo, model.ChangeFeedID{
		Namespace: changeFeedInfo.Namespace,
		ID:        changeFeedInfo.ID,
	})
}

func (o *ownerImpl) cleanUpChangefeed(state *orchestrator.ChangefeedReactorState) {
	state.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
		return nil, info != nil, nil
	})
	state.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		return nil, status != nil, nil
	})
	for captureID := range state.TaskPositions {
		state.PatchTaskPosition(captureID, func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
			return nil, position != nil, nil
		})
	}
}

func (o *ownerImpl) cleanStaleMetrics() {
	// The gauge metrics of the Owner should be reset
	// each time a new owner is launched, in case the previous owner
	// has crashed and has not cleaned up the stale metrics values.
	changefeedCheckpointTsGauge.Reset()
	changefeedCheckpointTsLagGauge.Reset()
	changefeedResolvedTsGauge.Reset()
	changefeedResolvedTsLagGauge.Reset()
	changefeedStatusGauge.Reset()
}

func (o *ownerImpl) updateMetrics() {
	// Keep the value of prometheus expression `rate(counter)` = 1
	// Please also change alert rule in ticdc.rules.yml when change the expression value.
	now := time.Now()
	ownershipCounter.Add(float64(now.Sub(o.lastTickTime)) / float64(time.Second))
	o.lastTickTime = now

	for cfID, cf := range o.changefeeds {
		if cf.latestInfo != nil {
			changefeedStatusGauge.WithLabelValues(cfID.Namespace, cfID.ID).
				Set(float64(cf.latestInfo.State.ToInt()))
		}
	}
}

func (o *ownerImpl) clusterVersionConsistent(captures map[model.CaptureID]*model.CaptureInfo) bool {
	versions := make(map[string]struct{}, len(captures))
	for _, capture := range captures {
		versions[capture.Version] = struct{}{}
	}

	if err := version.CheckTiCDCVersion(versions); err != nil {
		if o.logLimiter.Allow() {
			log.Warn("TiCDC cluster versions not allowed",
				zap.String("ownerVer", version.ReleaseVersion),
				zap.Any("captures", captures), zap.Error(err))
		}
		return false
	}
	return true
}

func (o *ownerImpl) handleDrainCaptures(ctx context.Context, query *scheduler.Query, done chan<- error) {
	if err := o.upstreamManager.Visit(func(upstream *upstream.Upstream) error {
		if err := version.CheckStoreVersion(ctx, upstream.PDClient, 0); err != nil {
			return errors.Trace(err)
		}
		return nil
	}); err != nil {
		log.Info("owner handle drain capture failed, since check upstream store version failed",
			zap.String("target", query.CaptureID), zap.Error(err))
		query.Resp = &model.DrainCaptureResp{CurrentTableCount: 0}
		done <- err
		close(done)
		return
	}

	var (
		changefeedWithTableCount int
		totalTableCount          int
		err                      error
	)
	for _, changefeed := range o.changefeeds {
		// Only count normal changefeed.
		state := changefeed.latestInfo.State
		if state != model.StateNormal {
			log.Info("skip drain changefeed",
				zap.String("state", string(state)),
				zap.String("target", query.CaptureID),
				zap.String("namespace", changefeed.id.Namespace),
				zap.String("changefeed", changefeed.id.ID))
			continue
		}
		if changefeed.scheduler == nil {
			// Scheduler is created lazily, it is nil before initialization.
			log.Info("drain a changefeed without scheduler",
				zap.String("state", string(state)),
				zap.String("target", query.CaptureID),
				zap.String("namespace", changefeed.id.Namespace),
				zap.String("changefeed", changefeed.id.ID))
			// To prevent a changefeed being considered drained,
			// we increase totalTableCount.
			totalTableCount++
			continue
		}
		count, e := changefeed.scheduler.DrainCapture(query.CaptureID)
		if e != nil {
			err = e
			break
		}
		if count > 0 {
			changefeedWithTableCount++
		}
		totalTableCount += count
	}

	query.Resp = &model.DrainCaptureResp{
		CurrentTableCount: totalTableCount,
	}

	if err != nil {
		log.Info("owner handle drain capture failed",
			zap.String("target", query.CaptureID), zap.Error(err))
		done <- err
		close(done)
		return
	}

	log.Info("owner handle drain capture",
		zap.String("target", query.CaptureID),
		zap.Int("changefeedWithTableCount", changefeedWithTableCount),
		zap.Int("totalTableCount", totalTableCount))
	close(done)
}

func (o *ownerImpl) handleJobs(ctx context.Context) {
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
			cfReactor.feedStateManager.PushAdminJob(job.AdminJob)
		case ownerJobTypeScheduleTable:
			// Scheduler is created lazily, it is nil before initialization.
			if cfReactor.scheduler != nil {
				cfReactor.scheduler.MoveTable(job.TableID, job.TargetCaptureID)
			}
		case ownerJobTypeDrainCapture:
			o.handleDrainCaptures(ctx, job.scheduleQuery, job.done)
			continue // continue here to prevent close the done channel twice
		case ownerJobTypeRebalance:
			// Scheduler is created lazily, it is nil before initialization.
			if cfReactor.scheduler != nil {
				cfReactor.scheduler.Rebalance()
			}
		case ownerJobTypeQuery:
			job.done <- o.handleQueries(job.query)
		case ownerJobTypeDebugInfo:
			// TODO: implement this function
		}
		close(job.done)
	}
}

func (o *ownerImpl) handleQueries(query *Query) error {
	switch query.Tp {
	case QueryChangeFeedStatuses:
		cfReactor, ok := o.changefeeds[query.ChangeFeedID]
		if !ok {
			query.Data = nil
			return nil
		}
		ret := &model.ChangeFeedStatusForAPI{}
		ret.ResolvedTs = cfReactor.resolvedTs
		ret.CheckpointTs = cfReactor.latestStatus.CheckpointTs
		query.Data = ret
	case QueryChangeFeedSyncedStatus:
		cfReactor, ok := o.changefeeds[query.ChangeFeedID]
		if !ok {
			query.Data = nil
			return nil
		}
		ret := &model.ChangeFeedSyncedStatusForAPI{}
		ret.LastSyncedTs = cfReactor.lastSyncedTs
		ret.CheckpointTs = cfReactor.latestStatus.CheckpointTs
		ret.PullerResolvedTs = cfReactor.pullerResolvedTs

		if cfReactor.latestInfo == nil {
			ret.CheckpointInterval = 0
			ret.SyncedCheckInterval = 0
		} else {
			ret.CheckpointInterval = cfReactor.latestInfo.Config.SyncedStatus.CheckpointInterval
			ret.SyncedCheckInterval = cfReactor.latestInfo.Config.SyncedStatus.SyncedCheckInterval
		}
		query.Data = ret
	case QueryChangefeedInfo:
		cfReactor, ok := o.changefeeds[query.ChangeFeedID]
		if !ok {
			query.Data = nil
			return nil
		}
		if cfReactor.latestInfo == nil {
			query.Data = &model.ChangeFeedInfo{}
		} else {
			var err error
			query.Data, err = cfReactor.latestInfo.Clone()
			if err != nil {
				return errors.Trace(err)
			}
		}
	case QueryAllTaskStatuses:
		cfReactor, ok := o.changefeeds[query.ChangeFeedID]
		if !ok {
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
	case QueryProcessors:
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
	case QueryCaptures:
		var ret []*model.CaptureInfo
		for _, captureInfo := range o.captures {
			ret = append(ret, &model.CaptureInfo{
				ID:            captureInfo.ID,
				AdvertiseAddr: captureInfo.AdvertiseAddr,
				Version:       captureInfo.Version,
			})
		}
		query.Data = ret
	case QueryHealth:
		query.Data = o.isHealthy()
	case QueryOwner:
		_, exist := o.changefeeds[query.ChangeFeedID]
		query.Data = exist
	}
	return nil
}

func (o *ownerImpl) isHealthy() bool {
	if !o.changefeedTicked {
		// Owner has not yet tick changefeeds, some changefeeds may be not
		// initialized.
		log.Warn("owner is not healthy since changefeeds are not ticked")
		return false
	}
	if !o.clusterVersionConsistent(o.captures) {
		return false
	}
	for _, changefeed := range o.changefeeds {
		if changefeed.latestInfo == nil {
			continue
		}
		if changefeed.latestInfo.State != model.StateNormal {
			log.Warn("isHealthy: changefeed not normal",
				zap.String("namespace", changefeed.id.Namespace),
				zap.String("changefeed", changefeed.id.ID),
				zap.Any("state", changefeed.latestInfo.State))
			continue
		}

		provider := changefeed.GetInfoProvider()
		if provider == nil || !provider.IsInitialized() {
			// The scheduler has not been initialized yet, it is considered
			// unhealthy, because owner can not schedule tables for now.
			log.Warn("isHealthy: changefeed is not initialized",
				zap.String("namespace", changefeed.id.Namespace),
				zap.String("changefeed", changefeed.id.ID))
			return false
		}
	}
	return true
}

func (o *ownerImpl) takeOwnerJobs() []*ownerJob {
	o.ownerJobQueue.Lock()
	defer o.ownerJobQueue.Unlock()

	jobs := o.ownerJobQueue.queue
	o.ownerJobQueue.queue = nil
	return jobs
}

func (o *ownerImpl) pushOwnerJob(job *ownerJob) {
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

func (o *ownerImpl) cleanupOwnerJob() {
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

// StatusProvider returns a StatusProvider
func (o *ownerImpl) StatusProvider() StatusProvider {
	return &ownerStatusProvider{owner: o}
}
