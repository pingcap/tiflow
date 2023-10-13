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

package controller

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"github.com/pingcap/tiflow/pkg/upstream"
	"github.com/pingcap/tiflow/pkg/version"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

type controllerJobType int

// All ControllerJob types
const (
	controllerJobTypeQuery controllerJobType = iota
)

// versionInconsistentLogRate represents the rate of log output when there are
// captures with versions different from that of the controller
const versionInconsistentLogRate = 1

// Controller is a manager to schedule changefeeds
type Controller interface {
	orchestrator.Reactor
	AsyncStop()
	GetChangefeedOwnerCaptureInfo(id model.ChangeFeedID) *model.CaptureInfo
	GetAllChangeFeedInfo(ctx context.Context) (
		map[model.ChangeFeedID]*model.ChangeFeedInfo, error,
	)
	GetAllChangeFeedCheckpointTs(ctx context.Context) (
		map[model.ChangeFeedID]uint64, error,
	)
	GetCaptures(ctx context.Context) ([]*model.CaptureInfo, error)
	GetProcessors(ctx context.Context) ([]*model.ProcInfoSnap, error)
	IsChangefeedExists(ctx context.Context, id model.ChangeFeedID) (bool, error)
	CreateChangefeed(context.Context,
		*model.UpstreamInfo,
		*model.ChangeFeedInfo,
	) error
}

var _ Controller = &controllerImpl{}

type controllerImpl struct {
	changefeeds     map[model.ChangeFeedID]*orchestrator.ChangefeedReactorState
	captures        map[model.CaptureID]*model.CaptureInfo
	upstreamManager *upstream.Manager

	// logLimiter controls cluster version check log output rate
	logLimiter   *rate.Limiter
	lastTickTime time.Time
	// bootstrapped specifies whether the controller has been initialized.
	// This will only be done when the controller starts the first Tick.
	// NOTICE: Do not use it in a method other than tick unexpectedly,
	//         as it is not a thread-safe value.
	bootstrapped bool

	closed int32

	controllerJobQueue struct {
		sync.Mutex
		queue []*controllerJob
	}
	etcdClient etcd.CDCEtcdClient

	captureInfo *model.CaptureInfo
}

// NewController creates a new Controller
func NewController(
	upstreamManager *upstream.Manager,
	captureInfo *model.CaptureInfo,
	etcdClient etcd.CDCEtcdClient,
) Controller {
	return &controllerImpl{
		upstreamManager: upstreamManager,
		changefeeds:     make(map[model.ChangeFeedID]*orchestrator.ChangefeedReactorState),
		lastTickTime:    time.Now(),
		logLimiter:      rate.NewLimiter(versionInconsistentLogRate, versionInconsistentLogRate),
		captureInfo:     captureInfo,
		etcdClient:      etcdClient,
	}
}

// Tick implements the Reactor interface
func (o *controllerImpl) Tick(stdCtx context.Context, rawState orchestrator.ReactorState) (nextState orchestrator.ReactorState, err error) {
	state := rawState.(*orchestrator.GlobalReactorState)
	o.captures = state.Captures

	// At the first Tick, we need to do a bootstrap operation.
	// Fix incompatible or incorrect meta information.
	if !o.bootstrapped {
		o.Bootstrap(state)
		o.bootstrapped = true
		return state, nil
	}
	// handleJobs() should be called before clusterVersionConsistent(), because
	// when there are different versions of cdc nodes in the cluster,
	// the admin job may not be processed all the time. And http api relies on
	// admin job, which will cause all http api unavailable.
	o.handleJobs(stdCtx)

	if !o.clusterVersionConsistent(state.Captures) {
		return state, nil
	}
	// controller should update GC safepoint before initializing changefeed, so
	// changefeed can remove its "ticdc-creating" service GC safepoint during
	// initializing.
	//
	// See more gc doc.
	if err = o.updateGCSafepoint(stdCtx, state); err != nil {
		return nil, errors.Trace(err)
	}

	// Tick all changefeeds.
	// ctx := stdCtx.(cdcContext.Context)
	for _, changefeed := range state.Changefeeds {
		o.changefeeds[changefeed.ID] = changefeed
	}

	// Cleanup changefeeds that are not in the state.
	if len(o.changefeeds) != len(state.Changefeeds) {
		for changefeedID := range o.changefeeds {
			if _, exist := state.Changefeeds[changefeedID]; exist {
				continue
			}
			delete(o.changefeeds, changefeedID)
		}
	}

	// if closed, exit the etcd worker loop
	if atomic.LoadInt32(&o.closed) != 0 {
		return state, cerror.ErrReactorFinished.GenWithStackByArgs()
	}

	return state, nil
}

// Bootstrap checks if the state contains incompatible or incorrect information and tries to fix it.
func (o *controllerImpl) Bootstrap(state *orchestrator.GlobalReactorState) {
	log.Info("Start bootstrapping")
	fixChangefeedInfos(state)
}

// fixChangefeedInfos attempts to fix incompatible or incorrect meta information in changefeed state.
func fixChangefeedInfos(state *orchestrator.GlobalReactorState) {
	for _, changefeedState := range state.Changefeeds {
		if changefeedState != nil {
			changefeedState.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
				if info == nil {
					return nil, false, nil
				}
				info.FixIncompatible()
				return info, true, nil
			})
		}
	}
}

func (o *controllerImpl) clusterVersionConsistent(captures map[model.CaptureID]*model.CaptureInfo) bool {
	versions := make(map[string]struct{}, len(captures))
	for _, capture := range captures {
		versions[capture.Version] = struct{}{}
	}

	if err := version.CheckTiCDCVersion(versions); err != nil {
		if o.logLimiter.Allow() {
			log.Warn("TiCDC cluster versions not allowed",
				zap.String("controllerVersion", version.ReleaseVersion),
				zap.Any("captures", captures), zap.Error(err))
		}
		return false
	}
	return true
}

func (o *controllerImpl) updateGCSafepoint(
	ctx context.Context, state *orchestrator.GlobalReactorState,
) error {
	minChekpoinTsMap, forceUpdateMap := o.calculateGCSafepoint(state)

	for upstreamID, minCheckpointTs := range minChekpoinTsMap {
		up, ok := o.upstreamManager.Get(upstreamID)
		if !ok {
			upstreamInfo := state.Upstreams[upstreamID]
			up = o.upstreamManager.AddUpstream(upstreamInfo)
		}
		if !up.IsNormal() {
			log.Warn("upstream is not ready, skip",
				zap.Uint64("id", up.ID),
				zap.Strings("pd", up.PdEndpoints))
			continue
		}

		// When the changefeed starts up, CDC will do a snapshot read at
		// (checkpointTs - 1) from TiKV, so (checkpointTs - 1) should be an upper
		// bound for the GC safepoint.
		gcSafepointUpperBound := minCheckpointTs - 1

		var forceUpdate bool
		if _, exist := forceUpdateMap[upstreamID]; exist {
			forceUpdate = true
		}

		err := up.GCManager.TryUpdateGCSafePoint(ctx, gcSafepointUpperBound, forceUpdate)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// calculateGCSafepoint calculates GCSafepoint for different upstream.
// Note: we need to maintain a TiCDC service GC safepoint for each upstream TiDB cluster
// to prevent upstream TiDB GC from removing data that is still needed by TiCDC.
// GcSafepoint is the minimum checkpointTs of all changefeeds that replicating a same upstream TiDB cluster.
func (o *controllerImpl) calculateGCSafepoint(state *orchestrator.GlobalReactorState) (
	map[uint64]uint64, map[uint64]interface{},
) {
	minCheckpointTsMap := make(map[uint64]uint64)
	forceUpdateMap := make(map[uint64]interface{})

	for changefeedID, changefeedState := range state.Changefeeds {
		if changefeedState.Info == nil || !changefeedState.Info.NeedBlockGC() {
			continue
		}

		checkpointTs := changefeedState.Info.GetCheckpointTs(changefeedState.Status)
		upstreamID := changefeedState.Info.UpstreamID

		if _, exist := minCheckpointTsMap[upstreamID]; !exist {
			minCheckpointTsMap[upstreamID] = checkpointTs
		}

		minCpts := minCheckpointTsMap[upstreamID]

		if minCpts > checkpointTs {
			minCpts = checkpointTs
			minCheckpointTsMap[upstreamID] = minCpts
		}
		// Force update when adding a new changefeed.
		_, exist := o.changefeeds[changefeedID]
		if !exist {
			forceUpdateMap[upstreamID] = nil
		}
	}

	// check if the upstream has a changefeed, if not we should update the gc safepoint
	_ = o.upstreamManager.Visit(func(up *upstream.Upstream) error {
		if _, exist := minCheckpointTsMap[up.ID]; !exist {
			ts := up.PDClock.CurrentTime()
			minCheckpointTsMap[up.ID] = oracle.GoTimeToTS(ts)
		}
		return nil
	})
	return minCheckpointTsMap, forceUpdateMap
}

// AsyncStop stops the server manager asynchronously
func (o *controllerImpl) AsyncStop() {
	atomic.StoreInt32(&o.closed, 1)
}

// GetChangefeedOwnerCaptureInfo returns the capture info of the owner of the changefeed
func (o *controllerImpl) GetChangefeedOwnerCaptureInfo(id model.ChangeFeedID) *model.CaptureInfo {
	// todo: schedule changefeed owner to other capture
	return o.captureInfo
}

func (o *controllerImpl) CreateChangefeed(ctx context.Context,
	upstreamInfo *model.UpstreamInfo,
	cfInfo *model.ChangeFeedInfo,
) error {
	return o.etcdClient.CreateChangefeedInfo(ctx, upstreamInfo, cfInfo)
}

// Export field names for pretty printing.
type controllerJob struct {
	Tp           controllerJobType
	ChangefeedID model.ChangeFeedID

	// for status provider
	query *Query

	done chan<- error
}
