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
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdcv2/metadata"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"github.com/pingcap/tiflow/pkg/upstream"
	"github.com/pingcap/tiflow/pkg/version"
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
}

type controllerImpl struct {
	changefeeds     map[model.ChangeFeedID]metadata.ScheduledChangefeed
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

	captureInfo *model.CaptureInfo

	controllerObservation metadata.ControllerObservation
}

func (o *controllerImpl) CreateChangefeedInfo(ctx context.Context,
	upstreamInfo *model.UpstreamInfo, cfInfo *model.ChangeFeedInfo, id model.ChangeFeedID) error {
	_, err := o.controllerObservation.CreateChangefeed(&metadata.ChangefeedInfo{
		Config:   cfInfo.Config,
		TargetTs: cfInfo.TargetTs,
		SinkURI:  cfInfo.SinkURI,
		StartTs:  cfInfo.StartTs,
		ChangefeedIdent: metadata.ChangefeedIdent{
			ID:        id.ID,
			Namespace: id.Namespace,
		},
	}, upstreamInfo)
	return err
}

// NewController creates a new Controller
func NewController(
	upstreamManager *upstream.Manager,
	captureInfo *model.CaptureInfo,
	controllerObservation metadata.ControllerObservation,
) *controllerImpl {
	return &controllerImpl{
		upstreamManager:       upstreamManager,
		changefeeds:           make(map[model.ChangeFeedID]metadata.ScheduledChangefeed),
		captures:              map[model.CaptureID]*model.CaptureInfo{},
		lastTickTime:          time.Now(),
		logLimiter:            rate.NewLimiter(versionInconsistentLogRate, versionInconsistentLogRate),
		captureInfo:           captureInfo,
		controllerObservation: controllerObservation,
	}
}

// Tick implements the Reactor interface
func (o *controllerImpl) Tick(stdCtx context.Context, rawState orchestrator.ReactorState) (nextState orchestrator.ReactorState, err error) {
	return nil, nil
}

func (o *controllerImpl) Run(stdCtx context.Context) error {
	tick := time.Tick(time.Second)
	for {
		select {
		case <-stdCtx.Done():
		case <-tick:
			changefeeds, captures, err := o.controllerObservation.ScheduleSnapshot()
			if err != nil {
				log.Error("failed to get snapshot", zap.Error(err))
			}

			o.captures = map[model.CaptureID]*model.CaptureInfo{}
			captures, _ = o.controllerObservation.RefreshCaptures()
			captureChangefeedSize := make(map[*model.CaptureID]int)
			for _, capture := range captures {
				o.captures[capture.ID] = capture
				captureChangefeedSize[&capture.ID] = 0
			}

			// At the first Tick, we need to do a bootstrap operation.
			// Fix incompatible or incorrect meta information.
			if !o.bootstrapped {
				o.Bootstrap()
				o.bootstrapped = true
				continue
			}
			// handleJobs() should be called before clusterVersionConsistent(), because
			// when there are different versions of cdc nodes in the cluster,
			// the admin job may not be processed all the time. And http api relies on
			// admin job, which will cause all http api unavailable.
			o.handleJobs(stdCtx)

			if !o.clusterVersionConsistent(o.captures) {
				return nil
			}
			// controller should update GC safepoint before initializing changefeed, so
			// changefeed can remove its "ticdc-creating" service GC safepoint during
			// initializing.
			//
			// See more gc doc.
			if err = o.updateGCSafepoint(stdCtx); err != nil {
				return errors.Trace(err)
			}

			// Tick all changefeeds.
			// ctx := stdCtx.(cdcContext.Context)

			var unssignedChangefeeds []metadata.ScheduledChangefeed
			newMap := make(map[model.ChangeFeedID]struct{})
			for _, changefeed := range changefeeds {
				o.changefeeds[model.ChangeFeedID{}] = changefeed
				newMap[model.ChangeFeedID{}] = struct{}{}
				if changefeed.Owner == nil || o.captures[*changefeed.Owner] == nil {
					unssignedChangefeeds = append(unssignedChangefeeds, changefeed)
					continue
				}
				captureChangefeedSize[changefeed.Owner]++
			}

			for _, changefeed := range unssignedChangefeeds {
				var captureID *model.CaptureID
				var maxChangefeedNumPerCapture = math.MaxInt
				for id, size := range captureChangefeedSize {
					if size < maxChangefeedNumPerCapture {
						captureID = id
						maxChangefeedNumPerCapture = size
					}
				}
				if captureID == nil {
					log.Warn("no capture available to assign changefeed",
						zap.Any("changefeed", changefeed))
					continue
				}
				changefeed.Owner = captureID
				changefeed.OwnerState = metadata.SchedLaunched
				if err := o.controllerObservation.SetOwner(changefeed); err != nil {
					changefeed.Owner = nil
					changefeed.OwnerState = metadata.SchedRemoved
					log.Warn("assign changefeed owner failed", zap.Error(err))
				} else {
					captureChangefeedSize[captureID]++
				}
			}

			// Cleanup changefeeds that are not in the state.
			if len(o.changefeeds) != len(changefeeds) {
				for changefeedID := range o.changefeeds {
					if _, exist := newMap[changefeedID]; exist {
						continue
					}
					delete(o.changefeeds, changefeedID)
				}
			}

			// if closed, exit the etcd worker loop
			if atomic.LoadInt32(&o.closed) != 0 {
				return cerror.ErrReactorFinished.GenWithStackByArgs()
			}
		}
	}
}

// Bootstrap checks if the state contains incompatible or incorrect information and tries to fix it.
func (o *controllerImpl) Bootstrap() {
	log.Info("Start bootstrapping")
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

func (o *controllerImpl) updateGCSafepoint(ctx context.Context) error {
	minChekpoinTsMap, forceUpdateMap := o.calculateGCSafepoint()

	for upstreamID, minCheckpointTs := range minChekpoinTsMap {
		up, _ := o.upstreamManager.Get(upstreamID)
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
func (o *controllerImpl) calculateGCSafepoint() (
	map[uint64]uint64, map[uint64]interface{},
) {
	minCheckpointTsMap := make(map[uint64]uint64)
	forceUpdateMap := make(map[uint64]interface{})

	/*todo: calculateGCSafepoint
	for changefeedID, changefeedState := range o.changefeeds {
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
	})*/
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

func (o *controllerImpl) CreateChangefeed(cf *model.ChangeFeedInfo, up *model.UpstreamInfo) error {
	_, err := o.controllerObservation.CreateChangefeed(&metadata.ChangefeedInfo{
		StartTs:    cf.StartTs,
		UpstreamID: cf.UpstreamID,
		Config:     cf.Config,
		TargetTs:   cf.TargetTs,
	}, up)
	return err
}

func (o *controllerImpl) RemoveChangefeed(cfID model.ChangeFeedID) error {
	c, ok := o.changefeeds[cfID]
	if !ok {
		return nil
	}
	return o.controllerObservation.RemoveChangefeed(c.ChangefeedUUID)
}

// Export field names for pretty printing.
type controllerJob struct {
	Tp           controllerJobType
	ChangefeedID model.ChangeFeedID

	// for status provider
	query *Query

	done chan<- error
}
