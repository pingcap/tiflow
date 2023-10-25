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

//nolint:unused
package controller

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/controller"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdcv2/metadata"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"github.com/pingcap/tiflow/pkg/upstream"
	"go.uber.org/zap"
)

var _ controller.Controller = &controllerImpl{}

type controllerImpl struct {
	captureInfo     *model.CaptureInfo
	captures        map[model.CaptureID]*model.CaptureInfo
	upstreamManager *upstream.Manager

	lastTickTime time.Time
	// bootstrapped specifies whether the controller has been initialized.
	// This will only be done when the controller starts the first Tick.
	// NOTICE: Do not use it in a method other than tick unexpectedly,
	//         as it is not a thread-safe value.
	bootstrapped bool
	closed       int32

	controllerObservation metadata.ControllerObservation
	captureObservation    metadata.CaptureObservation
}

// NewController creates a new Controller
func NewController(
	upstreamManager *upstream.Manager,
	captureInfo *model.CaptureInfo,
	controllerObservation metadata.ControllerObservation,
	captureObservation metadata.CaptureObservation,
) *controllerImpl {
	return &controllerImpl{
		upstreamManager:       upstreamManager,
		captures:              map[model.CaptureID]*model.CaptureInfo{},
		lastTickTime:          time.Now(),
		captureInfo:           captureInfo,
		controllerObservation: controllerObservation,
		captureObservation:    captureObservation,
	}
}

func (o *controllerImpl) Run(stdCtx context.Context) error {
	tick := time.Tick(time.Second * 5)
	for {
		select {
		case <-stdCtx.Done():
			return stdCtx.Err()
		case <-tick:
			changefeeds, captures, err := o.controllerObservation.ScheduleSnapshot()
			if err != nil {
				log.Error("failed to get snapshot", zap.Error(err))
			}
			log.Info("controller snapshot",
				zap.Int("changefeeds", len(changefeeds)),
				zap.Int("captures", len(captures)))

			// if closed, exit the etcd worker loop
			if atomic.LoadInt32(&o.closed) != 0 {
				return cerror.ErrReactorFinished.GenWithStackByArgs()
			}
		}
	}
}

func (o *controllerImpl) Tick(ctx context.Context,
	state orchestrator.ReactorState,
) (nextState orchestrator.ReactorState, err error) {
	// TODO implement me
	panic("implement me")
}

func (o *controllerImpl) AsyncStop() {
	// TODO implement me
	panic("implement me")
}

func (o *controllerImpl) GetChangefeedOwnerCaptureInfo(id model.ChangeFeedID) *model.CaptureInfo {
	// TODO implement me
	panic("implement me")
}

func (o *controllerImpl) GetAllChangeFeedInfo(ctx context.Context) (map[model.ChangeFeedID]*model.ChangeFeedInfo, error) {
	// TODO implement me
	panic("implement me")
}

func (o *controllerImpl) GetAllChangeFeedCheckpointTs(ctx context.Context) (map[model.ChangeFeedID]uint64, error) {
	// TODO implement me
	panic("implement me")
}

func (o *controllerImpl) GetCaptures(ctx context.Context) ([]*model.CaptureInfo, error) {
	// TODO implement me
	panic("implement me")
}

func (o *controllerImpl) GetProcessors(ctx context.Context) ([]*model.ProcInfoSnap, error) {
	// TODO implement me
	panic("implement me")
}

func (o *controllerImpl) IsChangefeedExists(ctx context.Context, id model.ChangeFeedID) (bool, error) {
	// TODO implement me
	panic("implement me")
}

func (o *controllerImpl) CreateChangefeed(
	ctx context.Context,
	upstreamInfo *model.UpstreamInfo,
	changefeedInfo *model.ChangeFeedInfo,
) error {
	// TODO implement me
	panic("implement me")
}
