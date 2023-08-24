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

package metadata

import (
	"context"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/election"
	"github.com/pingcap/tiflow/pkg/errors"
)

// Querier is used to query informations from metadata storage.
type Querier interface {
	// GetChangefeed queries some or all changefeeds.
	GetChangefeeds(...model.ChangeFeedID) ([]*ChangefeedInfo, []ChangefeedID, error)
}

// CaptureObservation is for observing and updating metadata on a CAPTURE instance.
//
// All intrefaces are thread-safe and shares one same Context.
type CaptureObservation interface {
	Elector

	// Run runs
	// the `eclector.RunElection` and other background tasks.
	// controllerCallback will be called when the capture campaign as the controller.
	Run(
		ctx context.Context,
		controllerCallback func(context.Context, ControllerObservation) error,
	) error

	// Advance advances some changefeed progresses that are collected from processors.
	Advance(cfs []ChangefeedID, progresses []ChangefeedProgress) error

	// Fetch owner modifications.
	OwnerChanges() <-chan ScheduledChangefeed

	// When an owner exits, inform the metadata storage.
	PostOwnerRemoved(cf ChangefeedID) error

	// Fetch processor list modifications.
	ProcessorChanges() <-chan ScheduledChangefeed

	// When a processor exits, inform the metadata storage.
	PostProcessorRemoved(cf ChangefeedID) error
}

// ControllerObservation is for observing and updating meta by Controller.
//
// All intrefaces are thread-safe and shares one same Context.
type ControllerObservation interface {
	// CreateChangefeed creates a changefeed, Epoch will be filled into the input ChangefeedInfo.
	CreateChangefeed(cf *ChangefeedInfo, up *model.UpstreamInfo) (ChangefeedID, error)

	// RemoveChangefeed removes a changefeed, will auto stop owner and processors.
	RemoveChangefeed(cf ChangefeedID) error

	// Fetch the latest capture list in the TiCDC cluster.
	RefreshCaptures() (captures []*model.CaptureInfo, changed bool)

	// Schedule a changefeed owner to a given target.
	// Notes:
	//   * the target capture can fetch the event by `OwnerChanges`.
	//   * target state can only be `SchedLaunched` or `SchedRemoving`.
	SetOwner(cf ChangefeedID, target ScheduledChangefeed) error

	// Schedule some captures as workers to a given changefeed.
	// Notes:
	//   * target captures can fetch the event by `ProcessorChanges`.
	//   * target state can only be `SchedLaunched` or `SchedRemoving`.
	SetProcessors(cf ChangefeedID, workers []ScheduledChangefeed) error

	// Get current schedule of the given changefeed.
	GetChangefeedSchedule(cf ChangefeedID) (ChangefeedSchedule, error)

	// Get a snapshot of all changefeeds current schedule.
	ScheduleSnapshot() ([]ChangefeedSchedule, []*model.CaptureInfo, error)
}

// OwnerObservation is for observing and updating running status of a changefeed.
//
// All intrefaces are thread-safe and shares one same Context.
type OwnerObservation interface {
	Self() (*ChangefeedInfo, ChangefeedID)

	// PauseChangefeed pauses a changefeed.
	PauseChangefeed() error

	// ResumeChangefeed resumes a changefeed.
	ResumeChangefeed() error

	// UpdateChangefeed updates changefeed metadata, must be called on a paused one.
	UpdateChangefeed(*ChangefeedInfo) error

	// set the changefeed to state finished.
	SetChangefeedFinished() error

	// Set the changefeed to state failed.
	SetChangefeedFailed(err model.RunningError) error

	// Set the changefeed to state warning.
	SetChangefeedWarning(warn model.RunningError) error

	// Set the changefeed to state pending.
	SetChangefeedPending() error

	// Fetch the latest capture list to launch processors.
	RefreshProcessors() (captures []ScheduledChangefeed, changed bool)
}

// Elector is used to campaign for capture controller.
type Elector interface {
	// Self tells the caller who am I.
	Self() *model.CaptureInfo

	// RunElection runs the elector to continuously campaign for leadership
	// until the context is canceled.
	// onTakeControl will be called when the capture campaign as the controller.
	RunElection(ctx context.Context, onTakeControl func(ctx context.Context) error) error

	// GetController returns the last observed controller whose lease is still valid.
	GetController() (*model.CaptureInfo, error)

	// GetCaptures queries some or all captures.
	GetCaptures(...model.CaptureID) ([]*model.CaptureInfo, error)
}

// NewElector creates a new elector.
func NewElector(
	selfInfo *model.CaptureInfo,
	storage election.Storage,
) Elector {
	return &electorImpl{
		selfInfo: selfInfo,
		config: election.Config{
			ID:              selfInfo.ID,
			Name:            selfInfo.Version, /* TODO: refine this filed */
			Address:         selfInfo.AdvertiseAddr,
			Storage:         storage,
			ExitOnRenewFail: true,
		},
	}
}

type electorImpl struct {
	selfInfo *model.CaptureInfo

	config  election.Config
	elector election.Elector
}

func (e *electorImpl) Self() *model.CaptureInfo {
	return e.selfInfo
}

func (e *electorImpl) RunElection(
	ctx context.Context, onTakeControl func(ctx context.Context) error,
) (err error) {
	e.config.LeaderCallback = onTakeControl
	e.elector, err = election.NewElector(e.config)
	if err != nil {
		return errors.Trace(err)
	}
	return e.elector.RunElection(ctx)
}

func (e *electorImpl) GetController() (*model.CaptureInfo, error) {
	leader, ok := e.elector.GetLeader()
	if !ok {
		return nil, errors.ErrOwnerNotFound.GenWithStackByArgs()
	}

	return &model.CaptureInfo{
		ID:            leader.ID,
		AdvertiseAddr: leader.Address,
		Version:       leader.Name,
	}, nil
}

func (e *electorImpl) GetCaptures(captureIDs ...model.CaptureID) ([]*model.CaptureInfo, error) {
	captureIDSet := make(map[string]struct{}, len(captureIDs))
	for _, cp := range captureIDs {
		captureIDSet[cp] = struct{}{}
	}

	members := e.elector.GetMembers()
	captureInfos := make([]*model.CaptureInfo, 0, len(members))
	for _, m := range members {
		if _, ok := captureIDSet[m.ID]; ok || len(captureIDs) == 0 {
			captureInfos = append(captureInfos, &model.CaptureInfo{
				ID:            m.ID,
				AdvertiseAddr: m.Address,
				Version:       m.Name,
			})
		}
	}
	return captureInfos, nil
}
