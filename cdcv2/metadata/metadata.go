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

// Querier is used to query information from metadata storage.
type Querier interface {
	// GetChangefeeds queries some or all changefeeds.
	GetChangefeed(...ChangefeedUUID) ([]*ChangefeedInfo, error)
	// GetChangefeedState queries some or all changefeed states.
	GetChangefeedState(...ChangefeedUUID) ([]*ChangefeedState, error)
	// GetChangefeedProgress queries some or all changefeed progresses.
	GetChangefeedProgress(...ChangefeedUUID) (map[ChangefeedUUID]ChangefeedProgress, error)
}

// -------------------- About owner schedule -------------------- //
// 1. ControllerObservation.SetOwner puts an owner on a given capture;
// 2. ControllerObservation.SetOwner can also stop an owner;
// 3. Capture fetches owner launch/stop events with CaptureObservation.OwnerChanges;
// 4. Capture calls Capture.PostOwnerRemoved when the owner exits;
// 5. After controller confirms the old owner exits, it can re-reschedule it.
// -------------------------------------------------------------- //

// ---------- About changefeed processor captures schedule ---------- //
// 1. ControllerObservation.SetProcessors attaches some captures to a changefeed;
// 2. ControllerObservation.SetProcessors can also detach captures from a changefeed;
// 3. Owner calls OwnerObservation.ProcessorChanges to know processors are created;
// 4. Capture fetches processor launch/stop events with CaptureObservation.ProcessorChanges;
// 5. How to rolling-update a changefeed with only one worker capture:
//    * controller needs only to attach more captures to the changefeed;
//    * it's owner's responsibility to evict tables between captures.
// 5. What if owner knows processors are created before captures?
//    * table schedule should be robust enough.
// ------------------------------------------------------------------ //

// ---------------- About keep-alive and heartbeat ---------------- //
// 1. Capture updates heartbeats to metadata by calling CaptureObservation.Heartbeat,
//    with a given timeout, for example, 1s;
// 2. On a capture, controller, owners and processors share one same Context, which is
//    associated with deadline 10s. CaptureObservation.Heartbeat will refresh the deadline.
// 3. Controller is binded with a lease (10+1+1)s, for deadline, heartbeat time-elapsed
//    and network clock skew.
// 4. Controller needs to consider re-schedule owners and processors from a capture,
//    if the capture has been partitioned with metadata storage more than lease+5s;
// ---------------------------------------------------------------- //

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
	Advance(cp CaptureProgress) error

	// OwnerChanges fetch owner modifications.
	OwnerChanges() <-chan ScheduledChangefeed

	// OnOwnerLaunched create an owner observation for a changefeed owner.
	OnOwnerLaunched(cf ChangefeedUUID) OwnerObservation

	// PostOwnerRemoved inform the metadata storage when an owner exits.
	PostOwnerRemoved(cf ChangefeedUUID, taskPosition ChangefeedProgress) error
}

// ControllerObservation is for observing and updating meta by Controller.
//
// All intrefaces are thread-safe and shares one same Context.
type ControllerObservation interface {
	// CreateChangefeed creates a changefeed, UUID will be filled into the input ChangefeedInfo.
	CreateChangefeed(cf *ChangefeedInfo, up *model.UpstreamInfo) (ChangefeedIdent, error)

	// RemoveChangefeed removes a changefeed, will mark it as removed and stop the owner and processors asynchronizely.
	RemoveChangefeed(cf ChangefeedUUID) error

	// CleanupChangefeed cleans up a changefeed, will delete info, schdule and state metadata.
	CleanupChangefeed(cf ChangefeedUUID) error

	// RefreshCaptures Fetch the latest capture list in the TiCDC cluster.
	RefreshCaptures() (captures []*model.CaptureInfo, changed bool)

	// SetOwner Schedule a changefeed owner to a given target.
	// Notes:
	//   * the target capture can fetch the event by `OwnerChanges`.
	//   * target state can only be `SchedLaunched` or `SchedRemoving`.
	SetOwner(target ScheduledChangefeed) error

	// GetChangefeedSchedule Get current schedule of the given changefeed.
	GetChangefeedSchedule(cf ChangefeedUUID) (ScheduledChangefeed, error)

	// ScheduleSnapshot Get a snapshot of all changefeeds current schedule.
	ScheduleSnapshot() ([]ScheduledChangefeed, []*model.CaptureInfo, error)
}

// OwnerObservation is for observing and updating running status of a changefeed.
//
// All intrefaces are thread-safe and shares one same Context.
type OwnerObservation interface {
	// Self returns the changefeed info of the owner.
	Self() ChangefeedUUID

	// UpdateChangefeed updates changefeed metadata, must be called on a paused one.
	UpdateChangefeed(*ChangefeedInfo) error

	// PauseChangefeed pauses a changefeed.
	PauseChangefeed() error

	// ResumeChangefeed resumes a changefeed.
	ResumeChangefeed() error

	// SetChangefeedFinished set the changefeed to state finished.
	SetChangefeedFinished() error

	// SetChangefeedRemoved set the changefeed to state removed.
	SetChangefeedRemoved() error

	// SetChangefeedFailed set the changefeed to state failed.
	SetChangefeedFailed(err *model.RunningError) error

	// SetChangefeedWarning set the changefeed to state warning.
	SetChangefeedWarning(warn *model.RunningError) error

	// SetChangefeedPending sets the changefeed to state pending.
	SetChangefeedPending(err *model.RunningError) error
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
