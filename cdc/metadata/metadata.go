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
	"github.com/pingcap/tiflow/pkg/config"
)

// ChangefeedInfo is a minimal info collection to describe a changefeed.
type ChangefeedInfo struct {
	ChangefeedID model.ChangeFeedID
	SinkURI      string
	StartTs      uint64
	TargetTs     uint64
	Config       *config.ReplicaConfig

	// Epoch can't be specified by users. It's used by TiCDC internally
	// to tell distinct changefeeds with a same ID.
	Epoch uint64
}

// UpstreamInfo is a minimal info collection to describe an upstream.
type UpstreamInfo struct {
	ID            uint64
	PDEndpoints   string
	KeyPath       string
	CertPath      string
	CAPath        string
	CertAllowedCN []string
}

// ChangefeedProgress is for changefeed progress.
type ChangefeedProgress struct {
	CheckpointTs      uint64
	MinTableBarrierTs uint64
}

// CaptureInfo indicates a capture.
type CaptureInfo struct {
	ID            string
	AdvertiseAddr string
	Version       string
}

// -------------------- About owner schedule -------------------- //
// 1. ControllerObservation.SetOwner puts an owner on a given capture;
// 2. ControllerObservation.SetOwner can also stop an owner;
// 3. Capture fetches owner launch/stop events with CaptureObservation.RefreshOwners;
// 4. Capture calls Capture.PostOwnerRemoved when the owner exits;
// 5. After controller confirms the old owner exits, it can re-reschedule it.
// -------------------------------------------------------------- //

// ---------- About changefeed processor captures schedule ---------- //
// 1. ControllerObservation.SetProcessors attaches some captures to a changefeed;
// 2. ControllerObservation.SetProcessors can also detach captures from a changefeed;
// 3. Owner calls OwnerObservation.RefreshProcessors to know processors are created;
// 4. Capture fetches processor launch/stop events with CaptureObservation.RefreshProcessors;
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

// SchedState is the type of state to schedule owners and processors.
type SchedState int

const (
	// SchedLaunched means the owner or processor is launched.
	SchedLaunched SchedState = iota
	// SchedRemoving means the owner or processor is in removing.
	SchedRemoving
	// SchedRemoved means the owner or processor is removed.
	SchedRemoved
)

// StateToString convert state to string.
func StateToString(state SchedState) string {
	switch state {
	case SchedLaunched:
		return "Launched"
	case SchedRemoving:
		return "Removing"
	case SchedRemoved:
		return "Removed"
	}
	return "unreachable"
}

// ScheduledOwner is for owner and processor schedule.
type ScheduledOwner struct {
	*ChangefeedInfo
	ChangefeedProgress
	Capture *CaptureInfo
	State   SchedState
}

// ScheduledProcessor is for owner and processor schedule.
type ScheduledProcessor struct {
	*ChangefeedInfo
	ChangefeedProgress
	Capture *CaptureInfo
	State   SchedState
}

// ChangefeedSchedule is used to query changefeed schedule information.
type ChangefeedSchedule struct {
	Owner     ScheduledOwner
	Processor []ScheduledProcessor
}

// CaptureObservation is for observing and updating metadata on a CAPTURE instance.
//
// All intrefaces are thread-safe and shares one same Context.
type CaptureObservation interface {
	// CaptureInfo tells the caller who am I.
	CaptureInfo() *CaptureInfo

	// GetChangefeed queries one changefeed or all changefeeds.
	GetChangefeeds(...model.ChangeFeedID) ([]*ChangefeedInfo, error)

	// GetCaptures queries one capture or all captures.
	GetCaptures(...string) ([]*CaptureInfo, error)

	// Heartbeat tells the metadata storage I'm still alive.
	Heartbeat(context.Context) error

	// TakeControl blocks until becomes controller or gets canceled.
	TakeControl() (ControllerObservation, error)

	// Advance advances some changefeed progresses.
	Advance(cfs []*ChangefeedInfo, progresses []*ChangefeedProgress) error

	// Fetch the latest changefeed owner list from metadata.
	RefreshOwners() <-chan []ScheduledOwner

	// When an owner exits, inform the metadata storage.
	PostOwnerRemoved(cf *ChangefeedInfo) error

	// Fetch the latest changefeed processors from metadata.
	RefreshProcessors() <-chan []ScheduledProcessor

	// When a processor exits, inform the metadata storage.
	PostProcessorRemoved(cf *ChangefeedInfo) error
}

// ControllerObservation is for observing and updating meta by Controller.
//
// All intrefaces are thread-safe and shares one same Context.
type ControllerObservation interface {
	// CreateChangefeed creates a changefeed.
	CreateChangefeed(cf *ChangefeedInfo, up *UpstreamInfo) error

	// RemoveChangefeed removes a changefeed, will auto stop owner and processors.
	RemoveChangefeed(cf *ChangefeedInfo) error

	// Fetch the latest capture list in the TiCDC cluster.
	RefreshCaptures() <-chan []*CaptureInfo

	// Schedule a changefeed owner to a given target.
	// Notes:
	//   * the target capture can fetch the event by `RefreshOwners`.
	//   * select `done` to wait the old owner in removing to be resolved.
	//   * target state can only be `SchedLaunched` or `SchedRemoving`.
	SetOwner(cf *ChangefeedInfo, target ScheduledOwner) (done <-chan struct{}, _ error)

	// Schedule some captures as workers to a given changefeed.
	// Notes:
	//   * target captures can fetch the event by `RefreshProcessors`.
	//   * select `done` to wait all processors in removing to be resolved.
	//   * target state can only be `SchedLaunched` or `SchedRemoving`.
	SetProcessors(cf *ChangefeedInfo, workers []ScheduledProcessor) (done <-chan struct{}, _ error)

	// Get a snapshot of all changefeeds current schedule.
	GetChangefeedSchedule() ([]ChangefeedSchedule, []*CaptureInfo, error)
}

// OwnerObservation is for observing and updating running status of a changefeed.
//
// All intrefaces are thread-safe and shares one same Context.
type OwnerObservation interface {
	ChangefeedInfo() *ChangefeedInfo

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
	RefreshProcessors() <-chan []ScheduledProcessor
}
