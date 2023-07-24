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

// -------------------- About owner scheduling -------------------- //
// 1. ControllerObservation.LaunchOwner puts a changefeed owner on a given capture;
// 2. Controller calls ControllerObservation.StopOwner to stop a owner;
// 3. Capture fetches owner launch/stop events with CaptureObservation.RefreshAliveOwners;
// 4. Capture calls Capture.PostOwnerStopped when the owner exits;
// 5. After controller confirms the old owner exits, it can re-reschedule it.
//
// So there is a state for a changefeed owner on metadata storage:
//   launched -> stopping -> stoped.
// ---------------------------------------------------------------- //

// ---------- About changefeed worker captures scheduling ---------- //
// 1. ControllerObservation.LaunchWorkers attaches some captures to a changefeed;
// 2. Owner calls OwnerObservation.RefreshProcessors to know workers are created;
// 3. Capture fetches worker launch/stop events with CaptureObservation.RefreshProcessors;
// 4. How to rolling-update a changefeed with only one worker capture:
//    * controller needs only to attach more captures to the changefeed;
//    * it's owner's responsibility to evict tables between captures.
// 5. What if owner knows workers are created before captures?
//    * table scheduling should be robust enough.
//
// So there is a state for a changefeed worker on metadata storage:
//   launched -> stopping -> stoped.
// ---------------------------------------------------------------- //

// ---------------- About keep-alive and heartbeat ---------------- //
// 1. Capture updates heartbeats to metadata by calling CaptureObservation.Heartbeat,
//    with a given timeout, for example, 1s;
// 2. On a capture, controller, owners and processors share one same Context, which is
//    associated with deadline 10s. CaptureObservation.Heartbeat will refresh the deadline.
// 3. Controller is binded with a lease (10+1+1)s, for deadline, heartbeat time-elapsed
//    and network clock skew.
// 4. Controller needs to consider re-scheduling owners and processors from a capture,
//    if the capture has been partitioned with metadata storage more than lease+5s;
// ---------------------------------------------------------------- //

// SchedState is the type of state to schedule owners and processors.
type SchedState int

const (
	// SchedLaunched means the owner or processor is launched.
	SchedLaunched SchedState = iota
	// SchedStopping means the owner or processor is in stopping.
	SchedStopping
	// SchedStopped means the owner or processor is stopped.
	SchedStopped
)

// ScheduledOwner is for owner and processor scheduling.
type ScheduledOwner struct {
	*ChangefeedInfo
	ChangefeedProgress
	state SchedState
}

// ScheduledProcessor is for owner and processor scheduling.
type ScheduledProcessor struct {
	*ChangefeedInfo
	ChangefeedProgress
	state SchedState
}

// CaptureObservation is for observing and updating metadata on a CAPTURE instance.
//
// All intrefaces are thread-safe and shares one same Context.
type CaptureObservation interface {
	// SelfCaptureInfo tells the caller who am I.
	SelfCaptureInfo() *CaptureInfo

	// GetChangefeed queries a changefeed.
	GetChangefeed(cf model.ChangeFeedID) (*ChangefeedInfo, error)

	// Heartbeat tells the metadata storage I'm still alive.
	Heartbeat(context.Context) error

	// TakeControl blocks until becomes controller or gets canceled.
	TakeControl() ControllerObservation

	// Advance advances some changefeed progresses.
	Advance(cfs []*ChangefeedInfo, progresses []*ChangefeedProgress) error

	// Fetch the latest alive changefeed owner list from metadata.
	//
	// owners in stopping state won't be in the list.
	RefreshOwners() <-chan []ScheduledOwner

	// When an owner exits, inform the metadata storage.
	PostOwnerStopped(cf *ChangefeedInfo) error

	// Fetch the latest alive changefeed workers from metadata.
	//
	// workers in stopping state won't be in the list.
	RefreshProcessors() <-chan []ScheduledProcessor

	// When a worker exits, inform the metadata storage.
	PostProcessorStopped(cf *ChangefeedInfo) error
}

// ControllerObservation is for observing and updating meta by Controller.
//
// All intrefaces are thread-safe and shares one same Context.
type ControllerObservation interface {
	// CreateChangefeed creates a changefeed.
	CreateChangefeed(cf *ChangefeedInfo, up *UpstreamInfo) error

	// RemoveChangefeed removes a changefeed.
	RemoveChangefeed(cf *ChangefeedInfo) error

	// PauseChangefeed pauses a changefeed.
	PauseChangefeed(cf *ChangefeedInfo) error

	// ResumeChangefeed resumes a changefeed.
	ResumeChangefeed(cf *ChangefeedInfo) error

	// UpdateChangefeed updates changefeed metadata, must be called on a stopped one.
	UpdateChangefeed(cf *ChangefeedInfo) error

	// Fetch the latest capture list in the TiCDC cluster.
	RefreshCaptures() <-chan []*CaptureInfo

	// Schedule a changefeed owner to the given target.
	//
	// The target capture can fetch the event by `RefreshOwners`.
	LaunchOwner(cf *ChangefeedInfo, target *CaptureInfo) error

	// Stop a changefeed owner.
	StopOwner(cf *ChangefeedInfo) (stopped chan struct{}, _ error)

	// Schedule some captures as workers to a given changefeed.
	//
	// Target captures can fetch the event by RefreshWorkers.
	LaunchWorkers(cf *ChangefeedInfo, workers []*CaptureInfo) error

	// Stop some changefeed workers.
	StopWorkers(cf *ChangefeedInfo, workers []*CaptureInfo) (stopped chan struct{}, _ error)

	// Combine LaunchOwner and LaunchWorkers into one transaction.
	LaunchOwnerAndWorkers(cf *ChangefeedInfo, owner *CaptureInfo, workers []*CaptureInfo) error

	// Get a snapshot of all changefeeds current scheduling state.
	GetSnapshot() ([]ScheduledOwner, []ScheduledProcessor, error)
}

// OwnerObservation is for observing and updating running status of a changefeed.
//
// All intrefaces are thread-safe and shares one same Context.
type OwnerObservation interface {
	// set the changefeed to state finished.
	SetChangefeedFinished() error

	// Set the changefeed to state failed.
	SetChangefeedFailed(err model.RunningError) error

	// Set the changefeed to state warning.
	SetChangefeedWarning(warn model.RunningError) error

	// Set the changefeed to state pending.
	SetChangefeedPending(cf model.ChangeFeedID) error

	// Fetch the latest capture list to launch processors.
	RefreshProcessors(cf *ChangefeedInfo) <-chan []ScheduledProcessor
}
