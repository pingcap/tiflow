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
// 1. ControllerObservation.LaunchOwner puts a changefeed onwer on a given capture;
// 2. Controller can call ControllerObservation.StopOwner to stop a owner;
// 3. Capture fetches owner launch/stop events with CaptureObservation.RefreshAliveOwners;
// 4. Owner calls OwnerObservation.PostStopped when it exits;
// 5. Controller can call ControllerObservation.LaunchOwner to re-scheduling it.
//
// So there is a state for changefeed owner on metadata storage:
//   stopped -> launched -> stopping -> stoped.
// ---------------------------------------------------------------- //

// ---------- About changefeed worker captures scheduling ---------- //
// 1. ControllerObservation.AttachWorkers attaches some captures to a changefeed;
// 2. Owner calls OwnerObservation.RefreshWorkers to know how to launch or stop processors;
// 3. Capture receives processor launch/stop messages with P2P mechanism;
// 4. Capture doesn't care anything about changefeed worker captures scheduling.
// 5. How to rolling-update a changefeed with only one worker capture:
//    * controller needs only to attach more captures to the changefeed;
//    * it's owner's responsibility to evict tables between captures;
// ---------------------------------------------------------------- //

// ---------------- About keep-alive and heartbeat ---------------- //
// 1. Capture updates heartbeats to metadata by calling CaptureObservation.Heartbeat,
//    with a given timeout, for example, 1s;
// 2. On a capture, controller and owners share one same Context, which is associated
//    with deadline 10s. CaptureObservation.Heartbeat will refresh the deadline.
// 3. Controller is binded with a lease (10+1+1)s, for deadline, heartbeat time-elapsed
//    and network clock skew.
// 4. Controller needs to consider re-scheduling owners from a capture, if the capture
//    has been partitioned with metadata storage more than lease+5s;
// 5. Controller needs to consider re-scheduling changefeed worker captures, if a
//    changefeed worker capture has been missed more than lease+5s, and changefeed lag
//    is larger than lease+5s.
// ---------------------------------------------------------------- //

// CaptureObservation is for observing and updating metadata on a CAPTURE instance.
//
// All intrefaces are thread-safe and shares one same Context.
type CaptureObservation interface {
	// SelfCaptureInfo tells the caller who am I.
	SelfCaptureInfo() *CaptureInfo

	// Heartbeat tells the metadata storage I'm still alive.
	Heartbeat(context.Context) error

	// TakeControl blocks until becomes controller or gets canceled.
	TakeControl() ControllerObservation

	// Advance is like OwnerObservation.Advance but handles many changefeeds.
	//
	// It should returns an InvalidOwner error if any changefeed ownership is revoked.
	Advance(cfs []*ChangefeedInfo, progresses []*ChangefeedProgress) error

	// Fetch the latest alive changefeed owner list from metadata.
	RefreshAliveOwners() <-chan []*ChangefeedInfo
}

// ControllerObservation is for observing and updating meta by Controller.
//
// All intrefaces are thread-safe and shares one same Context.
type ControllerObservation interface {
	// CreateChangefeed creates a changefeed.
	CreateChangefeed(cf *ChangefeedInfo, up *UpstreamInfo) error

	// UpdateChangefeed updates changefeed metadata, must be called on a stopped one.
	UpdateChangefeed(cf *ChangefeedInfo) error

	// Stop a changefeed.
	SetChangefeedStopped(cf *ChangefeedInfo) error

	// Fetch the latest capture list in the TiCDC cluster.
	RefreshCaptures() <-chan []*CaptureInfo

	// Schedule a changefeed owner to the given target.
	//
	// The target capture can fetch the event by `RefreshOwners`.
	LaunchOwner(cf *ChangefeedInfo, target *CaptureInfo) error

	// Stop a changefeed owner.
	StopOwner(cf *ChangefeedInfo) error

	// Schedule some captures as workers to a given changefeed.
	//
	// The target changefeed can fetch the event by xxx.
	AttachWorkers(cf *ChangefeedInfo, workers []*CaptureInfo) error
}

// OwnerObservation is for observing and updating running status of a changefeed.
//
// All intrefaces are thread-safe and shares one same Context.
//
// On a capture all owners shares one same OwnerObservation, because heartbeats
// to metadata storage is based on CAPTURE instead of CHANGEFEED.
type OwnerObservation interface {
	// Advance advances a changefeed progress.
	Advance(cf *ChangefeedInfo, progress *ChangefeedProgress) error

	// set the changefeed to state finished.
	SetChangefeedFinished(cf *ChangefeedInfo) error

	// Set the changefeed to state failed.
	SetChangefeedFailed(cf *ChangefeedInfo, err model.RunningError) error

	// Set the changefeed to state warning.
	SetChangefeedWarning(cf *ChangefeedInfo, warn model.RunningError) error

	// Set the changefeed to state pending.
	SetChangefeedPending(cf model.ChangeFeedID) error

	// Fetch the latest capture list to launch processors.
	RefreshWorkers(cf *ChangefeedInfo) <-chan []*CaptureInfo

	// When an owner exits, inform the metadata storage.
	PostStopped(cf *ChangefeedInfo) error
}
