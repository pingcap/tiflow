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
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/chann"
	"github.com/pingcap/tiflow/pkg/config"
)

// ChangefeedState indicate states of a changefeed.
type ChangefeedState int

// Defined changefeed states.
const (
	StateNormal ChangefeedState = iota
	StateFailed
	StateStopped
	StateRemoved
	StateFinished
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

// CaptureObservation is for observing and updating metadata on a CAPTURE instance.
//
// All intrefaces are thread-safe.
type CaptureObservation interface {
	keepAlive
    
    // TryTakeControl tries to become controller.
    TryTakeControl() bool

	// Advance is like OwnerObservation.Advance but handles many changefeeds.
	Advance(cfs []*ChangefeedInfo, progresses []*ChangefeedProgress) Error

	// WatchOwners watches changefeed owners scheduled to the capture.
	WatchOwners() <-chan *ChangefeedInfo

	// WatchProcessors watches changefeed processors scheduled to the capture.
	WatchProcessors() <-chan *ChangefeedInfo
}

// ControllerObservation is for observing and updating meta by Controller.
//
// All intrefaces are thread-safe.
type ControllerObservation interface {
	keepAlive

	// CreateChangefeed creates a changefeed.
	CreateChangefeed(clusterID string, cf *ChangefeedInfo, up *UpstreamInfo) Error

	// UpdateChangefeed updates changefeed metadata, must be called on a stopped one.
	UpdateChangefeed(clusterID string, cf *ChangefeedInfo) Error
    
	SetChangefeedStopped(clusterID string, cf *ChangefeedInfo) Error

	// WatchCaptures watches captures in the TiCDC cluster.
	WatchCaptures() *chann.DrainableChann[*CaptureInfo]
}

// ChangefeedObservation is for observing and updating running status of a changefeed.
//
// All intrefaces are thread-safe.
type ChangefeedObservation interface {
	SelfChangefeedInfo() *CaptureInfo

	// Advance advances a changefeed progress.
	Advance(progress *ChangefeedProgress) Error

	SetChangefeedFailed(clusterID string, cf model.ChangeFeedID) Error
	SetChangefeedFinished(clusterID string, cf model.ChangeFeedID) Error

	ReportChangefeedError(clusterID string, cf model.ChangeFeedID) Error
	ReportChangefeedWarning(clusterID string, cf model.ChangeFeedID) Error

	// NOTE: if there are any errors or warnings, owner may want to stop
	// or restart some processors. We don't want to implemenet it with
	// an `AdminJob` in metadata storage. It can be implemented based on P2P
	// messages instead.
}

type keepAlive interface {
	// SelfCaptureInfo tells the caller who am I.
	SelfCaptureInfo() *CaptureInfo

	// Heartbeat tells the metadata storage I'm still alive.
	Heartbeat() Error
}
