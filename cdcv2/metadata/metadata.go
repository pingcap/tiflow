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
	"fmt"
	"strings"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/election"
)

// ChangefeedID identifies a changefeed.
type ChangefeedID struct {
	model.ChangeFeedID

	// Epoch can't be specified by users. It's used by TiCDC internally
	// to tell distinct changefeeds with a same ID.
	Epoch uint64

	// Combine of name and epoch, for comparing 2 ChangefeedInfos.
	Comparable string
}

// ChangefeedInfo is a minimal info collection to describe a changefeed.
type ChangefeedInfo struct {
	ID       model.ChangeFeedID
	SinkURI  string
	StartTs  uint64
	TargetTs uint64
	Config   *config.ReplicaConfig
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

// SchedState is the type of state to schedule owners and processors.
type SchedState int

const (
	// SchedRemoved means the owner or processor is removed.
	SchedRemoved SchedState = SchedState(0)
	// SchedLaunched means the owner or processor is launched.
	SchedLaunched SchedState = SchedState(1)
	// SchedRemoving means the owner or processor is in removing.
	SchedRemoving SchedState = SchedState(2)

	totalStates int = 3
)

// ScheduledChangefeed is for owner and processor schedule.
type ScheduledChangefeed struct {
	ChangefeedID ChangefeedID
	CaptureID    string
	State        SchedState

	// TaskPosition is used for creating owner and processors on captures.
	//
	// When controller specifies resources to a changefeed, it won't care about it.
	TaskPosition ChangefeedProgress
}

// ChangefeedSchedule is used to query changefeed schedule information.
type ChangefeedSchedule struct {
	Owner      ScheduledChangefeed
	Processors []ScheduledChangefeed
}

// Querier is used to query informations from metadata storage.
type Querier interface {
	// GetChangefeed queries some or all changefeeds.
	GetChangefeeds(...model.ChangeFeedID) ([]*ChangefeedInfo, []ChangefeedID, error)

	// GetCaptures queries some or all captures.
	GetCaptures(...string) ([]*CaptureInfo, error)
}

// CaptureObservation is for observing and updating metadata on a CAPTURE instance.
//
// All intrefaces are thread-safe and shares one same Context.
type CaptureObservation interface {
	election.Elector

	// CaptureInfo tells the caller who am I.
	Self() *CaptureInfo

	// Advance advances some changefeed progresses.
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
	CreateChangefeed(cf *ChangefeedInfo, up *UpstreamInfo) (ChangefeedID, error)

	// RemoveChangefeed removes a changefeed, will auto stop owner and processors.
	RemoveChangefeed(cf ChangefeedID) error

	// Fetch the latest capture list in the TiCDC cluster.
	RefreshCaptures() (captures []*CaptureInfo, changed bool)

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
	ScheduleSnapshot() ([]ChangefeedSchedule, []*CaptureInfo, error)
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

// CheckScheduleState checks whether role state transformation is valid or not.
func CheckScheduleState(origin ScheduledChangefeed, target ScheduledChangefeed) error {
	if (int(origin.State)+1)%totalStates == int(target.State) {
		if origin.State == SchedLaunched && origin.CaptureID != target.CaptureID {
			msg := fmt.Sprintf("bad schedule: A.%s->B.%s", origin.State.toString(), target.State.toString())
			return NewScheduleError(msg)
		}
		return nil
	}
	if origin.State == SchedRemoving && target.State == SchedLaunched && origin.CaptureID == target.CaptureID {
		// NOTE: removing can't be cancaled by design.
		msg := fmt.Sprintf("bad schedule: A.%s->A.%s", origin.State.toString(), target.State.toString())
		return NewScheduleError(msg)
	}
	msg := fmt.Sprintf("bad schedule: %s->%s", origin.State.toString(), target.State.toString())
	return NewScheduleError(msg)
}

// DiffScheduledChangefeeds gets difference between origin and target.
//
// Both origin and target should be sorted by the given rule.
func DiffScheduledChangefeeds(
	origin, target []ScheduledChangefeed,
	sortedBy func(a, b ScheduledChangefeed) int,
) ([]ScheduledChangefeed, error) {
	badSchedule := func() error {
		originStrs := make([]string, 0, len(origin))
		targetStrs := make([]string, 0, len(target))
		for _, s := range origin {
			originStrs = append(originStrs, s.toString())
		}
		for _, s := range target {
			targetStrs = append(targetStrs, s.toString())
		}
		msg := fmt.Sprintf("bad schedule: [%s]->[%s]", strings.Join(originStrs, ","), strings.Join(targetStrs, ","))
		return NewScheduleError(msg)
	}

	var diffs []ScheduledChangefeed
	if len(origin) <= len(target) {
		diffs = make([]ScheduledChangefeed, 0, len(target))
	} else {
		diffs = make([]ScheduledChangefeed, 0, len(origin))
	}

	for i, j := 0, 0; i < len(origin) && j < len(target); {
		if i == len(origin) || sortedBy(origin[i], target[j]) > 0 {
			unexist := target[j]
			unexist.State = SchedRemoved
			if err := CheckScheduleState(unexist, target[j]); err != nil {
				return nil, badSchedule()
			}
			diffs = append(diffs, target[j])
			j += 1
		} else if j == len(target) || sortedBy(origin[i], target[j]) < 0 {
			unexist := origin[i]
			unexist.State = SchedRemoved
			if err := CheckScheduleState(origin[i], unexist); err != nil {
				return nil, badSchedule()
			}
			diffs = append(diffs, unexist)
			i += 1
		} else {
			if origin[i].State != target[j].State {
				if err := CheckScheduleState(origin[i], target[j]); err != nil {
					return nil, badSchedule()
				}
				diffs = append(diffs, target[j])
			}
			i += 1
			j += 1
		}
	}
	return diffs, nil
}

func (s SchedState) toString() string {
	switch s {
	case SchedLaunched:
		return "Launched"
	case SchedRemoving:
		return "Removing"
	case SchedRemoved:
		return "Removed"
	}
	return "unreachable"
}

func (s ScheduledChangefeed) toString() string {
	return fmt.Sprintf("%s.%s", s.CaptureID, s.State.toString())
}
