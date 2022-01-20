// Copyright 2020 PingCAP, Inc.
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

package model

import (
	"encoding/json"
	"math"
	"regexp"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/cyclic/mark"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/version"
	"go.uber.org/zap"
)

// SortEngine is the sorter engine
type SortEngine = string

// sort engines
const (
	SortInMemory SortEngine = "memory"
	SortInFile   SortEngine = "file"
	SortUnified  SortEngine = "unified"
)

// FeedState represents the running state of a changefeed
type FeedState string

// All FeedStates
const (
	StateNormal   FeedState = "normal"
	StateError    FeedState = "error"
	StateFailed   FeedState = "failed"
	StateStopped  FeedState = "stopped"
	StateRemoved  FeedState = "removed"
	StateFinished FeedState = "finished"
)

// ToInt return a int for each `FeedState`, only use this for metrics.
func (s FeedState) ToInt() int {
	switch s {
	case StateNormal:
		return 0
	case StateError:
		return 1
	case StateFailed:
		return 2
	case StateStopped:
		return 3
	case StateFinished:
		return 4
	case StateRemoved:
		return 5
	}
	// -1 for unknown feed state
	return -1
}

// ChangeFeedInfo describes the detail of a ChangeFeed
type ChangeFeedInfo struct {
	SinkURI    string            `json:"sink-uri"`
	Opts       map[string]string `json:"opts"`
	CreateTime time.Time         `json:"create-time"`
	// Start sync at this commit ts if `StartTs` is specify or using the CreateTime of changefeed.
	StartTs uint64 `json:"start-ts"`
	// The ChangeFeed will exits until sync to timestamp TargetTs
	TargetTs uint64 `json:"target-ts"`
	// used for admin job notification, trigger watch event in capture
	AdminJobType AdminJobType `json:"admin-job-type"`
	Engine       SortEngine   `json:"sort-engine"`
	// SortDir is deprecated
	// it cannot be set by user in changefeed level, any assignment to it should be ignored.
	// but can be fetched for backward compatibility
	SortDir string `json:"sort-dir"`

	Config *config.ReplicaConfig `json:"config"`
	State  FeedState             `json:"state"`
	Error  *RunningError         `json:"error"`

	SyncPointEnabled  bool          `json:"sync-point-enabled"`
	SyncPointInterval time.Duration `json:"sync-point-interval"`
	CreatorVersion    string        `json:"creator-version"`
}

const changeFeedIDMaxLen = 128

var changeFeedIDRe = regexp.MustCompile(`^[a-zA-Z0-9]+(-[a-zA-Z0-9]+)*$`)

// ValidateChangefeedID returns true if the changefeed ID matches
// the pattern "^[a-zA-Z0-9]+(\-[a-zA-Z0-9]+)*$", length no more than "changeFeedIDMaxLen", eg, "simple-changefeed-task".
func ValidateChangefeedID(changefeedID string) error {
	if !changeFeedIDRe.MatchString(changefeedID) || len(changefeedID) > changeFeedIDMaxLen {
		return cerror.ErrInvalidChangefeedID.GenWithStackByArgs(changeFeedIDMaxLen)
	}
	return nil
}

// String implements fmt.Stringer interface, but hide some sensitive information
func (info *ChangeFeedInfo) String() (str string) {
	var err error
	str, err = info.Marshal()
	if err != nil {
		log.Error("failed to marshal changefeed info", zap.Error(err))
		return
	}
	clone := new(ChangeFeedInfo)
	err = clone.Unmarshal([]byte(str))
	if err != nil {
		log.Error("failed to unmarshal changefeed info", zap.Error(err))
		return
	}
	clone.SinkURI = "***"
	str, err = clone.Marshal()
	if err != nil {
		log.Error("failed to marshal changefeed info", zap.Error(err))
	}
	return
}

// GetStartTs returns StartTs if it's  specified or using the CreateTime of changefeed.
func (info *ChangeFeedInfo) GetStartTs() uint64 {
	if info.StartTs > 0 {
		return info.StartTs
	}

	return oracle.EncodeTSO(info.CreateTime.Unix() * 1000)
}

// GetCheckpointTs returns CheckpointTs if it's specified in ChangeFeedStatus, otherwise StartTs is returned.
func (info *ChangeFeedInfo) GetCheckpointTs(status *ChangeFeedStatus) uint64 {
	if status != nil {
		return status.CheckpointTs
	}
	return info.GetStartTs()
}

// GetTargetTs returns TargetTs if it's specified, otherwise MaxUint64 is returned.
func (info *ChangeFeedInfo) GetTargetTs() uint64 {
	if info.TargetTs > 0 {
		return info.TargetTs
	}
	return uint64(math.MaxUint64)
}

// Marshal returns the json marshal format of a ChangeFeedInfo
func (info *ChangeFeedInfo) Marshal() (string, error) {
	data, err := json.Marshal(info)
	return string(data), cerror.WrapError(cerror.ErrMarshalFailed, err)
}

// Unmarshal unmarshals into *ChangeFeedInfo from json marshal byte slice
func (info *ChangeFeedInfo) Unmarshal(data []byte) error {
	err := json.Unmarshal(data, &info)
	if err != nil {
		return errors.Annotatef(
			cerror.WrapError(cerror.ErrUnmarshalFailed, err), "Unmarshal data: %v", data)
	}
	// TODO(neil) find a better way to let sink know cyclic is enabled.
	if info.Config != nil && info.Config.Cyclic.IsEnabled() {
		cyclicCfg, err := info.Config.Cyclic.Marshal()
		if err != nil {
			return errors.Annotatef(
				cerror.WrapError(cerror.ErrMarshalFailed, err), "Marshal data: %v", data)
		}
		info.Opts[mark.OptCyclicConfig] = cyclicCfg
	}
	return nil
}

// Clone returns a cloned ChangeFeedInfo
func (info *ChangeFeedInfo) Clone() (*ChangeFeedInfo, error) {
	s, err := info.Marshal()
	if err != nil {
		return nil, err
	}
	cloned := new(ChangeFeedInfo)
	err = cloned.Unmarshal([]byte(s))
	return cloned, err
}

// VerifyAndComplete verifies changefeed info and may fill in some fields.
// If a required field is not provided, return an error.
// If some necessary filed is missing but can use a default value, fill in it.
func (info *ChangeFeedInfo) VerifyAndComplete() error {
	defaultConfig := config.GetDefaultReplicaConfig()
	if info.Engine == "" {
		info.Engine = SortUnified
	}
	if info.Config.Filter == nil {
		info.Config.Filter = defaultConfig.Filter
	}
	if info.Config.Mounter == nil {
		info.Config.Mounter = defaultConfig.Mounter
	}
	if info.Config.Sink == nil {
		info.Config.Sink = defaultConfig.Sink
	}
	if info.Config.Cyclic == nil {
		info.Config.Cyclic = defaultConfig.Cyclic
	}
	if info.Config.Scheduler == nil {
		info.Config.Scheduler = defaultConfig.Scheduler
	}
	return nil
}

// FixIncompatible fixes incompatible changefeed meta info.
func (info *ChangeFeedInfo) FixIncompatible() {
	creatorVersionGate := version.NewCreatorVersionGate(info.CreatorVersion)
	if creatorVersionGate.ChangefeedStateFromAdminJob() {
		log.Info("Start fixing incompatible changefeed state", zap.Any("changefeed", info))
		info.fixState()
		log.Info("Fix incompatibility changefeed state completed", zap.Any("changefeed", info))
	}
}

// fixState attempts to fix state loss from upgrading the old owner to the new owner.
func (info *ChangeFeedInfo) fixState() {
	// Notice: In the old owner we used AdminJobType field to determine if the task was paused or not,
	// we need to handle this field in the new owner.
	// Otherwise, we will see that the old version of the task is paused and then upgraded,
	// and the task is automatically resumed after the upgrade.
	state := info.State
	// Upgrading from an old owner, we need to deal with cases where the state is normal,
	// but actually contains errors and does not match the admin job type.
	if state == StateNormal {
		switch info.AdminJobType {
		// This corresponds to the case of failure or error.
		case AdminNone, AdminResume:
			if info.Error != nil {
				if cerror.ChangefeedFastFailErrorCode(errors.RFCErrorCode(info.Error.Code)) {
					state = StateFailed
				} else {
					state = StateError
				}
			}
		case AdminStop:
			state = StateStopped
		case AdminFinish:
			state = StateFinished
		case AdminRemove:
			state = StateRemoved
		}
	}

	if state != info.State {
		log.Info("handle old owner inconsistent state",
			zap.String("old state", string(info.State)),
			zap.String("admin job type", info.AdminJobType.String()),
			zap.String("new state", string(state)))
		info.State = state
	}
}

// HasFastFailError returns true if the error in changefeed is fast-fail
func (info *ChangeFeedInfo) HasFastFailError() bool {
	if info.Error == nil {
		return false
	}
	return cerror.ChangefeedFastFailErrorCode(errors.RFCErrorCode(info.Error.Code))
}
