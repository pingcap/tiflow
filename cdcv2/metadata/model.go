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
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"go.uber.org/zap"
)

// ChangefeedUUID is the unique identifier of a changefeed.
type ChangefeedUUID = uint64

// ChangefeedIdent identifies a changefeed.
type ChangefeedIdent struct {
	// UUID is generated internally by TiCDC to distinguish between changefeeds with the same ID.
	// Note that it can't be specified by the user.
	UUID ChangefeedUUID `gorm:"column:uuid;type:bigint(20) unsigned;primaryKey" json:"uuid"`

	// Namespace and ID pair is unique in one ticdc cluster. And in the current implementation,
	// Namespace can only be set to `default`.
	Namespace string `gorm:"column:namespace;type:varchar(128);not null;uniqueIndex:namespace,priority:1" json:"namespace"`
	ID        string `gorm:"column:id;type:varchar(128);not null;uniqueIndex:namespace,priority:2" json:"id"`
}

// ToChangefeedID converts ChangefeedUUID to model.ChangeFeedID.
func (c ChangefeedIdent) ToChangefeedID() model.ChangeFeedID {
	return model.ChangeFeedID{
		Namespace: c.Namespace,
		ID:        c.ID,
	}
}

// String implements fmt.Stringer interface
func (c ChangefeedIdent) String() string {
	return fmt.Sprintf("%d(%s/%s)", c.UUID, c.Namespace, c.ID)
}

// Compare compares two ChangefeedIDWithEpoch base on their string representation.
func (c *ChangefeedIdent) Compare(other ChangefeedIdent) int {
	return strings.Compare(c.String(), other.String())
}

// ChangefeedInfo is a minimal info collection to describe a changefeed.
type ChangefeedInfo struct {
	ChangefeedIdent

	UpstreamID uint64 `gorm:"column:upstream_id;type:bigint(20) unsigned;not null;index:upstream_id,priority:1" json:"upstream_id"`
	SinkURI    string `gorm:"column:sink_uri;type:text;not null" json:"sink_uri"`
	StartTs    uint64 `gorm:"column:start_ts;type:bigint(20) unsigned;not null" json:"start_ts"`
	TargetTs   uint64 `gorm:"column:target_ts;type:bigint(20) unsigned;not null" json:"target_ts"`
	// Note that pointer type is used here for compatibility with the old model, and config should never be nil in practice.
	Config *config.ReplicaConfig `gorm:"column:config;type:longtext;not null" json:"config"`
}

// ChangefeedState is the status of a changefeed.
type ChangefeedState struct {
	ChangefeedUUID ChangefeedUUID      `gorm:"column:changefeed_uuid;type:bigint(20) unsigned;primaryKey" json:"changefeed_uuid"`
	State          model.FeedState     `gorm:"column:state;type:text;not null" json:"state"`
	Warning        *model.RunningError `gorm:"column:warning;type:text" json:"warning"`
	Error          *model.RunningError `gorm:"column:error;type:text" json:"error"`
}

// ChangefeedProgress is for changefeed progress. Use ChangeFeedStatus to maintain compatibility with older versions of the code.
type ChangefeedProgress model.ChangeFeedStatus

// Value implements the driver.Valuer interface.
func (cp ChangefeedProgress) Value() (driver.Value, error) {
	return json.Marshal(cp)
}

// Scan implements the sql.Scanner interface.
func (cp *ChangefeedProgress) Scan(value interface{}) error {
	b, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}

	return json.Unmarshal(b, cp)
}

// CaptureProgress stores the progress of all ChangeFeeds on single capture.
type CaptureProgress map[ChangefeedUUID]ChangefeedProgress

// Value implements the driver.Valuer interface.
func (cp CaptureProgress) Value() (driver.Value, error) {
	return json.Marshal(cp)
}

// Scan implements the sql.Scanner interface.
func (cp *CaptureProgress) Scan(value interface{}) error {
	b, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}

	return json.Unmarshal(b, cp)
}

// SchedState is the type of state to schedule owners and processors.
type SchedState int

const (
	// SchedInvalid should never be used.
	SchedInvalid SchedState = SchedState(0)
	// SchedRemoved means the owner or processor is removed.
	SchedRemoved SchedState = SchedState(1)
	// SchedLaunched means the owner or processor is launched.
	SchedLaunched SchedState = SchedState(2)
	// SchedRemoving means the owner or processor is in removing.
	SchedRemoving SchedState = SchedState(3)
)

// String implements the fmt.Stringer interface.
func (s SchedState) String() string {
	return s.toString()
}

func (s SchedState) toString() string {
	switch s {
	case SchedLaunched:
		return "Launched"
	case SchedRemoving:
		return "Removing"
	case SchedRemoved:
		return "Removed"
	default:
		return "unreachable"
	}
}

// nolint
func (s *SchedState) fromString(str string) error {
	switch str {
	case "Launched":
		*s = SchedLaunched
	case "Removing":
		*s = SchedRemoving
	case "Removed":
		*s = SchedRemoved
	default:
		*s = SchedInvalid
		return errors.New("unreachable")
	}
	return nil
}

// Value implements the driver.Valuer interface.
func (s SchedState) Value() (driver.Value, error) {
	return []byte(s.toString()), nil
}

// Scan implements the sql.Scanner interface.
func (s *SchedState) Scan(value interface{}) error {
	b, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}

	return s.fromString(string(b))
}

// ScheduledChangefeed is for owner and processor schedule.
type ScheduledChangefeed struct {
	ChangefeedUUID ChangefeedUUID   `gorm:"column:changefeed_uuid;type:bigint(20) unsigned;primaryKey" json:"changefeed_uuid"`
	Owner          *model.CaptureID `gorm:"column:owner;type:varchar(128)" json:"owner"`
	OwnerState     SchedState       `gorm:"column:owner_state;type:text;not null" json:"owner_state"`
	// Processors is always equal to the owner in the current implementation.
	Processors *model.CaptureID `gorm:"column:processors;type:text" json:"processors"`
	// TaskPosition is used to initialize changefeed owner on the capture.
	TaskPosition ChangefeedProgress `gorm:"column:task_position;type:text;not null" json:"task_position"`
}

// CheckScheduleState checks whether the origin and target schedule state is valid.
func CheckScheduleState(origin ScheduledChangefeed, target ScheduledChangefeed) error {
	if origin.ChangefeedUUID != target.ChangefeedUUID {
		log.Panic("bad schedule: changefeed id not match",
			zap.Any("origin", origin), zap.Any("target", target))
	}
	if origin.OwnerState == SchedInvalid || target.OwnerState == SchedInvalid {
		return NewBadScheduleError(origin, target)
	}

	if origin.Owner != target.Owner {
		// NOTE: owner id can be changed only when the old owner is removed.
		if origin.OwnerState == SchedRemoved && target.OwnerState == SchedLaunched {
			return nil
		}
		return NewBadScheduleError(origin, target)
	}

	switch origin.OwnerState {
	case SchedRemoved:
		// case 1: SchedRemoved -> SchedLaunched, this means a changefeed owner is rescheduled
		// to the same capture.
		if target.OwnerState != SchedLaunched {
			return NewBadScheduleError(origin, target)
		}
	case SchedLaunched:
		// case 1: SchedLaunched -> SchedRemoving, this is the normal scenario.
		// case 2: SchedLaunched -> SchedRemoved, this is the capture offline scenario.
		return nil
	case SchedRemoving:
		if target.OwnerState != SchedRemoved {
			return NewBadScheduleError(origin, target)
		}
	}
	return nil
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
			unexist.OwnerState = SchedRemoved
			if err := CheckScheduleState(unexist, target[j]); err != nil {
				return nil, badSchedule()
			}
			diffs = append(diffs, target[j])
			j += 1
		} else if j == len(target) || sortedBy(origin[i], target[j]) < 0 {
			unexist := origin[i]
			unexist.OwnerState = SchedRemoved
			if err := CheckScheduleState(origin[i], unexist); err != nil {
				return nil, badSchedule()
			}
			diffs = append(diffs, unexist)
			i += 1
		} else {
			if origin[i].OwnerState != target[j].OwnerState {
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

func (s ScheduledChangefeed) toString() string {
	return fmt.Sprintf("%s.%s", *s.Owner, s.OwnerState.String())
}
