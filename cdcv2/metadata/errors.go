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

import "fmt"

// ScheduleError is for role state transformation.
type ScheduleError struct {
	Msg string
}

// Error implements error interface.
func (e ScheduleError) Error() string {
	return e.Msg
}

// NewScheduleError creates an error.
func NewScheduleError(msg string) ScheduleError {
	return ScheduleError{Msg: msg}
}

// NewBadScheduleError creates a schedule error with detail information.
func NewBadScheduleError(origin ScheduledChangefeed, target ScheduledChangefeed) ScheduleError {
	msg := fmt.Sprintf("bad schedule for %d: A.%s->B.%s",
		origin.ChangefeedUUID, origin.OwnerState.String(), target.OwnerState.String())
	return NewScheduleError(msg)
}
