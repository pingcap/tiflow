// Copyright 2021 PingCAP, Inc.
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

package utils

import (
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbutil"
	"github.com/pingcap/tiflow/dm/pkg/terror"
)

const (
	StartTimeFormat  = "2006-01-02 15:04:05"
	StartTimeFormat2 = "2006-01-02T15:04:05"
)

// ParseTimeZone parse the time zone location by name or offset
//
// NOTE: we don't support the "SYSTEM" or "Local" time_zone.
func ParseTimeZone(s string) (*time.Location, error) {
	if s == "SYSTEM" || s == "Local" {
		return nil, terror.ErrConfigInvalidTimezone.New("'SYSTEM' or 'Local' time_zone is not supported")
	}

	loc, err := time.LoadLocation(s)
	if err == nil {
		return loc, nil
	}

	// The value can be given as a string indicating an offset from UTC, such as '+10:00' or '-6:00'.
	// The time zone's value should in [-12:59,+14:00].
	// See: https://dev.mysql.com/doc/refman/8.0/en/time-zone-support.html#time-zone-variables
	if strings.HasPrefix(s, "+") || strings.HasPrefix(s, "-") {
		d, _, err := types.ParseDuration(nil, s[1:], 0)
		if err == nil {
			if s[0] == '-' {
				if d.Duration > 12*time.Hour+59*time.Minute {
					return nil, terror.ErrConfigInvalidTimezone.Generate(s)
				}
			} else {
				if d.Duration > 14*time.Hour {
					return nil, terror.ErrConfigInvalidTimezone.Generate(s)
				}
			}

			ofst := int(d.Duration / time.Second)
			if s[0] == '-' {
				ofst = -ofst
			}
			name := dbutil.FormatTimeZoneOffset(d.Duration)
			return time.FixedZone(name, ofst), nil
		}
	}

	return nil, terror.ErrConfigInvalidTimezone.Generate(s)
}

// ParseStartTime parses start-time of task-start and validation-start in local location.
func ParseStartTime(timeStr string) (time.Time, error) {
	return ParseStartTimeInLoc(timeStr, time.Local)
}

// ParseStartTimeInLoc parses start-time of task-start and validation-start.
func ParseStartTimeInLoc(timeStr string, loc *time.Location) (time.Time, error) {
	t, err := time.ParseInLocation(StartTimeFormat, timeStr, loc)
	if err != nil {
		return time.ParseInLocation(StartTimeFormat2, timeStr, loc)
	}
	return t, nil
}
