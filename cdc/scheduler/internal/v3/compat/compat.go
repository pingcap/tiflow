// Copyright 2022 PingCAP, Inc.
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

package compat

import (
	"github.com/coreos/go-semver/semver"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/version"
)

// ChangefeedEpochMinVersion is the min version that enables changefeed epoch.
var ChangefeedEpochMinVersion = semver.New("6.5.2")

// Compat is a compatibility layer between span replication and table replication.
type Compat struct {
	captureInfo map[model.CaptureID]*model.CaptureInfo

	changefeedEpoch map[model.CaptureID]bool
}

// New returns a new Compat.
func New(
	captureInfo map[model.CaptureID]*model.CaptureInfo,
) *Compat {
	return &Compat{
		captureInfo:     captureInfo,
		changefeedEpoch: make(map[string]bool),
	}
}

// UpdateCaptureInfo update the latest alive capture info.
// Returns true if capture info has changed.
func (c *Compat) UpdateCaptureInfo(
	aliveCaptures map[model.CaptureID]*model.CaptureInfo,
) bool {
	if len(aliveCaptures) != len(c.captureInfo) {
		c.captureInfo = aliveCaptures
		c.changefeedEpoch = make(map[string]bool, len(aliveCaptures))
		return true
	}
	for id, alive := range aliveCaptures {
		info, ok := c.captureInfo[id]
		if !ok || info.Version != alive.Version {
			c.captureInfo = aliveCaptures
			c.changefeedEpoch = make(map[string]bool, len(aliveCaptures))
			return true
		}
	}
	return false
}

// CheckChangefeedEpochEnabled check if the changefeed enables epoch.
func (c *Compat) CheckChangefeedEpochEnabled(captureID model.CaptureID) bool {
	isEnabled, ok := c.changefeedEpoch[captureID]
	if ok {
		return isEnabled
	}

	captureInfo, ok := c.captureInfo[captureID]
	if !ok {
		return false
	}
	if len(captureInfo.Version) != 0 {
		captureVer := semver.New(version.SanitizeVersion(captureInfo.Version))
		isEnabled = captureVer.Compare(*ChangefeedEpochMinVersion) >= 0
	} else {
		isEnabled = false
	}
	c.changefeedEpoch[captureID] = isEnabled
	return isEnabled
}
