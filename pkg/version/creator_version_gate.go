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

package version

import (
	"github.com/coreos/go-semver/semver"
)

// CreatorVersionGate determines the introduced version and compatibility
// of some features based on the creator's version value.
type CreatorVersionGate struct {
	version string
}

// changefeedStateFromAdminJobVersions specifies the version before
// which we use the admin job type to control the state of the changefeed.
var changefeedStateFromAdminJobVersions = []semver.Version{
	// Introduced in https://github.com/pingcap/ticdc/pull/3014.
	*semver.New("4.0.16"),
	// Introduced in https://github.com/pingcap/ticdc/pull/2946.
	*semver.New("5.0.6"),
}

// NewCreatorVersionGate creates the creator version gate.
func NewCreatorVersionGate(version string) *CreatorVersionGate {
	return &CreatorVersionGate{
		version: version,
	}
}

// ChangefeedStateFromAdminJob determines if admin job is the state
// of changefeed based on the version of the creator.
func (f *CreatorVersionGate) ChangefeedStateFromAdminJob() bool {
	// Introduced in https://github.com/pingcap/ticdc/pull/1341.
	// The changefeed before it was introduced was using the old owner.
	if f.version == "" {
		return true
	}

	creatorVersion := semver.New(removeVAndHash(f.version))
	for _, version := range changefeedStateFromAdminJobVersions {
		// NOTICE: To compare against the same major version.
		if creatorVersion.Major == version.Major &&
			creatorVersion.LessThan(version) {
			return true
		}
	}

	return false
}
