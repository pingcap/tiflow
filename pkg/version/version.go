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

package version

import (
	"fmt"

	"github.com/coreos/go-semver/semver"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// Version information.
var (
	ReleaseVersion = "None"
	BuildTS        = "None"
	GitHash        = "None"
	GitBranch      = "None"
	GoVersion      = "None"
)

// ReleaseSemver returns a valid Semantic Versions or an empty if the
// ReleaseVersion is not set at compile time.
func ReleaseSemver() string {
	s := removeVAndHash(ReleaseVersion)
	v, err := semver.NewVersion(s)
	if err != nil {
		return ""
	}
	return v.String()
}

// LogVersionInfo prints the CDC version information.
func LogVersionInfo() {
	log.Info("Welcome to Change Data Capture (CDC)",
		zap.String("release-version", ReleaseVersion),
		zap.String("git-hash", GitHash),
		zap.String("git-branch", GitBranch),
		zap.String("utc-build-time", BuildTS),
		zap.String("go-version", GoVersion),
	)
}

// GetRawInfo returns basic version information string.
func GetRawInfo() string {
	var info string
	info += fmt.Sprintf("Release Version: %s\n", ReleaseVersion)
	info += fmt.Sprintf("Git Commit Hash: %s\n", GitHash)
	info += fmt.Sprintf("Git Branch: %s\n", GitBranch)
	info += fmt.Sprintf("UTC Build Time: %s\n", BuildTS)
	info += fmt.Sprintf("Go Version: %s\n", GoVersion)
	return info
}
