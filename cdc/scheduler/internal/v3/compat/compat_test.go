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
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/stretchr/testify/require"
)

func TestCheckChangefeedEpochEnabled(t *testing.T) {
	t.Parallel()

	c := New(map[string]*model.CaptureInfo{})

	// Unknown capture always return false
	require.False(t, c.CheckChangefeedEpochEnabled("unknown"))

	// Add 1 supported capture.
	require.True(t, c.UpdateCaptureInfo(map[string]*model.CaptureInfo{
		"a": {Version: ChangefeedEpochMinVersion.String()},
	}))
	require.True(t, c.CheckChangefeedEpochEnabled("a"))
	// Check again.
	require.True(t, c.CheckChangefeedEpochEnabled("a"))

	// Add 1 supported capture again.
	require.True(t, c.UpdateCaptureInfo(map[string]*model.CaptureInfo{
		"a": {Version: ChangefeedEpochMinVersion.String()},
		"b": {Version: ChangefeedEpochMinVersion.String()},
	}))
	require.True(t, c.CheckChangefeedEpochEnabled("a"))
	require.True(t, c.CheckChangefeedEpochEnabled("b"))

	// Replace 1 unsupported capture.
	unsupported := *ChangefeedEpochMinVersion
	unsupported.Major--
	require.True(t, c.UpdateCaptureInfo(map[string]*model.CaptureInfo{
		"a": {Version: ChangefeedEpochMinVersion.String()},
		"c": {Version: unsupported.String()},
	}))
	require.True(t, c.CheckChangefeedEpochEnabled("a"))
	require.False(t, c.CheckChangefeedEpochEnabled("b"))
	require.False(t, c.CheckChangefeedEpochEnabled("c"))
}
