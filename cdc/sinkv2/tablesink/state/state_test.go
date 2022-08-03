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

package state

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStateString(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		state TableSinkState
		want  string
	}{
		{TableSinkStateUnknown, "Unknown"},
		{TableSinkSinking, "Sinking"},
		{TableSinkStopping, "Stopping"},
		{TableSinkStopped, "Stopped"},
	}

	for _, tc := range testCases {
		t.Run(tc.want, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, tc.state.String())
		})
	}
}
