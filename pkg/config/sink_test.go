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

package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidate(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		protocol       string
		enableOldValue bool
		expectedErr    string
	}{
		{
			protocol:       "default",
			enableOldValue: false,
			expectedErr:    "",
		},
		{
			protocol:       "default",
			enableOldValue: true,
			expectedErr:    "",
		},
		{
			protocol:       "canal-json",
			enableOldValue: false,
			expectedErr:    ".*canal-json protocol requires old value to be enabled.*",
		},
		{
			protocol:       "canal-json",
			enableOldValue: true,
			expectedErr:    "",
		},
		{
			protocol:       "canal",
			enableOldValue: false,
			expectedErr:    ".*canal protocol requires old value to be enabled.*",
		},
		{
			protocol:       "canal",
			enableOldValue: true,
			expectedErr:    "",
		},
		{
			protocol:       "maxwell",
			enableOldValue: false,
			expectedErr:    ".*maxwell protocol requires old value to be enabled.*",
		},
		{
			protocol:       "maxwell",
			enableOldValue: true,
			expectedErr:    "",
		},
	}

	for _, tc := range testCases {
		cfg := SinkConfig{
			Protocol: tc.protocol,
		}
		if tc.expectedErr == "" {
			require.Nil(t, cfg.validate(tc.enableOldValue))
		} else {
			require.Regexp(t, tc.expectedErr, cfg.validate(tc.enableOldValue))
		}
	}
}
