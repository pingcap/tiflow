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

package util

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetTimezoneFromZonefile(t *testing.T) {
	testCases := []struct {
		hasErr   bool
		zonefile string
		name     string
	}{
		{true, "", ""},
		{false, "UTC", "UTC"},
		{false, "/usr/share/zoneinfo/UTC", "UTC"},
		{false, "/usr/share/zoneinfo/Etc/UTC", "Etc/UTC"},
		{false, "/usr/share/zoneinfo/Asia/Shanghai", "Asia/Shanghai"},
	}
	for _, tc := range testCases {
		loc, err := GetTimezoneFromZonefile(tc.zonefile)
		if tc.hasErr {
			require.NotNil(t, err)
		} else {
			require.Nil(t, err)
			require.Equal(t, tc.name, loc.String())
		}
	}
}
