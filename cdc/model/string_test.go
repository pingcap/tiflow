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

package model

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestHolderString(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		count    int
		expected string
	}{
		{1, "?"},
		{2, "?,?"},
		{10, "?,?,?,?,?,?,?,?,?,?"},
	}
	for _, tc := range testCases {
		s := HolderString(tc.count)
		require.Equal(t, tc.expected, s)
	}
	// test invalid input
	require.Panics(t, func() { HolderString(0) }, "strings.Builder.Grow: negative count")
	require.Panics(t, func() { HolderString(-1) }, "strings.Builder.Grow: negative count")
}

func TestExtractKeySuffix(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		input  string
		expect string
		hasErr bool
	}{
		{"/tidb/cdc/capture/info/6a6c6dd290bc8732", "6a6c6dd290bc8732", false},
		{"/tidb/cdc/capture/info/6a6c6dd290bc8732/", "", false},
		{"/tidb/cdc", "cdc", false},
		{"/tidb", "tidb", false},
		{"", "", true},
	}
	for _, tc := range testCases {
		key, err := ExtractKeySuffix(tc.input)
		if tc.hasErr {
			require.NotNil(t, err)
		} else {
			require.Nil(t, err)
			require.Equal(t, tc.expect, key)
		}
	}
}
