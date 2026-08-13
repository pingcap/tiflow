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

package utils

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseStartTime(t *testing.T) {
	var err error
	_, err = ParseStartTime("2006-01-02T15:04:05")
	require.NoError(t, err)
	_, err = ParseStartTime("2006-01-02 15:04:05")
	require.NoError(t, err)
	_, err = ParseStartTime("2006-01-02T15:04:05+08:00")
	require.NoError(t, err)
	_, err = ParseStartTime("2006-01-02 15:04:05+08:00")
	require.NoError(t, err)
	_, err = ParseStartTime("2006-01-02T15:04:05+0800")
	require.NoError(t, err)
	_, err = ParseStartTime("2006-01-02 15:04:05+0800")
	require.NoError(t, err)
	_, err = ParseStartTime("2006-01-02T15:04:05Z")
	require.NoError(t, err)

	for _, invalid := range []string{
		"15:04:05",
		"2006/01/02 15:04:05",
		"20060102 150405",
		"2006-01-02",
		"2006-01-02 15:04:05.123",
	} {
		_, err = ParseStartTime(invalid)
		require.Error(t, err)
		require.ErrorContains(t, err, "unsupported start-time format")
	}
}

func TestParseStartTimeInLoc(t *testing.T) {
	upstreamTZ := time.UTC

	legacy, err := ParseStartTimeInLoc("2026-04-17 08:00:00", upstreamTZ)
	require.NoError(t, err)
	require.Equal(t, time.Date(2026, 4, 17, 8, 0, 0, 0, time.UTC).Unix(), legacy.Unix())

	absolute, err := ParseStartTimeInLoc("2026-04-17T08:00:00+08:00", upstreamTZ)
	require.NoError(t, err)
	require.Equal(t, time.Date(2026, 4, 17, 0, 0, 0, 0, time.UTC).Unix(), absolute.Unix())

	absoluteWithoutColon, err := ParseStartTimeInLoc("2026-04-17T08:00:00+0800", upstreamTZ)
	require.NoError(t, err)
	require.Equal(t, time.Date(2026, 4, 17, 0, 0, 0, 0, time.UTC).Unix(), absoluteWithoutColon.Unix())
}
