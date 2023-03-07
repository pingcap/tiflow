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

package version

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestChangefeedStateFromAdminJob(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		creatorVersion string
		expected       bool
	}{
		{
			creatorVersion: "",
			expected:       true,
		},
		{
			creatorVersion: "4.0.12",
			expected:       true,
		},
		{
			creatorVersion: "4.0.14",
			expected:       true,
		},
		{
			creatorVersion: "4.0.15",
			expected:       true,
		},
		{
			creatorVersion: "4.0.16",
			expected:       false,
		},
		{
			creatorVersion: "5.0.0",
			expected:       true,
		},
		{
			creatorVersion: "5.0.1",
			expected:       true,
		},
		{
			creatorVersion: "5.0.6",
			expected:       false,
		},
		{
			creatorVersion: "5.1.0",
			expected:       false,
		},
		{
			creatorVersion: "5.2.0",
			expected:       false,
		},
		{
			creatorVersion: "5.3.0",
			expected:       false,
		},
	}

	for _, tc := range testCases {
		creatorVersionGate := CreatorVersionGate{version: tc.creatorVersion}
		require.Equal(t, tc.expected, creatorVersionGate.ChangefeedStateFromAdminJob())
	}
}

func TestChangefeedAcceptUnknownProtocols(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		creatorVersion string
		expected       bool
	}{
		{
			creatorVersion: "",
			expected:       true,
		},
		{
			creatorVersion: "4.0.12",
			expected:       true,
		},
		{
			creatorVersion: "4.0.14",
			expected:       true,
		},
		{
			creatorVersion: "4.0.15",
			expected:       true,
		},
		{
			creatorVersion: "5.0.0",
			expected:       true,
		},
		{
			creatorVersion: "5.0.1",
			expected:       true,
		},
		{
			creatorVersion: "5.1.0",
			expected:       true,
		},
		{
			creatorVersion: "5.2.0",
			expected:       true,
		},
		{
			creatorVersion: "5.3.0",
			expected:       true,
		},
		{
			creatorVersion: "5.4.0",
			expected:       false,
		},
	}

	for _, tc := range testCases {
		creatorVersionGate := CreatorVersionGate{version: tc.creatorVersion}
		require.Equal(t, tc.expected, creatorVersionGate.ChangefeedAcceptUnknownProtocols())
	}
}

func TestChangefeedAcceptProtocolInMysqlSinURI(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		creatorVersion string
		expected       bool
	}{
		{
			creatorVersion: "",
			expected:       true,
		},
		{
			creatorVersion: "4.0.12",
			expected:       true,
		},
		{
			creatorVersion: "4.0.14",
			expected:       true,
		},
		{
			creatorVersion: "4.0.15",
			expected:       true,
		},
		{
			creatorVersion: "5.0.0",
			expected:       true,
		},
		{
			creatorVersion: "5.0.1",
			expected:       true,
		},
		{
			creatorVersion: "5.1.0",
			expected:       true,
		},
		{
			creatorVersion: "5.2.0-nightly",
			expected:       true,
		},
		{
			creatorVersion: "5.3.0",
			expected:       true,
		},
		{
			creatorVersion: "6.1.0",
			expected:       true,
		},
		{
			creatorVersion: "6.1.1",
			expected:       false,
		},
		{
			creatorVersion: "6.2.0",
			expected:       false,
		},
	}

	for _, tc := range testCases {
		creatorVersionGate := CreatorVersionGate{version: tc.creatorVersion}
		require.Equal(t, tc.expected, creatorVersionGate.ChangefeedAcceptProtocolInMysqlSinURI())
	}
}

func TestChangefeedInheritSchedulerConfigFromV66(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		creatorVersion string
		expected       bool
	}{
		{
			creatorVersion: "",
			expected:       false,
		},
		{
			creatorVersion: "4.0.12",
			expected:       false,
		},
		{
			creatorVersion: "6.6.0-aplha",
			expected:       true,
		},
		{
			creatorVersion: "6.6.0",
			expected:       true,
		},
		{
			creatorVersion: "6.7.0",
			expected:       false,
		},
	}

	for _, tc := range testCases {
		creatorVersionGate := CreatorVersionGate{version: tc.creatorVersion}
		require.Equal(t, tc.expected, creatorVersionGate.ChangefeedInheritSchedulerConfigFromV66())
	}
}
