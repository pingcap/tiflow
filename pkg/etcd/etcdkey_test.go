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

package etcd

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEtcdKey(t *testing.T) {
	testcases := []struct {
		name     string
		key      string
		expected *CDCKey
	}{{
		key: "/tidb/cdc/owner/223176cb44d20a13",
		expected: &CDCKey{
			Tp:           CDCKeyTypeOwner,
			OwnerLeaseID: "223176cb44d20a13",
		},
	}, {
		key: "/tidb/cdc/owner",
		expected: &CDCKey{
			Tp:           CDCKeyTypeOwner,
			OwnerLeaseID: "",
		},
	}, {
		key: "/tidb/cdc/capture/6bbc01c8-0605-4f86-a0f9-b3119109b225",
		expected: &CDCKey{
			Tp:        CDCKeyTypeCapture,
			CaptureID: "6bbc01c8-0605-4f86-a0f9-b3119109b225",
		},
	}, {
		key: "/tidb/cdc/changefeed/info/test-_@#$%changefeed",
		expected: &CDCKey{
			Tp:           CDCKeyTypeChangefeedInfo,
			ChangefeedID: "test-_@#$%changefeed",
		},
	}, {
		key: "/tidb/cdc/changefeed/info/test/changefeed",
		expected: &CDCKey{
			Tp:           CDCKeyTypeChangefeedInfo,
			ChangefeedID: "test/changefeed",
		},
	}, {
		key: "/tidb/cdc/job/test-changefeed",
		expected: &CDCKey{
			Tp:           CDCKeyTypeChangeFeedStatus,
			ChangefeedID: "test-changefeed",
		},
	}, {
		name: "verification",
		key:  "/tidb/cdc/verification/test-changefeed1",
		expected: &CDCKey{
			Tp:           CDCKeyTypeVerification,
			ChangefeedID: "test-changefeed1",
		},
	}, {
		key: "/tidb/cdc/task/position/6bbc01c8-0605-4f86-a0f9-b3119109b225/test-changefeed",
		expected: &CDCKey{
			Tp:           CDCKeyTypeTaskPosition,
			ChangefeedID: "test-changefeed",
			CaptureID:    "6bbc01c8-0605-4f86-a0f9-b3119109b225",
		},
	}, {
		key: "/tidb/cdc/task/position/6bbc01c8-0605-4f86-a0f9-b3119109b225/test/changefeed",
		expected: &CDCKey{
			Tp:           CDCKeyTypeTaskPosition,
			ChangefeedID: "test/changefeed",
			CaptureID:    "6bbc01c8-0605-4f86-a0f9-b3119109b225",
		},
	}, {
		key: "/tidb/cdc/task/status/6bbc01c8-0605-4f86-a0f9-b3119109b225/test-changefeed",
		expected: &CDCKey{
			Tp:           CDCKeyTypeTaskStatus,
			ChangefeedID: "test-changefeed",
			CaptureID:    "6bbc01c8-0605-4f86-a0f9-b3119109b225",
		},
	}, {
		key: "/tidb/cdc/task/workload/6bbc01c8-0605-4f86-a0f9-b3119109b225/test-changefeed",
		expected: &CDCKey{
			Tp:           CDCKeyTypeTaskWorkload,
			ChangefeedID: "test-changefeed",
			CaptureID:    "6bbc01c8-0605-4f86-a0f9-b3119109b225",
		},
	}}
	for _, tc := range testcases {
		k := new(CDCKey)
		err := k.Parse(tc.key)
		require.NoError(t, err, tc.name)
		require.Equal(t, k, tc.expected, tc.name)
		require.Equal(t, k.String(), tc.key, tc.name)
	}
}

func TestEtcdKeyParseError(t *testing.T) {
	testCases := []struct {
		name  string
		key   string
		error bool
	}{{
		key:   "/tidb/cdc/task/position/6bbc01c8-0605-4f86-a0f9-b3119109b225/test/changefeed",
		error: false,
	}, {
		key:   "/tidb/cdc/task/position/6bbc01c8-0605-4f86-a0f9-b3119109b225/",
		error: false,
	}, {
		key:   "/tidb/cdc/task/position/6bbc01c8-0605-4f86-a0f9-b3119109b225",
		error: true,
	}, {
		key:   "/tidb/cdc/task/status/6bbc01c8-0605-4f86-a0f9-b3119109b225",
		error: true,
	}, {
		key:   "/tidb/cdc/task/workload/6bbc01c8-0605-4f86-a0f9-b3119109b225",
		error: true,
	}, {
		key:   "/tidb/cd",
		error: true,
	}, {
		key:   "/tidb/cdc/",
		error: true,
	}, {
		name:  "verification",
		key:   "/tidb/cdc/verification/16bbc01c8-0605-4f86-a0f9-b3119109b225",
		error: false,
	}}
	for _, tc := range testCases {
		k := new(CDCKey)
		err := k.Parse(tc.key)
		if tc.error {
			require.NotNil(t, err, tc.name)
		} else {
			require.Nil(t, err, tc.name)
		}
	}
}
