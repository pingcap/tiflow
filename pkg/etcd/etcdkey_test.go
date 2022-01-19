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
	"github.com/pingcap/check"
	"github.com/pingcap/tiflow/pkg/util/testleak"
)

type etcdkeySuite struct{}

var _ = check.Suite(&etcdkeySuite{})

func (s *etcdkeySuite) TestEtcdKey(c *check.C) {
	defer testleak.AfterTest(c)()
	testcases := []struct {
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
		c.Assert(err, check.IsNil)
		c.Assert(k, check.DeepEquals, tc.expected)
		c.Assert(k.String(), check.Equals, tc.key)
	}
}

func (s *etcdkeySuite) TestEtcdKeyParseError(c *check.C) {
	defer testleak.AfterTest(c)()
	testCases := []struct {
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
	}}
	for _, tc := range testCases {
		k := new(CDCKey)
		err := k.Parse(tc.key)
		if tc.error {
			c.Assert(err, check.NotNil)
		} else {
			c.Assert(err, check.IsNil)
		}
	}
}
