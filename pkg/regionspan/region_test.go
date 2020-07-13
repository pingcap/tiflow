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

package regionspan

import (
	"github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
)

type regionSuite struct{}

var _ = check.Suite(&regionSuite{})

func (s *regionSuite) TestCheckRegionsLeftCover(c *check.C) {
	cases := []struct {
		regions []*metapb.Region
		span    ComparableSpan
		cover   bool
	}{
		{[]*metapb.Region{}, ComparableSpan{[]byte{1}, []byte{2}}, false},
		{[]*metapb.Region{{StartKey: nil, EndKey: nil}}, ComparableSpan{[]byte{1}, []byte{2}}, true},
		{[]*metapb.Region{{StartKey: []byte{1}, EndKey: []byte{2}}}, ComparableSpan{[]byte{1}, []byte{2}}, true},
		{[]*metapb.Region{{StartKey: []byte{0}, EndKey: []byte{4}}}, ComparableSpan{[]byte{1}, []byte{2}}, true},
		{[]*metapb.Region{
			{StartKey: []byte{1}, EndKey: []byte{2}},
			{StartKey: []byte{2}, EndKey: []byte{3}},
		}, ComparableSpan{[]byte{1}, []byte{3}}, true},
		{[]*metapb.Region{
			{StartKey: []byte{1}, EndKey: []byte{2}},
			{StartKey: []byte{3}, EndKey: []byte{4}},
		}, ComparableSpan{[]byte{1}, []byte{4}}, false},
		{[]*metapb.Region{
			{StartKey: []byte{1}, EndKey: []byte{2}},
			{StartKey: []byte{2}, EndKey: []byte{3}},
		}, ComparableSpan{[]byte{1}, []byte{4}}, true},
		{[]*metapb.Region{
			{StartKey: []byte{2}, EndKey: []byte{3}},
		}, ComparableSpan{[]byte{1}, []byte{3}}, false},
	}

	for _, tc := range cases {
		c.Assert(CheckRegionsLeftCover(tc.regions, tc.span), check.Equals, tc.cover)
	}
}
