// Copyright 2019 PingCAP, Inc.
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
	"github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
)

type regionSuite struct{}

var _ = check.Suite(&regionSuite{})

func (s *regionSuite) TestCheckRegionsCover(c *check.C) {
	cases := []struct {
		regions []*metapb.Region
		span    Span
		cover   bool
	}{
		{[]*metapb.Region{}, Span{[]byte{1}, []byte{2}}, false},
		{[]*metapb.Region{{StartKey: nil, EndKey: nil}}, Span{[]byte{1}, []byte{2}}, true},
		{[]*metapb.Region{{StartKey: []byte{1}, EndKey: []byte{2}}}, Span{[]byte{1}, []byte{2}}, true},
		{[]*metapb.Region{{StartKey: []byte{0}, EndKey: []byte{4}}}, Span{[]byte{1}, []byte{2}}, true},
		{[]*metapb.Region{
			{StartKey: []byte{1}, EndKey: []byte{2}},
			{StartKey: []byte{2}, EndKey: []byte{3}},
		}, Span{[]byte{1}, []byte{3}}, true},
		{[]*metapb.Region{
			{StartKey: []byte{1}, EndKey: []byte{2}},
			{StartKey: []byte{3}, EndKey: []byte{4}},
		}, Span{[]byte{1}, []byte{4}}, false},
		{[]*metapb.Region{
			{StartKey: []byte{1}, EndKey: []byte{2}},
			{StartKey: []byte{2}, EndKey: []byte{3}},
		}, Span{[]byte{1}, []byte{4}}, false},
		{[]*metapb.Region{
			{StartKey: []byte{2}, EndKey: []byte{3}},
		}, Span{[]byte{1}, []byte{3}}, false},
	}

	for _, tc := range cases {
		c.Assert(CheckRegionsCover(tc.regions, tc.span), check.Equals, tc.cover)
	}
}
