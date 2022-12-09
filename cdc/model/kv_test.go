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
	"github.com/pingcap/check"
	"github.com/pingcap/tiflow/pkg/regionspan"
	"github.com/pingcap/tiflow/pkg/util/testleak"
)

type kvSuite struct{}

var _ = check.Suite(&kvSuite{})

func (s *kvSuite) TestRegionFeedEvent(c *check.C) {
	defer testleak.AfterTest(c)()
	raw := &RawKVEntry{
		CRTs:   1,
		OpType: OpTypePut,
	}
	resolved := &ResolvedSpan{
		Span:       regionspan.ComparableSpan{Start: []byte("a"), End: []byte("b")},
		ResolvedTs: 111,
	}

	ev := &RegionFeedEvent{}
	c.Assert(ev.GetValue(), check.IsNil)

	ev = &RegionFeedEvent{Val: raw}
	c.Assert(ev.GetValue(), check.DeepEquals, raw)

	ev = &RegionFeedEvent{Resolved: resolved}
	c.Assert(ev.GetValue(), check.DeepEquals, resolved)

	c.Assert(resolved.String(), check.Equals, "span: [61, 62), resolved-ts: 111")
}

func (s *kvSuite) TestRawKVEntry(c *check.C) {
	defer testleak.AfterTest(c)()
	raw := &RawKVEntry{
		StartTs: 100,
		CRTs:    101,
		OpType:  OpTypePut,
		Key:     []byte("123"),
		Value:   []byte("345"),
	}

	c.Assert(raw.String(), check.Equals, "OpType: 1, Key: 123, Value: 345, OldValue: , StartTs: 100, CRTs: 101, RegionID: 0")
	c.Assert(raw.ApproximateSize(), check.Equals, int64(6))
}
