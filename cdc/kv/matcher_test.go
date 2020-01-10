package kv

import (
	"github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/cdcpb"
)

type MatcherSuite struct{}

var _ = check.Suite(&MatcherSuite{})

func (s *MatcherSuite) TestMatcher(c *check.C) {
	matcher := newMatcher()
	matcher.putPrewriteRow(&cdcpb.Event_Row{
		StartTs: 1,
		Key:     []byte("k1"),
		Value:   []byte("v1"),
	})
	matcher.putPrewriteRow(&cdcpb.Event_Row{
		StartTs: 2,
		Key:     []byte("k1"),
		Value:   []byte("v2"),
	})
	matcher.rollbackRow(&cdcpb.Event_Row{
		StartTs: 1,
		Key:     []byte("k1"),
	})
	commitRow1 := &cdcpb.Event_Row{
		StartTs: 1,
		Key:     []byte("k1"),
	}
	err := matcher.matchRow(commitRow1)
	c.Assert(err, check.ErrorMatches, "*not found*")
	commitRow2 := &cdcpb.Event_Row{
		StartTs: 2,
		Key:     []byte("k1"),
	}
	err = matcher.matchRow(commitRow2)
	c.Assert(err, check.IsNil)
	c.Assert(commitRow2.Value, check.BytesEquals, []byte("v2"))
}
