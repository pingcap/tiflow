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

package kv

import (
	"testing"

	"github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/tiflow/pkg/util/testleak"
	"github.com/stretchr/testify/require"
)

type matcherSuite struct{}

var _ = check.Suite(&matcherSuite{})

func (s *matcherSuite) TestMatchRow(c *check.C) {
	defer testleak.AfterTest(c)()
	matcher := newMatcher()
	matcher.putPrewriteRow(&cdcpb.Event_Row{
		StartTs: 1,
		Key:     []byte("k1"),
		Value:   []byte("v1"),
	})
	matcher.putPrewriteRow(&cdcpb.Event_Row{
		StartTs:  2,
		Key:      []byte("k1"),
		Value:    []byte("v2"),
		OldValue: []byte("v3"),
	})

	// test rollback
	matcher.rollbackRow(&cdcpb.Event_Row{
		StartTs: 1,
		Key:     []byte("k1"),
	})
	commitRow1 := &cdcpb.Event_Row{
		StartTs: 1,
		Key:     []byte("k1"),
	}
	ok := matcher.matchRow(commitRow1, true)
	c.Assert(ok, check.IsFalse)
	c.Assert(commitRow1, check.DeepEquals, &cdcpb.Event_Row{
		StartTs: 1,
		Key:     []byte("k1"),
	})

	// test match commit
	commitRow2 := &cdcpb.Event_Row{
		StartTs:  2,
		CommitTs: 3,
		Key:      []byte("k1"),
	}
	ok = matcher.matchRow(commitRow2, true)
	c.Assert(ok, check.IsTrue)
	c.Assert(commitRow2, check.DeepEquals, &cdcpb.Event_Row{
		StartTs:  2,
		CommitTs: 3,
		Key:      []byte("k1"),
		Value:    []byte("v2"),
		OldValue: []byte("v3"),
	})
}

func (s *matcherSuite) TestMatchFakePrewrite(c *check.C) {
	defer testleak.AfterTest(c)()
	matcher := newMatcher()
	matcher.putPrewriteRow(&cdcpb.Event_Row{
		StartTs:  1,
		Key:      []byte("k1"),
		Value:    []byte("v1"),
		OldValue: []byte("v3"),
	})
	// fake prewrite
	matcher.putPrewriteRow(&cdcpb.Event_Row{
		StartTs:  1,
		Key:      []byte("k1"),
		OldValue: []byte("v4"),
	})

	commitRow1 := &cdcpb.Event_Row{
		StartTs:  1,
		CommitTs: 2,
		Key:      []byte("k1"),
	}
	ok := matcher.matchRow(commitRow1, true)
	c.Assert(commitRow1, check.DeepEquals, &cdcpb.Event_Row{
		StartTs:  1,
		CommitTs: 2,
		Key:      []byte("k1"),
		Value:    []byte("v1"),
		OldValue: []byte("v3"),
	})
	c.Assert(ok, check.IsTrue)
}

func (s *matcherSuite) TestMatchMatchCachedRow(c *check.C) {
	defer testleak.AfterTest(c)()
	matcher := newMatcher()
	c.Assert(len(matcher.matchCachedRow(true)), check.Equals, 0)
	matcher.cacheCommitRow(&cdcpb.Event_Row{
		StartTs:  1,
		CommitTs: 2,
		Key:      []byte("k1"),
	})
	matcher.cacheCommitRow(&cdcpb.Event_Row{
		StartTs:  3,
		CommitTs: 4,
		Key:      []byte("k2"),
	})
	matcher.cacheCommitRow(&cdcpb.Event_Row{
		StartTs:  4,
		CommitTs: 5,
		Key:      []byte("k3"),
	})
	c.Assert(len(matcher.matchCachedRow(true)), check.Equals, 0)

	matcher.cacheCommitRow(&cdcpb.Event_Row{
		StartTs:  1,
		CommitTs: 2,
		Key:      []byte("k1"),
	})
	matcher.cacheCommitRow(&cdcpb.Event_Row{
		StartTs:  3,
		CommitTs: 4,
		Key:      []byte("k2"),
	})
	matcher.cacheCommitRow(&cdcpb.Event_Row{
		StartTs:  4,
		CommitTs: 5,
		Key:      []byte("k3"),
	})

	matcher.putPrewriteRow(&cdcpb.Event_Row{
		StartTs:  1,
		Key:      []byte("k1"),
		Value:    []byte("v1"),
		OldValue: []byte("ov1"),
	})
	matcher.putPrewriteRow(&cdcpb.Event_Row{
		StartTs:  3,
		Key:      []byte("k2"),
		Value:    []byte("v2"),
		OldValue: []byte("ov2"),
	})
	matcher.putPrewriteRow(&cdcpb.Event_Row{
		StartTs:  4,
		Key:      []byte("k2"),
		Value:    []byte("v3"),
		OldValue: []byte("ov3"),
	})

	c.Assert(matcher.matchCachedRow(true), check.DeepEquals, []*cdcpb.Event_Row{{
		StartTs:  1,
		CommitTs: 2,
		Key:      []byte("k1"),
		Value:    []byte("v1"),
		OldValue: []byte("ov1"),
	}, {
		StartTs:  3,
		CommitTs: 4,
		Key:      []byte("k2"),
		Value:    []byte("v2"),
		OldValue: []byte("ov2"),
	}})
}

func TestMatchRowUninitialized(t *testing.T) {
	t.Parallel()
	matcher := newMatcher()

	// fake prewrite before init.
	matcher.putPrewriteRow(&cdcpb.Event_Row{
		StartTs:  1,
		Key:      []byte("k1"),
		OldValue: []byte("v4"),
	})
	commitRow1 := &cdcpb.Event_Row{
		StartTs:  1,
		CommitTs: 2,
		Key:      []byte("k1"),
	}
	ok := matcher.matchRow(commitRow1, false)
	require.Equal(t, &cdcpb.Event_Row{
		StartTs:  1,
		CommitTs: 2,
		Key:      []byte("k1"),
	}, commitRow1)
	require.False(t, ok)
	matcher.cacheCommitRow(commitRow1)

	// actual prewrite before init.
	matcher.putPrewriteRow(&cdcpb.Event_Row{
		StartTs:  1,
		Key:      []byte("k1"),
		Value:    []byte("v3"),
		OldValue: []byte("v4"),
	})

	// normal prewrite and commit before init.
	matcher.putPrewriteRow(&cdcpb.Event_Row{
		StartTs:  2,
		Key:      []byte("k2"),
		Value:    []byte("v3"),
		OldValue: []byte("v4"),
	})
	commitRow2 := &cdcpb.Event_Row{
		StartTs:  2,
		CommitTs: 3,
		Key:      []byte("k2"),
	}
	ok = matcher.matchRow(commitRow2, false)
	require.Equal(t, &cdcpb.Event_Row{
		StartTs:  2,
		CommitTs: 3,
		Key:      []byte("k2"),
		Value:    []byte("v3"),
		OldValue: []byte("v4"),
	}, commitRow2)
	require.True(t, ok)

	// match cached row after init.
	rows := matcher.matchCachedRow(true)
	require.Len(t, rows, 1)
	require.Equal(t, &cdcpb.Event_Row{
		StartTs:  1,
		CommitTs: 2,
		Key:      []byte("k1"),
		Value:    []byte("v3"),
		OldValue: []byte("v4"),
	}, rows[0])
}

func TestMatchMatchCachedRollbackRow(t *testing.T) {
	t.Parallel()
	matcher := newMatcher()
	matcher.matchCachedRollbackRow(true)
	matcher.cacheRollbackRow(&cdcpb.Event_Row{
		StartTs: 1,
		Key:     []byte("k1"),
	})
	matcher.cacheRollbackRow(&cdcpb.Event_Row{
		StartTs: 3,
		Key:     []byte("k2"),
	})
	matcher.cacheRollbackRow(&cdcpb.Event_Row{
		StartTs: 4,
		Key:     []byte("k3"),
	})
	matcher.matchCachedRollbackRow(true)

	matcher.cacheRollbackRow(&cdcpb.Event_Row{
		StartTs: 1,
		Key:     []byte("k1"),
	})
	matcher.cacheRollbackRow(&cdcpb.Event_Row{
		StartTs: 3,
		Key:     []byte("k2"),
	})
	matcher.cacheRollbackRow(&cdcpb.Event_Row{
		StartTs: 4,
		Key:     []byte("k3"),
	})

	matcher.putPrewriteRow(&cdcpb.Event_Row{
		StartTs:  1,
		Key:      []byte("k1"),
		Value:    []byte("v1"),
		OldValue: []byte("ov1"),
	})
	matcher.putPrewriteRow(&cdcpb.Event_Row{
		StartTs:  3,
		Key:      []byte("k2"),
		Value:    []byte("v2"),
		OldValue: []byte("ov2"),
	})
	matcher.putPrewriteRow(&cdcpb.Event_Row{
		StartTs:  4,
		Key:      []byte("k3"),
		Value:    []byte("v3"),
		OldValue: []byte("ov3"),
	})

	matcher.matchCachedRollbackRow(true)
	require.Empty(t, matcher.unmatchedValue)
}
