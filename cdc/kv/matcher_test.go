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

	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/stretchr/testify/require"
)

func TestMatchRow(t *testing.T) {
	t.Parallel()
	matcher := newMatcher()
	p1 := &cdcpb.Event_Row{
		StartTs: 1,
		Key:     []byte("k1"),
		Value:   []byte("v1"),
	}
	p1Key := newMatchKey(p1)
	size := matcher.putPrewriteRow(p1)
	require.Equal(t, sizeOfEventRow(p1)+(&p1Key).size(), size)
	p2 := &cdcpb.Event_Row{
		StartTs:  2,
		Key:      []byte("k1"),
		Value:    []byte("v2"),
		OldValue: []byte("v3"),
	}
	p2Key := newMatchKey(p2)
	size = matcher.putPrewriteRow(p2)
	require.Equal(t, sizeOfEventRow(p2)+(&p2Key).size(), size)

	// test rollback
	r1 := &cdcpb.Event_Row{
		StartTs: 1,
		Key:     []byte("k1"),
	}
	size = matcher.rollbackRow(r1)
	require.Equal(t, -(sizeOfEventRow(p1) + (&p1Key).size()), size)

	commitRow1 := &cdcpb.Event_Row{
		StartTs: 1,
		Key:     []byte("k1"),
	}
	ok, size := matcher.matchRow(commitRow1)
	require.False(t, ok)
	require.Equal(t, &cdcpb.Event_Row{
		StartTs: 1,
		Key:     []byte("k1"),
	}, commitRow1)
	require.Equal(t, 0, size)

	// test match commit
	commitRow2 := &cdcpb.Event_Row{
		StartTs:  2,
		CommitTs: 3,
		Key:      []byte("k1"),
	}
	ok, size = matcher.matchRow(commitRow2)
	require.True(t, ok)
	require.Equal(t, &cdcpb.Event_Row{
		StartTs:  2,
		CommitTs: 3,
		Key:      []byte("k1"),
		Value:    []byte("v2"),
		OldValue: []byte("v3"),
	}, commitRow2)
	require.Equal(t, -(sizeOfEventRow(p2) + (&p2Key).size()), size)
}

func TestMatchFakePrewrite(t *testing.T) {
	t.Parallel()
	matcher := newMatcher()
	p1 := &cdcpb.Event_Row{
		StartTs:  1,
		Key:      []byte("k1"),
		Value:    []byte("v1"),
		OldValue: []byte("v3"),
	}
	p1Key := newMatchKey(p1)
	size := matcher.putPrewriteRow(p1)
	require.Equal(t, sizeOfEventRow(p1)+(&p1Key).size(), size)

	// fake prewrite
	size = matcher.putPrewriteRow(&cdcpb.Event_Row{
		StartTs:  1,
		Key:      []byte("k1"),
		OldValue: []byte("v4"),
	})
	require.Equal(t, 0, size)

	commitRow1 := &cdcpb.Event_Row{
		StartTs:  1,
		CommitTs: 2,
		Key:      []byte("k1"),
	}
	c1Key := newMatchKey(commitRow1)
	ok, size := matcher.matchRow(commitRow1)
	require.Equal(t, &cdcpb.Event_Row{
		StartTs:  1,
		CommitTs: 2,
		Key:      []byte("k1"),
		Value:    []byte("v1"),
		OldValue: []byte("v3"),
	}, commitRow1)
	require.True(t, ok)
	require.Equal(t, -(sizeOfEventRow(commitRow1) + (&c1Key).size()), size)
}

func TestMatchMatchCachedRow(t *testing.T) {
	t.Parallel()
	matcher := newMatcher()
	rows, size := matcher.matchCachedRow()
	require.Empty(t, 0, rows)
	require.Equal(t, 0, size)
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
	rows, size = matcher.matchCachedRow()
	require.Empty(t, 0, rows)
	require.Equal(t, 0, size)

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

	rows, size = matcher.matchCachedRow()
	require.Equal(t, []*cdcpb.Event_Row{{
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
	}}, rows)
	require.True(t, 0 > size)
}
