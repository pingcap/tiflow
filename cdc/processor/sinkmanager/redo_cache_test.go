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

package sinkmanager

import (
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/engine"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/stretchr/testify/require"
)

func TestRedoEventCache(t *testing.T) {
	cache := newRedoEventCache(model.ChangeFeedID{}, 1000)
	var ok bool
	var broken uint64
	var popRes popResult

	span := spanz.TableIDToComparableSpan(3)
	appender := cache.maybeCreateAppender(span, engine.Position{StartTs: 3, CommitTs: 4})

	appender.push(&model.RowChangedEvent{StartTs: 3, CommitTs: 4}, 100, engine.Position{})
	appender.push(&model.RowChangedEvent{StartTs: 3, CommitTs: 4}, 200, engine.Position{})
	appender.push(&model.RowChangedEvent{StartTs: 5, CommitTs: 6}, 300, engine.Position{StartTs: 5, CommitTs: 6})
	appender.push(&model.RowChangedEvent{StartTs: 7, CommitTs: 8}, 400, engine.Position{})
	require.Equal(t, uint64(1000), cache.allocated)

	ok, broken = appender.push(&model.RowChangedEvent{StartTs: 7, CommitTs: 8}, 400, engine.Position{StartTs: 7, CommitTs: 8})
	require.False(t, ok)
	require.Equal(t, uint64(400), broken)
	require.True(t, appender.broken)
	require.Equal(t, uint64(5), appender.upperBound.StartTs)
	require.Equal(t, uint64(6), appender.upperBound.CommitTs)
	// Until here, cache contains events in range [{3,4}, {5,6}].

	// Try to pop [{0,1}, {0,4}], shoud fail. And the returned boundary should be {1,4}.
	popRes = appender.pop(engine.Position{StartTs: 0, CommitTs: 1}, engine.Position{StartTs: 0, CommitTs: 4})
	require.False(t, popRes.success)
	require.Equal(t, uint64(1), popRes.boundary.StartTs)
	require.Equal(t, uint64(4), popRes.boundary.CommitTs)

	// Try to pop [{0,2}, {0,4}], shoud fail. And the returned boundary should be {3,4}.
	popRes = appender.pop(engine.Position{StartTs: 0, CommitTs: 1}, engine.Position{StartTs: 5, CommitTs: 6})
	require.False(t, popRes.success)
	require.Equal(t, uint64(3), popRes.boundary.StartTs)
	require.Equal(t, uint64(4), popRes.boundary.CommitTs)

	// Try to pop [{3,4}, {3,4}], should success.
	popRes = appender.pop(engine.Position{StartTs: 3, CommitTs: 4}, engine.Position{StartTs: 3, CommitTs: 4})
	require.True(t, popRes.success)
	require.Equal(t, 2, len(popRes.events))
	require.Equal(t, uint64(300), popRes.size)
	require.Equal(t, 2, popRes.pushCount)
	require.Equal(t, uint64(3), popRes.boundary.StartTs)
	require.Equal(t, uint64(4), popRes.boundary.CommitTs)

	// Try to pop [{3,4}, {3,4}] again, shoud fail. And the returned boundary should be {4,4}.
	popRes = appender.pop(engine.Position{StartTs: 3, CommitTs: 4}, engine.Position{StartTs: 3, CommitTs: 4})
	require.False(t, popRes.success)
	require.Equal(t, uint64(4), popRes.boundary.StartTs)
	require.Equal(t, uint64(4), popRes.boundary.CommitTs)

	popRes = appender.pop(engine.Position{StartTs: 4, CommitTs: 4}, engine.Position{StartTs: 9, CommitTs: 10})
	require.True(t, popRes.success)
	require.Equal(t, 1, len(popRes.events))
	require.Equal(t, uint64(300), popRes.size)
	require.Equal(t, 1, popRes.pushCount)
	require.Equal(t, uint64(5), popRes.boundary.StartTs)
	require.Equal(t, uint64(6), popRes.boundary.CommitTs)
	require.Equal(t, 0, len(appender.events))
	require.True(t, appender.broken)

	appender = cache.maybeCreateAppender(span, engine.Position{StartTs: 11, CommitTs: 12})
	require.False(t, appender.broken)
	require.Equal(t, uint64(0), appender.upperBound.StartTs)
	require.Equal(t, uint64(0), appender.upperBound.CommitTs)
}
