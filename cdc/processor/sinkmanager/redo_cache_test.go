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
	"github.com/stretchr/testify/require"
)

func TestRedoEventCache(t *testing.T) {
	cache := newRedoEventCache(model.ChangeFeedID{}, 1000)
	var ok bool
	var broken uint64
	var popRes popResult

	appender := cache.maybeCreateAppender(3, engine.Position{StartTs: 1, CommitTs: 2})

	appender.push(&model.RowChangedEvent{StartTs: 1, CommitTs: 2}, 100, engine.Position{})
	appender.push(&model.RowChangedEvent{StartTs: 1, CommitTs: 2}, 200, engine.Position{})
	appender.push(&model.RowChangedEvent{StartTs: 3, CommitTs: 4}, 300, engine.Position{StartTs: 3, CommitTs: 4})
	appender.push(&model.RowChangedEvent{StartTs: 5, CommitTs: 6}, 400, engine.Position{})
	require.Equal(t, uint64(1000), cache.allocated)

	ok, broken = appender.push(&model.RowChangedEvent{StartTs: 5, CommitTs: 6}, 400, engine.Position{StartTs: 3, CommitTs: 4})
	require.False(t, ok)
	require.Equal(t, uint64(400), broken)
	require.True(t, appender.broken)
	require.Equal(t, uint64(3), appender.upperBound.StartTs)
	require.Equal(t, uint64(4), appender.upperBound.CommitTs)

	popRes = appender.pop(engine.Position{StartTs: 0, CommitTs: 1}, engine.Position{StartTs: 5, CommitTs: 6})
	require.False(t, popRes.success)
	require.Equal(t, uint64(1), popRes.boundary.StartTs)
	require.Equal(t, uint64(2), popRes.boundary.CommitTs)

	popRes = appender.pop(engine.Position{StartTs: 1, CommitTs: 2}, engine.Position{StartTs: 1, CommitTs: 2})
	require.True(t, popRes.success)
	require.Equal(t, 2, len(popRes.events))
	require.Equal(t, uint64(300), popRes.size)
	require.Equal(t, 2, popRes.pushCount)
	require.Equal(t, uint64(1), popRes.boundary.StartTs)
	require.Equal(t, uint64(2), popRes.boundary.CommitTs)

	popRes = appender.pop(engine.Position{StartTs: 1, CommitTs: 2}, engine.Position{StartTs: 1, CommitTs: 2})
	require.False(t, popRes.success)

	popRes = appender.pop(engine.Position{StartTs: 2, CommitTs: 2}, engine.Position{StartTs: 7, CommitTs: 8})
	require.True(t, popRes.success)
	require.Equal(t, 1, len(popRes.events))
	require.Equal(t, uint64(300), popRes.size)
	require.Equal(t, 1, popRes.pushCount)
	require.Equal(t, uint64(3), popRes.boundary.StartTs)
	require.Equal(t, uint64(4), popRes.boundary.CommitTs)
	require.Equal(t, 0, len(appender.events))
	require.True(t, appender.broken)

	appender = cache.maybeCreateAppender(3, engine.Position{StartTs: 11, CommitTs: 12})
	require.False(t, appender.broken)
	require.Equal(t, uint64(0), appender.upperBound.StartTs)
	require.Equal(t, uint64(0), appender.upperBound.CommitTs)
}
