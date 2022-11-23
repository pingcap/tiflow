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
	cache := newRedoEventCache(1000)

	appender := cache.getAppender(3)
	require.True(t, appender.push(&model.RowChangedEvent{StartTs: 1, CommitTs: 2}, 100, false))
	require.True(t, appender.push(&model.RowChangedEvent{StartTs: 1, CommitTs: 2}, 200, true))
	require.True(t, appender.push(&model.RowChangedEvent{StartTs: 3, CommitTs: 4}, 300, true))
	// Append a unfinished transaction, which can't be popped.
	require.True(t, appender.push(&model.RowChangedEvent{StartTs: 5, CommitTs: 6}, 400, false))
	// Append beyond capacity, can't success.
	require.False(t, appender.push(&model.RowChangedEvent{StartTs: 5, CommitTs: 6}, 100, false))

	events, _, _ := cache.pop(1)
	require.Nil(t, events)

	events, size, pos := cache.pop(3)
	require.Equal(t, 3, len(events))
	require.Equal(t, uint64(600), size)
	require.Equal(t, 0, pos.Compare(engine.Position{StartTs: 3, CommitTs: 4}))

	require.Equal(t, uint64(400), cache.allocated)

	// Still can't push into the appender, because it's broken.
	require.False(t, appender.push(&model.RowChangedEvent{StartTs: 5, CommitTs: 6}, 100, false))

	// Clean broken events, and then push again.
	require.True(t, appender.broken)
	require.Equal(t, uint64(400), appender.cleanBrokenEvents())
	require.Equal(t, uint64(0), cache.allocated)
	require.True(t, appender.push(&model.RowChangedEvent{StartTs: 5, CommitTs: 6}, 100, false))
}
