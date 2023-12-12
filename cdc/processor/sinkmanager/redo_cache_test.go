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
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/sorter"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/stretchr/testify/require"
)

func TestRedoEventCache(t *testing.T) {
	cache := newRedoEventCache(model.ChangeFeedID{}, 1000)
	var ok bool
	var broken uint64
	var popRes popResult

	span := spanz.TableIDToComparableSpan(3)
	appender := cache.maybeCreateAppender(span, sorter.Position{StartTs: 3, CommitTs: 4})

	appender.push(&model.RowChangedEvent{StartTs: 3, CommitTs: 4}, 100, sorter.Position{})
	appender.push(&model.RowChangedEvent{StartTs: 3, CommitTs: 4}, 200, sorter.Position{})
	appender.push(&model.RowChangedEvent{StartTs: 5, CommitTs: 6}, 300, sorter.Position{StartTs: 5, CommitTs: 6})
	appender.push(&model.RowChangedEvent{StartTs: 7, CommitTs: 8}, 400, sorter.Position{})
	require.Equal(t, uint64(1000), cache.allocated)

	ok, broken = appender.push(&model.RowChangedEvent{StartTs: 7, CommitTs: 8}, 400, sorter.Position{StartTs: 7, CommitTs: 8})
	require.False(t, ok)
	require.Equal(t, uint64(400), broken)
	require.True(t, appender.broken)
	require.Equal(t, uint64(5), appender.upperBound.StartTs)
	require.Equal(t, uint64(6), appender.upperBound.CommitTs)
	// Until here, cache contains events in range [{3,4}, {5,6}].

	// Try to pop [{0,1}, {0,4}], shoud fail. And the returned boundary should be {1,4}.
	popRes = appender.pop(sorter.Position{StartTs: 0, CommitTs: 1}, sorter.Position{StartTs: 0, CommitTs: 4})
	require.False(t, popRes.success)
	require.Equal(t, uint64(1), popRes.lowerBoundIfFail.StartTs)
	require.Equal(t, uint64(4), popRes.lowerBoundIfFail.CommitTs)

	// Try to pop [{0,2}, {0,4}], shoud fail. And the returned boundary should be {3,4}.
	popRes = appender.pop(sorter.Position{StartTs: 0, CommitTs: 1}, sorter.Position{StartTs: 5, CommitTs: 6})
	require.False(t, popRes.success)
	require.Equal(t, uint64(3), popRes.lowerBoundIfFail.StartTs)
	require.Equal(t, uint64(4), popRes.lowerBoundIfFail.CommitTs)

	// Try to pop [{3,4}, {3,4}], should success.
	popRes = appender.pop(sorter.Position{StartTs: 3, CommitTs: 4}, sorter.Position{StartTs: 3, CommitTs: 4})
	require.True(t, popRes.success)
	require.Equal(t, 2, len(popRes.events))
	require.Equal(t, uint64(300), popRes.size)
	require.Equal(t, 2, popRes.pushCount)
	require.Equal(t, uint64(3), popRes.upperBoundIfSuccess.StartTs)
	require.Equal(t, uint64(4), popRes.upperBoundIfSuccess.CommitTs)

	// Try to pop [{3,4}, {3,4}] again, shoud fail. And the returned boundary should be {4,4}.
	popRes = appender.pop(sorter.Position{StartTs: 3, CommitTs: 4}, sorter.Position{StartTs: 3, CommitTs: 4})
	require.False(t, popRes.success)
	require.Equal(t, uint64(4), popRes.lowerBoundIfFail.StartTs)
	require.Equal(t, uint64(4), popRes.lowerBoundIfFail.CommitTs)

	popRes = appender.pop(sorter.Position{StartTs: 4, CommitTs: 4}, sorter.Position{StartTs: 9, CommitTs: 10})
	require.True(t, popRes.success)
	require.Equal(t, 1, len(popRes.events))
	require.Equal(t, uint64(300), popRes.size)
	require.Equal(t, 1, popRes.pushCount)
	require.Equal(t, uint64(5), popRes.upperBoundIfSuccess.StartTs)
	require.Equal(t, uint64(6), popRes.upperBoundIfSuccess.CommitTs)
	require.Equal(t, 0, len(appender.events))
	require.True(t, appender.broken)

	appender = cache.maybeCreateAppender(span, sorter.Position{StartTs: 11, CommitTs: 12})
	require.False(t, appender.broken)
	require.Equal(t, uint64(0), appender.upperBound.StartTs)
	require.Equal(t, uint64(0), appender.upperBound.CommitTs)
}

func TestRedoEventCacheAllPopBranches(t *testing.T) {
	cache := newRedoEventCache(model.ChangeFeedID{}, 1000)
	span := spanz.TableIDToComparableSpan(3)
	appender := cache.maybeCreateAppender(span, sorter.Position{StartTs: 101, CommitTs: 111})
	var batch []*model.RowChangedEvent
	var ok bool
	var popRes popResult

	batch = []*model.RowChangedEvent{{StartTs: 1, CommitTs: 11}, {StartTs: 1, CommitTs: 11}}
	ok, _ = appender.pushBatch(batch, 0, sorter.Position{})
	require.True(t, ok)

	batch = []*model.RowChangedEvent{{StartTs: 2, CommitTs: 12}}
	ok, _ = appender.pushBatch(batch, 0, sorter.Position{})
	require.True(t, ok)

	popRes = appender.pop(sorter.Position{StartTs: 1, CommitTs: 2}, sorter.Position{StartTs: 3, CommitTs: 4})
	require.False(t, popRes.success)
	require.Equal(t, sorter.Position{StartTs: 4, CommitTs: 4}, popRes.lowerBoundIfFail)

	popRes = appender.pop(sorter.Position{StartTs: 1, CommitTs: 2}, sorter.Position{StartTs: 300, CommitTs: 400})
	require.False(t, popRes.success)
	require.Equal(t, sorter.Position{StartTs: 101, CommitTs: 111}, popRes.lowerBoundIfFail)

	popRes = appender.pop(sorter.Position{StartTs: 1, CommitTs: 11}, sorter.Position{StartTs: 2, CommitTs: 12})
	require.False(t, popRes.success)
	require.Equal(t, sorter.Position{StartTs: 3, CommitTs: 12}, popRes.lowerBoundIfFail)

	batch = []*model.RowChangedEvent{{StartTs: 101, CommitTs: 111}, {StartTs: 101, CommitTs: 111}}
	ok, _ = appender.pushBatch(batch, 0, sorter.Position{StartTs: 101, CommitTs: 111})
	require.True(t, ok)

	batch = []*model.RowChangedEvent{{StartTs: 102, CommitTs: 112}}
	ok, _ = appender.pushBatch(batch, 0, sorter.Position{})
	require.True(t, ok)
	require.Equal(t, 5, appender.readyCount)

	popRes = appender.pop(sorter.Position{StartTs: 101, CommitTs: 111}, sorter.Position{StartTs: 102, CommitTs: 112})
	require.True(t, popRes.success)
	require.Equal(t, sorter.Position{StartTs: 101, CommitTs: 111}, popRes.upperBoundIfSuccess)
	require.Equal(t, 2, len(popRes.events))
	require.Equal(t, 1, popRes.pushCount)
	require.Equal(t, uint64(101), popRes.events[1].StartTs)
	require.Equal(t, 0, appender.readyCount)

	popRes = appender.pop(sorter.Position{StartTs: 102, CommitTs: 111}, sorter.Position{StartTs: 102, CommitTs: 112})
	require.False(t, popRes.success)
	require.Equal(t, sorter.Position{StartTs: 103, CommitTs: 112}, popRes.lowerBoundIfFail)

	batch = []*model.RowChangedEvent{{StartTs: 102, CommitTs: 112}}
	ok, _ = appender.pushBatch(batch, 0, sorter.Position{StartTs: 102, CommitTs: 102})
	require.True(t, ok)
	require.Equal(t, 2, appender.readyCount)

	popRes = appender.pop(sorter.Position{StartTs: 501, CommitTs: 502}, sorter.Position{StartTs: 701, CommitTs: 702})
	require.True(t, popRes.success)
	require.Equal(t, 0, len(popRes.events))
	require.Equal(t, sorter.Position{StartTs: 500, CommitTs: 502}, popRes.upperBoundIfSuccess)
	require.Equal(t, 0, appender.readyCount)
	require.Equal(t, 0, len(appender.events))
}
