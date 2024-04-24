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
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/sorter"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

const (
	// defaultRequestMemSize is the default memory usage for a request.
	defaultRequestMemSize = uint64(1024 * 1024) // 1MB
	// Avoid update resolved ts too frequently, if there are too many small transactions.
	defaultMaxUpdateIntervalSize = uint64(1024 * 256) // 256KB
	// bufferSize is the size of the buffer used to store the events.
	bufferSize = 1024
)

// Make these values be variables, so that we can mock them in unit tests.
var (
	requestMemSize        = defaultRequestMemSize
	maxUpdateIntervalSize = defaultMaxUpdateIntervalSize

	// Sink manager schedules table tasks based on lag. Limit the max task range
	// can be helpful to reduce changefeed latency for large initial data.
	maxTaskTimeRange = 30 * time.Minute
)

// Used to record the progress of the table.
type writeSuccessCallback func(lastWrittenPos sorter.Position)

// Used to get an upper bound.
type upperBoundGetter func(tableSinkUpperBoundTs model.Ts) sorter.Position

// Used to abort the task processing of the table.
type isCanceled func() bool

// sinkTask is a task for a table sink.
// It only considers how to control the table sink.
type sinkTask struct {
	span tablepb.Span
	// lowerBound indicates the lower bound of the task.
	// It is a closed interval.
	lowerBound sorter.Position
	// getUpperBound is used to get the upper bound of the task.
	// It is a closed interval.
	// Use a method to get the latest value, because the upper bound may change(only can increase).
	getUpperBound upperBoundGetter
	tableSink     *tableSinkWrapper
	callback      writeSuccessCallback
	isCanceled    isCanceled
}

// redoTask is a task for the redo log.
type redoTask struct {
	span          tablepb.Span
	lowerBound    sorter.Position
	getUpperBound upperBoundGetter
	tableSink     *tableSinkWrapper
	callback      writeSuccessCallback
	isCanceled    isCanceled
}

// The range of a task is limited by:
// 1. maxTaskTimeRange, to avoid one table holding a worker too long time;
// 2. TsWindow, to avoid a task covering more than one TsWindow.
func validateAndAdjustBound(
	changefeedID model.ChangeFeedID,
	span *tablepb.Span,
	lowerBound, upperBound sorter.Position,
	useTsWindow ...sorter.TsWindow,
) (sorter.Position, sorter.Position) {
	lowerPhs := oracle.GetTimeFromTS(lowerBound.CommitTs)
	upperPhs := oracle.GetTimeFromTS(upperBound.CommitTs)
	// The time range of a task should not exceed maxTaskTimeRange.
	// This would help for reduce changefeed latency.
	if upperPhs.Sub(lowerPhs) > maxTaskTimeRange {
		newUpperCommitTs := oracle.GoTimeToTS(lowerPhs.Add(maxTaskTimeRange))
		upperBound = sorter.GenCommitFence(newUpperCommitTs)
	}

	if len(useTsWindow) > 0 {
		// NOTE: if ts window is too small and there are too many tables,
		// the issue may be triggered again: https://github.com/pingcap/tiflow/issues/10169.
		tsWindow1 := useTsWindow[0].ExtractTsWindow(lowerBound.CommitTs)
		tsWindow2 := useTsWindow[0].ExtractTsWindow(upperBound.CommitTs)
		if tsWindow1 != tsWindow2 {
			upperBound = sorter.GenCommitFence(useTsWindow[0].MinTsInWindow(tsWindow1+1) - 1)
		}
	}

	if !upperBound.IsCommitFence() {
		log.Panic("Task upperbound must be a ResolvedTs",
			zap.String("namespace", changefeedID.Namespace),
			zap.String("changefeed", changefeedID.ID),
			zap.Stringer("span", span),
			zap.Any("upperBound", upperBound))
	}
	return lowerBound, upperBound
}
