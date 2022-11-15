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
	"context"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/engine"
)

// Used to record the progress of the table.
type writeSuccessCallback func(lastWrittenPos engine.Position)

// Used to get an upper bound.
type upperBoundGetter func() engine.Position

// Used to abort the task processing of the table.
type isCanceled func() bool

// sinkTask is a task for a table sink.
// It only considers how to control the table sink.
type sinkTask struct {
	tableID model.TableID
	// lowerBound indicates the lower bound of the task.
	// It is a closed interval.
	lowerBound engine.Position
	// getUpperBound is used to get the upper bound of the task.
	// It is a closed interval.
	// Use a method to get the latest value, because the upper bound may change(only can increase).
	getUpperBound upperBoundGetter
	tableSink     *tableSinkWrapper
	callback      writeSuccessCallback
	isCanceled    isCanceled
}

type sinkWorker interface {
	// Pull data from source manager for the table sink.
	// We suppose that the worker only handle one task at a time.
	handleTasks(ctx context.Context, taskChan <-chan *sinkTask) error
}

type redoTask struct {
	tableID       model.TableID
	lowerBound    engine.Position
	getUpperBound upperBoundGetter
	tableSink     *tableSinkWrapper
	callback      writeSuccessCallback
}

type redoWorker interface {
	// Pull data from source manager for the table sink.
	// We suppose that the worker only handle one task at a time.
	handleTasks(ctx context.Context, taskChan <-chan *redoTask) error
}
