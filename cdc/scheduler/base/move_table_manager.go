// Copyright 2021 PingCAP, Inc.
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

package base

import (
	"context"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/model"
)

// Design Notes:
//
// This file contains the definition and implementation of the move table manager,
// which is responsible for implementing the logic for manual table moves. The logic
// here will ultimately be triggered by the user's call to the move table HTTP API.
//
// POST /api/v1/changefeeds/{changefeed_id}/tables/move_table
//
// Abstracting out moveTableManager makes it easier to both test the implementation
// and modify the behavior of this API.
//
// The moveTableManager will help the ScheduleDispatcher to track which tables are being
// moved to which capture.

// removeTableFunc is a function used to de-schedule a table from its current processor.
type removeTableFunc = func(
	ctx context.Context,
	tableID model.TableID,
	target model.CaptureID) (result removeTableResult, err error)

type removeTableResult int

const (
	// removeTableResultOK indicates that the table has been de-scheduled
	removeTableResultOK removeTableResult = iota + 1

	// removeTableResultUnavailable indicates that the table
	// is temporarily not available for removal. The operation
	// can be tried again later.
	removeTableResultUnavailable

	// removeTableResultGiveUp indicates that the operation is
	// not successful but there is no point in trying again. Such as when
	// 1) the table to be removed is not found,
	// 2) the capture to move the table to is not found.
	removeTableResultGiveUp
)

type moveTableManager interface {
	// Add adds a table to the move table manager.
	// It returns false **if the table is already being moved manually**.
	Add(tableID model.TableID, target model.CaptureID) (ok bool)

	// DoRemove tries to de-schedule as many tables as possible by using the
	// given function fn. If the function fn returns false, it means the table
	// can not be removed (de-scheduled) for now.
	DoRemove(ctx context.Context, fn removeTableFunc) (ok bool, err error)

	// GetTargetByTableID returns the target capture ID of the given table.
	// It will only return a target if the table is in the process of being manually
	// moved, and the request to de-schedule the given table has already been sent.
	GetTargetByTableID(tableID model.TableID) (target model.CaptureID, ok bool)

	// MarkDone informs the moveTableManager that the given table has successfully
	// been moved.
	MarkDone(tableID model.TableID)

	// OnCaptureRemoved informs the moveTableManager that a capture has gone offline.
	// Then the moveTableManager will clear all pending jobs to that capture.
	OnCaptureRemoved(captureID model.CaptureID)
}

type moveTableJobStatus int

const (
	moveTableJobStatusReceived = moveTableJobStatus(iota + 1)
	moveTableJobStatusRemoved
)

type moveTableJob struct {
	target model.CaptureID
	status moveTableJobStatus
}

type moveTableManagerImpl struct {
	mu            sync.Mutex
	moveTableJobs map[model.TableID]*moveTableJob
}

func newMoveTableManager() moveTableManager {
	return &moveTableManagerImpl{
		moveTableJobs: make(map[model.TableID]*moveTableJob),
	}
}

func (m *moveTableManagerImpl) Add(tableID model.TableID, target model.CaptureID) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.moveTableJobs[tableID]; ok {
		// Returns false if the table is already in a move table job.
		return false
	}

	m.moveTableJobs[tableID] = &moveTableJob{
		target: target,
		status: moveTableJobStatusReceived,
	}
	return true
}

func (m *moveTableManagerImpl) DoRemove(ctx context.Context, fn removeTableFunc) (ok bool, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// This function tries to remove as many tables as possible.
	// But when we cannot proceed (i.e., fn returns false), we return false,
	// so that the caller can retry later.

	for tableID, job := range m.moveTableJobs {
		if job.status == moveTableJobStatusRemoved {
			continue
		}

		result, err := fn(ctx, tableID, job.target)
		if err != nil {
			return false, errors.Trace(err)
		}

		switch result {
		case removeTableResultOK:
			job.status = moveTableJobStatusRemoved
			continue
		case removeTableResultGiveUp:
			delete(m.moveTableJobs, tableID)
			// Giving up means that we can move forward,
			// so there is no need to return false here.
			continue
		case removeTableResultUnavailable:
		}

		// Returning false means that there is a table that cannot be removed for now.
		// This is usually caused by temporary unavailability of underlying resources, such
		// as a congestion in the messaging client.
		//
		// So when we have returned false, the caller should try again later and refrain from
		// other scheduling operations.
		return false, nil
	}
	return true, nil
}

func (m *moveTableManagerImpl) GetTargetByTableID(tableID model.TableID) (model.CaptureID, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	job, ok := m.moveTableJobs[tableID]
	if !ok {
		return "", false
	}

	// Only after the table has been removed by the moveTableManager,
	// can we provide the target. Otherwise, we risk interfering with
	// other operations.
	if job.status != moveTableJobStatusRemoved {
		return "", false
	}

	return job.target, true
}

func (m *moveTableManagerImpl) MarkDone(tableID model.TableID) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.moveTableJobs, tableID)
}

func (m *moveTableManagerImpl) OnCaptureRemoved(captureID model.CaptureID) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for tableID, job := range m.moveTableJobs {
		if job.target == captureID {
			delete(m.moveTableJobs, tableID)
		}
	}
}
