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

package scheduler

import (
	"math"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/scheduler/util"
	"github.com/pingcap/ticdc/pkg/context"
	"go.uber.org/zap"
)

type ScheduleDispatcher interface {
	// Tick is called periodically to update the SchedulerDispatcher on the latest state of replication.
	// This function should NOT be assumed to be thread-safe. No concurrent calls allowed.
	Tick(
		ctx context.Context,
		checkpointTs model.Ts,
		currentTables []model.TableID,
		captures map[model.CaptureID]*model.CaptureInfo,
	) (newCheckpointTs, resolvedTs model.Ts, err error)

	// MoveTable requests that a table be moved to target.
	// It should be thread-safe.
	MoveTable(tableID model.TableID, target model.CaptureID)

	// Rebalance triggers a rebalance operation.
	// It should be thread-safe
	Rebalance()

	// OnAgentFinishedTableOperation is called when an agent has finished processing
	// an operation associated with the table.
	OnAgentFinishedTableOperation(captureID model.CaptureID, tableID model.TableID)

	// OnAgentSyncTaskStatuses is called when an agent sends the schedule dispatcher its current states.
	OnAgentSyncTaskStatuses(captureID model.CaptureID, running, adding, removing []model.TableID)

	// OnAgentCheckpoint is called when an agent sends a checkpoint.
	OnAgentCheckpoint(captureID model.CaptureID, checkpointTs model.Ts, resolvedTs model.Ts)
}

type ScheduleDispatcherCommunicator interface {
	DispatchTable(ctx context.Context,
		changeFeedID model.ChangeFeedID,
		tableID model.TableID,
		captureID model.CaptureID,
		boundaryTs model.Ts,
		isDelete bool) (done bool, err error)

	Announce(ctx context.Context,
		changeFeedID model.ChangeFeedID,
		captureID model.CaptureID) (done bool, err error)
}

const (
	CheckpointCannotProceed = model.Ts(0)
)

type BaseScheduleDispatcher struct {
	mu     sync.Mutex
	tables *util.TableSet
	// captureSynced records whether the capture has sent us the tables
	// it is currently replicating. We need them to tell us their status
	// when we just have succeeded a previous owner.
	captureStatus map[model.CaptureID]*captureStatus
	captures      map[model.CaptureID]*model.CaptureInfo
	checkpointTs  model.Ts

	moveTableJobQueue []*moveTableJob
	moveTableTarget   map[model.TableID]model.CaptureID

	lastTickCaptureCount int
	needRebalance        bool

	changeFeedID model.ChangeFeedID

	logger *zap.Logger

	callbacks ScheduleDispatcherCommunicator
}

func NewScheduleDispatcher(
	changeFeedID model.ChangeFeedID,
	callbacks ScheduleDispatcherCommunicator,
	checkpointTs model.Ts,
) ScheduleDispatcher {
	logger := log.L().With(zap.String("changefeed-id", changeFeedID))
	return &BaseScheduleDispatcher{
		tables:          util.NewTableSet(),
		captureStatus:   map[model.CaptureID]*captureStatus{},
		moveTableTarget: map[model.TableID]model.CaptureID{},
		changeFeedID:    changeFeedID,
		logger:          logger,
		callbacks:       callbacks,
		checkpointTs:    checkpointTs,
	}
}

const (
	addingTable = util.TableStatus(iota)
	removingTable
	runningTable
)

type captureStatus struct {
	SyncStatus   captureSyncStatus
	CheckpointTs model.Ts
	ResolvedTs   model.Ts
}

type captureSyncStatus int32

const (
	captureUninitialized = captureSyncStatus(iota)
	captureSyncSent
	captureSyncFinished
)

type moveTableJob struct {
	tableID model.TableID
	target  model.CaptureID
}

type tableRecord struct {
	Capture model.CaptureID
	Status  util.TableStatus
}

func (s *BaseScheduleDispatcher) Tick(
	ctx context.Context,
	checkpointTs model.Ts,
	currentTables []model.TableID,
	captures map[model.CaptureID]*model.CaptureInfo,
) (newCheckpointTs, resolvedTs model.Ts, err error) {
	s.captures = captures
	if s.checkpointTs > checkpointTs {
		s.logger.Panic("checkpointTs regressed",
			zap.Uint64("old", s.checkpointTs),
			zap.Uint64("new", checkpointTs))
	}
	s.checkpointTs = checkpointTs

	done, err := s.syncCaptures(ctx)
	if err != nil {
		return CheckpointCannotProceed, CheckpointCannotProceed, errors.Trace(err)
	}
	if !done {
		// returns early if not all captures have synced their states with us.
		// We need to know all captures' status in order to proceed.
		return CheckpointCannotProceed, CheckpointCannotProceed, nil
	}

	if s.needRebalance {
		ok, err := s.rebalance(ctx)
		if err != nil {
			return CheckpointCannotProceed, CheckpointCannotProceed, errors.Trace(err)
		}
		if ok {
			s.needRebalance = false
		}
	}

	for _, captureID := range s.tables.GetDistinctCaptures() {
		if _, ok := s.captures[captureID]; !ok {
			s.logger.Info("capture down, removing tables",
				zap.String("capture-id", captureID))
			s.tables.RemoveTableRecordByCaptureID(captureID)
		}
	}

	shouldReplicateTableSet := make(map[model.TableID]struct{})
	for _, tableID := range currentTables {
		shouldReplicateTableSet[tableID] = struct{}{}
	}

	for tableID := range shouldReplicateTableSet {
		if _, ok := s.tables.GetTableRecord(tableID); ok {
			// table is found
			continue
		}
		// table not found
		target, ok := s.moveTableTarget[tableID]
		if !ok {
			target, ok = s.findTargetCapture()
			if !ok {
				s.logger.Warn("no active capture")
				return CheckpointCannotProceed, CheckpointCannotProceed, nil
			}
		}

		ok, err = s.callbacks.DispatchTable(ctx, s.changeFeedID, tableID, target, s.checkpointTs, false)
		if err != nil {
			return CheckpointCannotProceed, CheckpointCannotProceed, errors.Trace(err)
		}

		if !ok {
			s.logger.Warn("dispatching table failed, will try again",
				zap.String("target", target),
				zap.Int64("table-id", tableID))
			return CheckpointCannotProceed, CheckpointCannotProceed, nil
		}

		if ok := s.tables.AddTableRecord(&util.TableRecord{
			TableID:   tableID,
			CaptureID: target,
			Status:    addingTable,
		}); !ok {
			s.logger.Panic("duplicate table", zap.Int64("table-id", tableID))
		}
	}

	for tableID, record := range s.tables.GetAllTables() {
		if _, ok := shouldReplicateTableSet[tableID]; ok {
			continue
		}
		if record.Status != runningTable {
			// another operation is in progress
			continue
		}

		// need to delete table
		captureID := record.CaptureID
		ok, err := s.callbacks.DispatchTable(ctx, s.changeFeedID, tableID, captureID, s.checkpointTs, true)
		if err != nil {
			return CheckpointCannotProceed, CheckpointCannotProceed, errors.Trace(err)
		}
		if !ok {
			s.logger.Warn("removing table failed, will try again",
				zap.String("target", captureID),
				zap.Int64("table-id", tableID))
			return CheckpointCannotProceed, CheckpointCannotProceed, nil
		}
		record.Status = removingTable
	}

	if err := s.handleMoveTableJobs(ctx); err != nil {
		return CheckpointCannotProceed, CheckpointCannotProceed, errors.Trace(err)
	}

	if !s.needRebalance && s.lastTickCaptureCount != len(captures) {
		s.needRebalance = true
	}

	runningCount := 0
	for _, record := range s.tables.GetAllTables() {
		if record.Status == runningTable {
			runningCount++
		}
	}

	allTasksNormal := runningCount == len(currentTables)
	s.lastTickCaptureCount = len(captures)

	if !allTasksNormal {
		// TODO add detailed log
		s.logger.Info("scheduler has pending jobs")
		return CheckpointCannotProceed, CheckpointCannotProceed, nil
	}

	newCheckpointTs, resolvedTs = s.calculateTs()
	s.logger.Debug("scheduler checkpoint updated",
		zap.Uint64("checkpoint-ts", newCheckpointTs),
		zap.Uint64("resolved-ts", resolvedTs),
		zap.Any("capture-status", s.captureStatus))
	return
}

func (s *BaseScheduleDispatcher) calculateTs() (checkpointTs, resolvedTs model.Ts) {
	checkpointTs = math.MaxUint64
	resolvedTs = math.MaxUint64

	for captureID, status := range s.captureStatus {
		if s.tables.CountTableByCaptureID(captureID) == 0 {
			// the checkpoint (as well as resolved-ts) from a capture
			// that is not replicating any table is meaningless.
			continue
		}
		if status.ResolvedTs < resolvedTs {
			resolvedTs = status.ResolvedTs
		}
		if status.CheckpointTs < checkpointTs {
			checkpointTs = status.CheckpointTs
		}
	}
	return
}

func (s *BaseScheduleDispatcher) syncCaptures(ctx context.Context) (bool, error) {
	for captureID := range s.captureStatus {
		if _, ok := s.captures[captureID]; !ok {
			// removes expired captures from the captureSynced map
			delete(s.captureStatus, captureID)
			s.logger.Debug("syncCaptures: remove offline capture",
				zap.String("capture-id", captureID))
		}
	}
	for captureID := range s.captures {
		if _, ok := s.captureStatus[captureID]; !ok {
			s.captureStatus[captureID] = &captureStatus{
				SyncStatus:   captureUninitialized,
				CheckpointTs: s.checkpointTs,
				ResolvedTs:   s.checkpointTs,
			}
			s.logger.Debug("syncCaptures: set capture status uninitialized",
				zap.String("capture-id", captureID))
		}
	}

	finishedCount := 0
	for captureID, status := range s.captureStatus {
		switch status.SyncStatus {
		case captureUninitialized:
			done, err := s.callbacks.Announce(ctx, s.changeFeedID, captureID)
			if err != nil {
				return false, errors.Trace(err)
			}
			if done {
				s.captureStatus[captureID].SyncStatus = captureSyncSent
				s.logger.Debug("syncCaptures: sent sync request",
					zap.String("capture-id", captureID))
			}
		case captureSyncFinished:
			finishedCount++
		default:
			continue
		}
	}

	return finishedCount == len(s.captureStatus), nil
}

func (s *BaseScheduleDispatcher) MoveTable(tableID model.TableID, target model.CaptureID) {
	s.moveTableJobQueue = append(s.moveTableJobQueue, &moveTableJob{
		tableID: tableID,
		target:  target,
	})
}

func (s *BaseScheduleDispatcher) handleMoveTableJobs(ctx context.Context) error {
	for len(s.moveTableJobQueue) > 0 {
		job := s.moveTableJobQueue[0]

		record, ok := s.tables.GetTableRecord(job.tableID)
		if !ok {
			s.logger.Warn("table to be move does not exist", zap.Any("job", job))
			s.moveTableJobQueue = s.moveTableJobQueue[1:]
			continue
		}

		if _, ok := s.captures[record.CaptureID]; !ok {
			s.logger.Warn("tried to move table to a non-existent capture", zap.Any("job", job))
			s.moveTableJobQueue = s.moveTableJobQueue[1:]
			continue
		}

		if job.target == record.CaptureID {
			s.logger.Info("try to move table to its current capture, doing nothing", zap.Any("job", job))
			s.moveTableJobQueue = s.moveTableJobQueue[1:]
			continue
		}

		// Records the target so that when we redispatch the table,
		// it goes to the desired capture.
		s.moveTableTarget[job.tableID] = job.target

		// Removes the table from the current capture
		ok, err := s.callbacks.DispatchTable(ctx, s.changeFeedID, job.tableID, record.CaptureID, s.checkpointTs, true)
		if err != nil {
			return errors.Trace(err)
		}
		if !ok {
			s.logger.Warn("dispatching table failed, will try again",
				zap.String("target", record.CaptureID),
				zap.Int64("table-id", job.tableID))
			return nil
		}

		record.Status = removingTable
		s.moveTableJobQueue = s.moveTableJobQueue[1:]
	}

	return nil
}

func (s *BaseScheduleDispatcher) Rebalance() {
	s.needRebalance = true
}

func (s *BaseScheduleDispatcher) rebalance(ctx context.Context) (done bool, err error) {
	totalTableNum := len(s.tables.GetAllTables())
	captureNum := len(s.captures)
	upperLimitPerCapture := int(math.Ceil(float64(totalTableNum) / float64(captureNum)))

	s.logger.Info("Start rebalancing",
		zap.Int("table-num", totalTableNum),
		zap.Int("capture-num", captureNum),
		zap.Int("target-limit", upperLimitPerCapture))

	for captureID, tables := range s.tables.GetAllTablesGroupedByCaptures() {
		tableNum2Remove := len(tables) - upperLimitPerCapture
		if tableNum2Remove <= 0 {
			continue
		}

		// here we pick `tableNum2Remove` tables to delete,
		// and then the removed tables will be dispatched by `Tick` function in the next tick
		for tableID, record := range tables {
			if tableNum2Remove <= 0 {
				break
			}

			ok, err := s.callbacks.DispatchTable(ctx, s.changeFeedID, tableID, captureID, s.checkpointTs, true)
			if err != nil {
				return false, errors.Trace(err)
			}
			if !ok {
				s.logger.Warn("removing table failed, will try again",
					zap.String("target", captureID),
					zap.Int64("table-id", tableID))
				return false, nil
			}

			if record.Status != runningTable {
				s.logger.DPanic("unexpected table status", zap.Any("table-record", record))
			}

			s.logger.Info("Rebalance: Move table",
				zap.Int64("table-id", tableID),
				zap.String("capture", captureID))
			record.Status = removingTable
			tableNum2Remove--
		}
	}
	return true, nil
}

func (s *BaseScheduleDispatcher) findTargetCapture() (model.CaptureID, bool) {
	if len(s.captures) == 0 {
		return "", false
	}

	captureWorkload := make(map[model.CaptureID]int)
	for captureID := range s.captures {
		captureWorkload[captureID] = 0
	}

	for _, record := range s.tables.GetAllTables() {
		captureWorkload[record.CaptureID]++
	}

	candidate := ""
	minWorkload := math.MaxInt64

	for captureID, workload := range captureWorkload {
		if workload < minWorkload {
			minWorkload = workload
			candidate = captureID
		}
	}

	if minWorkload == math.MaxInt64 {
		s.logger.Panic("unexpected minWorkerload == math.MaxInt64")
	}

	return candidate, true
}

func (s *BaseScheduleDispatcher) OnAgentFinishedTableOperation(captureID model.CaptureID, tableID model.TableID) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.captures[captureID]; !ok {
		s.logger.Warn("stale message from dead processor, ignore",
			zap.String("capture-id", captureID),
			zap.Int64("tableID", tableID))
	}

	record, ok := s.tables.GetTableRecord(tableID)
	if !ok {
		s.logger.Warn("response about a stale table, ignore",
			zap.String("source", captureID),
			zap.Int64("table-id", tableID))
	}

	if record.CaptureID != captureID {
		s.logger.Panic("message from unexpected capture",
			zap.String("expected", record.CaptureID),
			zap.String("actual", captureID),
			zap.Int64("tableID", tableID))
	}
	s.logger.Info("owner received dispatch finished",
		zap.String("capture", captureID),
		zap.Int64("table-id", tableID))

	switch record.Status {
	case addingTable:
		record.Status = runningTable
		delete(s.moveTableTarget, tableID)
	case removingTable:
		s.tables.RemoveTableRecord(tableID)
	case runningTable:
		s.logger.Panic("response to invalid dispatch message",
			zap.String("source", captureID),
			zap.Int64("table-id", tableID))
	}
}

func (s *BaseScheduleDispatcher) OnAgentSyncTaskStatuses(captureID model.CaptureID, running, adding, removing []model.TableID) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.logger.Debug("scheduler received sync",
		zap.String("capture-id", captureID),
		zap.Any("running", running),
		zap.Any("adding", adding),
		zap.Any("removing", removing))

	s.tables.RemoveTableRecordByCaptureID(captureID)

	for _, tableID := range adding {
		if record, ok := s.tables.GetTableRecord(tableID); ok {
			s.logger.Panic("duplicate table tasks",
				zap.Int64("table-id", tableID),
				zap.String("capture-id-1", record.CaptureID),
				zap.String("capture-id-2", captureID))
		}
		s.tables.AddTableRecord(&util.TableRecord{CaptureID: captureID, Status: addingTable})
	}
	for _, tableID := range running {
		if record, ok := s.tables.GetTableRecord(tableID); ok {
			s.logger.Panic("duplicate table tasks",
				zap.Int64("table-id", tableID),
				zap.String("capture-id-1", record.CaptureID),
				zap.String("capture-id-2", captureID))
		}
		s.tables.AddTableRecord(&util.TableRecord{CaptureID: captureID, Status: runningTable})
	}
	for _, tableID := range removing {
		if record, ok := s.tables.GetTableRecord(tableID); ok {
			s.logger.Panic("duplicate table tasks",
				zap.Int64("table-id", tableID),
				zap.String("capture-id-1", record.CaptureID),
				zap.String("capture-id-2", captureID))
		}
		s.tables.AddTableRecord(&util.TableRecord{CaptureID: captureID, Status: removingTable})
	}

	if _, ok := s.captureStatus[captureID]; !ok {
		s.captureStatus[captureID] = &captureStatus{
			CheckpointTs: s.checkpointTs,
			ResolvedTs:   s.checkpointTs,
		}
		s.logger.Debug("capture status updated during sync",
			zap.Any("capture-status", s.captureStatus))
	}
	s.captureStatus[captureID].SyncStatus = captureSyncFinished
}

func (s *BaseScheduleDispatcher) OnAgentCheckpoint(captureID model.CaptureID, checkpointTs model.Ts, resolvedTs model.Ts) {
	s.mu.Lock()
	defer s.mu.Unlock()

	status, ok := s.captureStatus[captureID]
	if !ok {
		s.logger.Info("received checkpoint from non-existing capture, ignore",
			zap.String("capture-id", captureID),
			zap.Uint64("checkpoint-ts", checkpointTs),
			zap.Uint64("resolved-ts", resolvedTs))
		return
	}

	if status.SyncStatus != captureSyncFinished {
		s.logger.Warn("received checkpoint from a capture not synced, ignore",
			zap.String("capture-id", captureID),
			zap.Uint64("checkpoint-ts", checkpointTs),
			zap.Uint64("resolved-ts", resolvedTs))
	}

	status.CheckpointTs = checkpointTs
	status.ResolvedTs = resolvedTs
	s.logger.Debug("checkpoint saved",
		zap.String("capture-id", captureID),
		zap.Uint64("checkpoint-ts", checkpointTs),
		zap.Uint64("resolved-ts", resolvedTs))
}
