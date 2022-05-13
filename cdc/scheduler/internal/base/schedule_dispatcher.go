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
	"math"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	sched "github.com/pingcap/tiflow/cdc/scheduler/internal"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/base/protocol"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/util"
	"go.uber.org/zap"
)

// ScheduleDispatcherCommunicator is an interface for the BaseScheduleDispatcher to
// send commands to Processors. The owner of a BaseScheduleDispatcher should provide
// an implementation of ScheduleDispatcherCommunicator to supply BaseScheduleDispatcher
// some methods to specify its behavior.
type ScheduleDispatcherCommunicator interface {
	// DispatchTable should send a dispatch command to the Processor.
	DispatchTable(ctx context.Context,
		changeFeedID model.ChangeFeedID,
		tableID model.TableID,
		startTs model.Ts,
		captureID model.CaptureID,
		isDelete bool,
		epoch protocol.ProcessorEpoch,
	) (done bool, err error)

	// Announce announces to the specified capture that the current node has become the Owner.
	Announce(ctx context.Context,
		changeFeedID model.ChangeFeedID,
		captureID model.CaptureID) (done bool, err error)
}

const (
	// captureCountUninitialized is a placeholder for an unknown total capture count.
	captureCountUninitialized = -1
)

// ScheduleDispatcher implements the basic logic of a ScheduleDispatcher.
// For it to be directly useful to the Owner, the Owner should implement it own
// ScheduleDispatcherCommunicator.
type ScheduleDispatcher struct {
	mu sync.Mutex
	// information of all actually running tables
	tables *util.TableSet
	// basic information of all captures
	captures map[model.CaptureID]*model.CaptureInfo
	// more information on the captures
	captureStatus map[model.CaptureID]*captureStatus
	// current checkpoint-ts
	checkpointTs model.Ts

	moveTableManager moveTableManager
	balancer         balancer

	lastTickCaptureCount int
	needRebalance        bool

	// read only fields
	changeFeedID model.ChangeFeedID
	communicator ScheduleDispatcherCommunicator
	logger       *zap.Logger
}

// NewBaseScheduleDispatcher creates a new BaseScheduleDispatcher.
func NewBaseScheduleDispatcher(
	changeFeedID model.ChangeFeedID,
	communicator ScheduleDispatcherCommunicator,
	checkpointTs model.Ts,
) *ScheduleDispatcher {
	// logger is just the global logger with the `changefeed-id` field attached.
	logger := log.L().With(
		zap.String("namespace", changeFeedID.Namespace),
		zap.String("changefeed", changeFeedID.ID))

	return &ScheduleDispatcher{
		tables:               util.NewTableSet(),
		captureStatus:        map[model.CaptureID]*captureStatus{},
		moveTableManager:     newMoveTableManager(),
		balancer:             newTableNumberRebalancer(logger),
		changeFeedID:         changeFeedID,
		logger:               logger,
		communicator:         communicator,
		checkpointTs:         checkpointTs,
		lastTickCaptureCount: captureCountUninitialized,
	}
}

type captureStatus struct {
	// SyncStatus indicates what we know about the capture's internal state.
	// We need to know this before we can make decision whether to
	// dispatch a table.
	SyncStatus captureSyncStatus

	// Epoch is reset when the processor's internal states
	// have been reset.
	Epoch protocol.ProcessorEpoch

	// Watermark fields
	CheckpointTs model.Ts
	ResolvedTs   model.Ts
	// We can add more fields here in the future, such as: RedoLogTs.
}

type captureSyncStatus int32

const (
	// captureUninitialized indicates that we have not sent an `Announce` to the capture yet,
	// nor has it sent us a `Sync` message.
	captureUninitialized = captureSyncStatus(iota) + 1
	// captureSyncSent indicates that we have sent a `Sync` message to the capture, but have had
	// no response yet.
	captureSyncSent
	// captureSyncFinished indicates that the capture has been fully initialized and is ready to
	// accept `DispatchTable` messages.
	captureSyncFinished
)

// Tick implements the interface ScheduleDispatcher.
func (s *ScheduleDispatcher) Tick(
	ctx context.Context,
	checkpointTs model.Ts,
	// currentTables are tables that SHOULD be running given the current checkpoint-ts.
	// It is maintained by the caller of this function.
	currentTables []model.TableID,
	captures map[model.CaptureID]*model.CaptureInfo,
) (newCheckpointTs, resolvedTs model.Ts, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Update the internal capture list with information from the Owner
	// (from Etcd in the current implementation).
	s.captures = captures

	// We trigger an automatic rebalance if the capture count has changed.
	// This logic is the same as in the older implementation of sched.
	// TODO a better criterion is needed.
	// NOTE: We need to check whether the capture count has changed in every tick,
	// and set needRebalance to true if it has. If we miss a capture count change,
	// the workload may never be balanced until user manually triggers a rebalance.
	if s.lastTickCaptureCount != captureCountUninitialized &&
		s.lastTickCaptureCount != len(captures) {

		s.needRebalance = true
	}
	s.lastTickCaptureCount = len(captures)

	// Checks for checkpoint regression as a safety measure.
	if s.checkpointTs > checkpointTs {
		s.logger.Panic("checkpointTs regressed",
			zap.Uint64("old", s.checkpointTs),
			zap.Uint64("new", checkpointTs))
	}
	// Updates the internally maintained last checkpoint-ts.
	s.checkpointTs = checkpointTs

	// Makes sure that captures have all been synchronized before proceeding.
	done, err := s.syncCaptures(ctx)
	if err != nil {
		return sched.CheckpointCannotProceed, sched.CheckpointCannotProceed, errors.Trace(err)
	}
	if !done {
		// Returns early if not all captures have synced their states with us.
		// We need to know all captures' status in order to proceed.
		// This is crucial for ensuring that no table is double-scheduled.
		return sched.CheckpointCannotProceed, sched.CheckpointCannotProceed, nil
	}

	s.descheduleTablesFromDownCaptures()

	shouldReplicateTableSet := make(map[model.TableID]struct{})
	for _, tableID := range currentTables {
		shouldReplicateTableSet[tableID] = struct{}{}
	}

	// findDiffTables compares the tables that should be running and
	// the tables that are actually running.
	// Note: Tables that are being added and removed are considered
	// "running" for the purpose of comparison, and we do not interrupt
	// these operations.
	toAdd, toRemove := s.findDiffTables(shouldReplicateTableSet)

	for _, tableID := range toAdd {
		ok, err := s.addTable(ctx, tableID, checkpointTs)
		if err != nil {
			return sched.CheckpointCannotProceed, sched.CheckpointCannotProceed, errors.Trace(err)
		}
		if !ok {
			return sched.CheckpointCannotProceed, sched.CheckpointCannotProceed, nil
		}
	}

	for _, tableID := range toRemove {
		record, ok := s.tables.GetTableRecord(tableID)
		if !ok {
			s.logger.Panic("table not found", zap.Int64("tableID", tableID))
		}
		if record.Status != util.RunningTable {
			// another operation is in progress
			continue
		}

		ok, err := s.removeTable(ctx, tableID)
		if err != nil {
			return sched.CheckpointCannotProceed, sched.CheckpointCannotProceed, errors.Trace(err)
		}
		if !ok {
			return sched.CheckpointCannotProceed, sched.CheckpointCannotProceed, nil
		}
	}

	checkAllTasksNormal := func() bool {
		return s.tables.CountTableByStatus(util.RunningTable) == len(currentTables) &&
			s.tables.CountTableByStatus(util.AddingTable) == 0 &&
			s.tables.CountTableByStatus(util.RemovingTable) == 0
	}
	if !checkAllTasksNormal() {
		return sched.CheckpointCannotProceed, sched.CheckpointCannotProceed, nil
	}

	// handleMoveTableJobs tries to execute user-specified manual move table jobs.
	ok, err := s.handleMoveTableJobs(ctx)
	if err != nil {
		return sched.CheckpointCannotProceed, sched.CheckpointCannotProceed, errors.Trace(err)
	}
	if !ok {
		return sched.CheckpointCannotProceed, sched.CheckpointCannotProceed, nil
	}
	if !checkAllTasksNormal() {
		return sched.CheckpointCannotProceed, sched.CheckpointCannotProceed, nil
	}

	if s.needRebalance {
		ok, err := s.rebalance(ctx, checkpointTs)
		if err != nil {
			return sched.CheckpointCannotProceed, sched.CheckpointCannotProceed, errors.Trace(err)
		}
		if !ok {
			return sched.CheckpointCannotProceed, sched.CheckpointCannotProceed, nil
		}
		s.needRebalance = false
	}
	if !checkAllTasksNormal() {
		return sched.CheckpointCannotProceed, sched.CheckpointCannotProceed, nil
	}

	newCheckpointTs, resolvedTs = s.calculateTs()
	return
}

func (s *ScheduleDispatcher) calculateTs() (checkpointTs, resolvedTs model.Ts) {
	checkpointTs = math.MaxUint64
	resolvedTs = math.MaxUint64

	for captureID, status := range s.captureStatus {
		if s.tables.CountTableByCaptureID(captureID) == 0 {
			// the checkpoint (as well as resolved-ts) from a capture
			// that is not replicating any table is meaningless.
			log.Debug("skip collecting resolvedTs and checkpointTs of this capture"+
				"because the capture not replicating any table",
				zap.String("captureID", captureID),
			)
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

func (s *ScheduleDispatcher) syncCaptures(ctx context.Context) (capturesAllSynced bool, err error) {
	for captureID := range s.captureStatus {
		if _, ok := s.captures[captureID]; !ok {
			// removes expired captures from the captureSynced map
			delete(s.captureStatus, captureID)
			s.logger.Debug("syncCaptures: remove offline capture",
				zap.String("captureID", captureID))
		}
	}
	for captureID := range s.captures {
		if _, ok := s.captureStatus[captureID]; !ok {
			s.captureStatus[captureID] = &captureStatus{
				SyncStatus:   captureUninitialized,
				CheckpointTs: s.checkpointTs,
				ResolvedTs:   s.checkpointTs,
			}
		}
	}

	finishedCount := 0
	for captureID, status := range s.captureStatus {
		switch status.SyncStatus {
		case captureUninitialized:
			done, err := s.communicator.Announce(ctx, s.changeFeedID, captureID)
			if err != nil {
				return false, errors.Trace(err)
			}
			if done {
				s.captureStatus[captureID].SyncStatus = captureSyncSent
				s.logger.Info("syncCaptures: sent sync request",
					zap.String("captureID", captureID))
			}
		case captureSyncFinished:
			finishedCount++
		case captureSyncSent:
			continue
		default:
			panic("unreachable")
		}
	}

	return finishedCount == len(s.captureStatus), nil
}

// descheduleTablesFromDownCaptures removes tables from `s.tables` that are
// associated with a capture that no longer exists.
// `s.captures` MUST be updated before calling this method.
func (s *ScheduleDispatcher) descheduleTablesFromDownCaptures() {
	for _, captureID := range s.tables.GetDistinctCaptures() {
		// If the capture is not in the current list of captures, it means that
		// the capture has been removed from the system.
		if _, ok := s.captures[captureID]; !ok {
			// Remove records for all table previously replicated by the
			// gone capture.
			removed := s.tables.RemoveTableRecordByCaptureID(captureID)
			s.logger.Info("capture down, removing tables",
				zap.String("captureID", captureID),
				zap.Any("removedTables", removed))
			s.moveTableManager.OnCaptureRemoved(captureID)
		}
	}
}

func (s *ScheduleDispatcher) findDiffTables(
	shouldReplicateTables map[model.TableID]struct{},
) (toAdd, toRemove []model.TableID) {
	// Find tables that need to be added.
	for tableID := range shouldReplicateTables {
		if _, ok := s.tables.GetTableRecord(tableID); !ok {
			// table is not found in `s.tables`.
			toAdd = append(toAdd, tableID)
		}
	}

	// Find tables that need to be removed.
	for tableID := range s.tables.GetAllTables() {
		if _, ok := shouldReplicateTables[tableID]; !ok {
			// table is not found in `shouldReplicateTables`.
			toRemove = append(toRemove, tableID)
		}
	}
	return
}

func (s *ScheduleDispatcher) addTable(
	ctx context.Context,
	tableID model.TableID,
	startTs model.Ts,
) (done bool, err error) {
	// A user triggered move-table will have had the target recorded.
	target, ok := s.moveTableManager.GetTargetByTableID(tableID)
	isManualMove := ok
	if !ok {
		target, ok = s.balancer.FindTarget(s.tables, s.captures)
		if !ok {
			s.logger.Warn("no active capture")
			return true, nil
		}
	}

	epoch := s.captureStatus[target].Epoch
	ok, err = s.communicator.DispatchTable(
		ctx, s.changeFeedID, tableID, startTs, target, false, epoch)
	if err != nil {
		return false, errors.Trace(err)
	}

	if !ok {
		return false, nil
	}
	if isManualMove {
		s.moveTableManager.MarkDone(tableID)
	}

	if ok := s.tables.AddTableRecord(&util.TableRecord{
		TableID:   tableID,
		CaptureID: target,
		Status:    util.AddingTable,
	}); !ok {
		s.logger.Panic("duplicate table", zap.Int64("tableID", tableID))
	}
	return true, nil
}

func (s *ScheduleDispatcher) removeTable(
	ctx context.Context,
	tableID model.TableID,
) (done bool, err error) {
	record, ok := s.tables.GetTableRecord(tableID)
	if !ok {
		s.logger.Panic("table not found", zap.Int64("tableID", tableID))
	}
	// need to delete table
	captureID := record.CaptureID
	epoch := s.captureStatus[captureID].Epoch
	ok, err = s.communicator.DispatchTable(ctx, s.changeFeedID, tableID,
		0, captureID, true, epoch)
	if err != nil {
		return false, errors.Trace(err)
	}
	if !ok {
		return false, nil
	}

	record.Status = util.RemovingTable
	s.tables.UpdateTableRecord(record)
	return true, nil
}

// MoveTable implements the interface SchedulerDispatcher.
func (s *ScheduleDispatcher) MoveTable(tableID model.TableID, target model.CaptureID) {
	if !s.moveTableManager.Add(tableID, target) {
		log.Info("Move Table command has been ignored, because the last user triggered"+
			"move has not finished",
			zap.Int64("tableID", tableID),
			zap.String("targetCapture", target))
	}
}

func (s *ScheduleDispatcher) handleMoveTableJobs(ctx context.Context) (bool, error) {
	removeAllDone, err := s.moveTableManager.DoRemove(ctx,
		func(
			ctx context.Context, tableID model.TableID, target model.CaptureID,
		) (removeTableResult, error) {
			_, ok := s.tables.GetTableRecord(tableID)
			if !ok {
				s.logger.Warn("table does not exist", zap.Int64("tableID", tableID))
				return removeTableResultGiveUp, nil
			}

			if _, ok := s.captures[target]; !ok {
				s.logger.Warn("move table target does not exist",
					zap.Int64("tableID", tableID),
					zap.String("targetCapture", target))
				return removeTableResultGiveUp, nil
			}

			ok, err := s.removeTable(ctx, tableID)
			if err != nil {
				return removeTableResultUnavailable, errors.Trace(err)
			}
			if !ok {
				return removeTableResultUnavailable, nil
			}
			return removeTableResultOK, nil
		},
	)
	if err != nil {
		return false, errors.Trace(err)
	}
	return removeAllDone, nil
}

// Rebalance implements the interface ScheduleDispatcher.
func (s *ScheduleDispatcher) Rebalance() {
	s.needRebalance = true
}

func (s *ScheduleDispatcher) rebalance(
	ctx context.Context, checkpointTs model.Ts,
) (done bool, err error) {
	tablesToRemove := s.balancer.FindVictims(s.tables, s.captures)
	for _, record := range tablesToRemove {
		if record.Status != util.RunningTable {
			s.logger.DPanic("unexpected table status",
				zap.Any("tableRecord", record))
		}

		epoch := s.captureStatus[record.CaptureID].Epoch
		// Removes the table from the current capture
		ok, err := s.communicator.DispatchTable(
			ctx, s.changeFeedID, record.TableID, checkpointTs, record.CaptureID, true, epoch)
		if err != nil {
			return false, errors.Trace(err)
		}
		if !ok {
			return false, nil
		}

		record.Status = util.RemovingTable
		s.tables.UpdateTableRecord(record)
	}
	return true, nil
}

// OnAgentFinishedTableOperation is called when a table operation has been finished by
// the processor.
func (s *ScheduleDispatcher) OnAgentFinishedTableOperation(
	captureID model.CaptureID,
	tableID model.TableID,
	epoch protocol.ProcessorEpoch,
) {
	s.mu.Lock()
	defer s.mu.Unlock()

	logger := s.logger.With(
		zap.String("captureID", captureID),
		zap.Int64("tableID", tableID),
		zap.String("epoch", epoch),
	)

	if _, ok := s.captures[captureID]; !ok {
		logger.Warn("stale message from dead processor, ignore")
		return
	}

	captureSt, ok := s.captureStatus[captureID]
	if !ok {
		logger.Warn("Message from an unknown processor, ignore")
		return
	}

	if captureSt.Epoch != epoch {
		logger.Warn("Processor epoch does not match",
			zap.String("expected", captureSt.Epoch))
		return
	}

	record, ok := s.tables.GetTableRecord(tableID)
	if !ok {
		logger.Warn("response about a stale table, ignore")
		return
	}

	if record.CaptureID != captureID {
		logger.Panic("message from unexpected capture",
			zap.String("expected", record.CaptureID))
	}
	logger.Info("owner received dispatch finished")

	switch record.Status {
	case util.AddingTable:
		record.Status = util.RunningTable
		s.tables.UpdateTableRecord(record)
	case util.RemovingTable:
		if !s.tables.RemoveTableRecord(tableID) {
			logger.Panic("failed to remove table")
		}
	case util.RunningTable:
		logger.Panic("response to invalid dispatch message")
	}
}

// OnAgentSyncTaskStatuses is called when the processor sends its complete current state.
func (s *ScheduleDispatcher) OnAgentSyncTaskStatuses(
	captureID model.CaptureID,
	epoch protocol.ProcessorEpoch,
	running, adding, removing []model.TableID,
) {
	s.mu.Lock()
	defer s.mu.Unlock()

	logger := s.logger.With(zap.String("captureID", captureID))
	logger.Info("scheduler received sync",
		zap.String("captureID", captureID),
		zap.String("epoch", epoch))

	if ce := logger.Check(zap.DebugLevel, "OnAgentSyncTaskStatuses"); ce != nil {
		// Print this information only in debug mode.
		ce.Write(
			zap.Any("running", running),
			zap.Any("adding", adding),
			zap.Any("removing", removing))
	}

	// Clear all tables previously run by the sender capture,
	// because `Sync` tells the Owner to reset its state regarding
	// the sender capture.
	s.tables.RemoveTableRecordByCaptureID(captureID)

	if _, ok := s.captureStatus[captureID]; !ok {
		logger.Warn("received sync from a capture not previously tracked, ignore",
			zap.Any("captureStatus", s.captureStatus))
		return
	}

	for _, tableID := range adding {
		if record, ok := s.tables.GetTableRecord(tableID); ok {
			logger.Panic("duplicate table tasks",
				zap.Int64("tableID", tableID),
				zap.String("actualCaptureID", record.CaptureID))
		}
		s.tables.AddTableRecord(&util.TableRecord{
			TableID: tableID, CaptureID: captureID, Status: util.AddingTable,
		})
	}
	for _, tableID := range running {
		if record, ok := s.tables.GetTableRecord(tableID); ok {
			logger.Panic("duplicate table tasks",
				zap.Int64("tableID", tableID),
				zap.String("actualCaptureID", record.CaptureID))
		}
		s.tables.AddTableRecord(&util.TableRecord{
			TableID: tableID, CaptureID: captureID, Status: util.RunningTable,
		})
	}
	for _, tableID := range removing {
		if record, ok := s.tables.GetTableRecord(tableID); ok {
			logger.Panic("duplicate table tasks",
				zap.Int64("tableID", tableID),
				zap.String("actualCaptureID", record.CaptureID))
		}
		s.tables.AddTableRecord(&util.TableRecord{
			TableID: tableID, CaptureID: captureID, Status: util.RemovingTable,
		})
	}

	status := s.captureStatus[captureID]
	status.SyncStatus = captureSyncFinished
	status.Epoch = epoch
}

// OnAgentCheckpoint is called when the processor sends a checkpoint.
func (s *ScheduleDispatcher) OnAgentCheckpoint(
	captureID model.CaptureID, checkpointTs model.Ts, resolvedTs model.Ts,
) {
	s.mu.Lock()
	defer s.mu.Unlock()

	logger := s.logger.With(zap.String("captureID", captureID),
		zap.Uint64("checkpointTs", checkpointTs),
		zap.Uint64("resolvedTs", resolvedTs))

	status, ok := s.captureStatus[captureID]
	if !ok || status.SyncStatus != captureSyncFinished {
		logger.Warn("received checkpoint from a capture not synced, ignore")
		return
	}

	status.CheckpointTs = checkpointTs
	status.ResolvedTs = resolvedTs
}
