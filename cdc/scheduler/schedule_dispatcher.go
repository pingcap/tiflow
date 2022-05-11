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
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/scheduler/util"
	"github.com/pingcap/tiflow/pkg/context"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

const (
	// CheckpointCannotProceed is a placeholder indicating that the
	// Owner should not advance the global checkpoint TS just yet.
	CheckpointCannotProceed = model.Ts(0)
)

// ScheduleDispatcher is an interface for a table scheduler used in Owner.
type ScheduleDispatcher interface {
	// Tick is called periodically to update the SchedulerDispatcher on the latest state of replication.
	// This function should NOT be assumed to be thread-safe. No concurrent calls allowed.
	Tick(
		ctx context.Context,
		// Latest global checkpoint of the changefeed
		checkpointTs model.Ts,
		// All tables that SHOULD be replicated (or started) at the current checkpoint.
		currentTables []model.TableID,
		// All captures that are alive according to the latest Etcd states.
		captures map[model.CaptureID]*model.CaptureInfo,
	) (newCheckpointTs, newResolvedTs model.Ts, err error)

	// MoveTable requests that a table be moved to target.
	// It should be thread-safe.
	MoveTable(tableID model.TableID, target model.CaptureID) error

	// Rebalance triggers a rebalance operation.
	// It should be thread-safe
	Rebalance() error

	// DrainCapture remove all tables from the target capture.
	// should only be called when a capture is planning to shut down.
	// It should be thread-safe.
	DrainCapture(target model.CaptureID) (int, error)
}

// ScheduleDispatcherCommunicator is an interface for the BaseScheduleDispatcher to
// send commands to Processors. The owner of a BaseScheduleDispatcher should provide
// an implementation of ScheduleDispatcherCommunicator to supply BaseScheduleDispatcher
// some methods to specify its behavior.
type ScheduleDispatcherCommunicator interface {
	// DispatchTable should send a dispatch command to the Processor.
	DispatchTable(ctx context.Context,
		changeFeedID model.ChangeFeedID,
		tableID model.TableID,
		captureID model.CaptureID,
		isDelete bool,
		epoch model.ProcessorEpoch,
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

// BaseScheduleDispatcher implements the basic logic of a ScheduleDispatcher.
// For it to be directly useful to the Owner, the Owner should implement it own
// ScheduleDispatcherCommunicator.
type BaseScheduleDispatcher struct {
	mu            sync.Mutex
	tables        *util.TableSet                         // information of all actually running tables
	captures      map[model.CaptureID]*model.CaptureInfo // basic information of all captures
	captureStatus map[model.CaptureID]*captureStatus     // more information on the captures
	checkpointTs  model.Ts                               // current checkpoint-ts

	moveTableManager moveTableManager

	balancer           balancer
	balancerCandidates []model.CaptureID

	lastTickCaptureCount int
	needRebalance        bool

	// at most one capture can be drained at any time.
	drainTarget   model.CaptureID
	drainingTable model.TableID

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
) *BaseScheduleDispatcher {
	// logger is just the global logger with the `changefeed-id` field attached.
	logger := log.L().With(
		zap.String("namespace", changeFeedID.Namespace),
		zap.String("changefeed", changeFeedID.ID))

	return &BaseScheduleDispatcher{
		tables:               util.NewTableSet(),
		captureStatus:        map[model.CaptureID]*captureStatus{},
		moveTableManager:     newMoveTableManager(),
		balancer:             newTableNumberRebalancer(logger),
		balancerCandidates:   make([]model.CaptureID, 0),
		changeFeedID:         changeFeedID,
		logger:               logger,
		communicator:         communicator,
		checkpointTs:         checkpointTs,
		lastTickCaptureCount: captureCountUninitialized,
		drainTarget:          captureIDNotDraining,
	}
}

type captureStatus struct {
	// SyncStatus indicates what we know about the capture's internal state.
	// We need to know this before we can make decision whether to
	// dispatch a table.
	SyncStatus captureSyncStatus

	// Epoch is reset when the processor's internal states
	// have been reset.
	Epoch model.ProcessorEpoch

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

func (s *BaseScheduleDispatcher) resetDrainingTarget() {
	if s.drainTarget == captureIDNotDraining {
		return
	}
	// draining target cannot be found in the latest captures, which means it's
	// offline, so reset the `drainTarget`.
	if _, ok := s.captures[s.drainTarget]; !ok {
		log.Info("DrainCapture: Done", zap.String("target", s.drainTarget))
		s.drainTarget = captureIDNotDraining
	}

	if len(s.captures) < 2 {
		log.Info("DrainCapture: Abort, not enough running captures",
			zap.String("target", s.drainTarget))
		s.drainTarget = captureIDNotDraining
	}
}

func (s *BaseScheduleDispatcher) setCaptures(captures map[model.CaptureID]*model.CaptureInfo) {
	// Update the internal capture list with information from the Owner
	// (from Etcd in the current implementation).
	s.captures = captures

	s.resetDrainingTarget()

	s.balancerCandidates = s.balancerCandidates[:0]
	for captureID := range s.captures {
		if captureID != s.drainTarget {
			s.balancerCandidates = append(s.balancerCandidates, captureID)
		}
	}
}

// Tick implements the interface ScheduleDispatcher.
func (s *BaseScheduleDispatcher) Tick(
	ctx context.Context,
	checkpointTs model.Ts,
	// currentTables are tables that SHOULD be running given the current checkpoint-ts.
	// It is maintained by the caller of this function.
	currentTables []model.TableID,
	captures map[model.CaptureID]*model.CaptureInfo,
) (newCheckpointTs, resolvedTs model.Ts, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.setCaptures(captures)

	// We trigger an automatic rebalance if the capture count has changed.
	// This logic is the same as in the older implementation of scheduler.
	// TODO a better criterion is needed.
	// NOTE: We need to check whether the capture count has changed in every tick,
	// and set needRebalance to true if it has. If we miss a capture count change,
	// the workload may never be balanced until user manually triggers a rebalance.
	if s.lastTickCaptureCount != captureCountUninitialized &&
		s.lastTickCaptureCount != len(captures) {
		// when the capture count has changed but still during draining process, do not rebalance.
		if s.drainTarget == captureIDNotDraining {
			s.needRebalance = true
		}
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
		return CheckpointCannotProceed, CheckpointCannotProceed, errors.Trace(err)
	}
	if !done {
		// Returns early if not all captures have synced their states with us.
		// We need to know all captures' status in order to proceed.
		// This is crucial for ensuring that no table is double-scheduled.
		return CheckpointCannotProceed, CheckpointCannotProceed, nil
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
		ok, err := s.addTable(ctx, tableID)
		if err != nil {
			return CheckpointCannotProceed, CheckpointCannotProceed, errors.Trace(err)
		}
		if !ok {
			return CheckpointCannotProceed, CheckpointCannotProceed, nil
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
			return CheckpointCannotProceed, CheckpointCannotProceed, errors.Trace(err)
		}
		if !ok {
			return CheckpointCannotProceed, CheckpointCannotProceed, nil
		}
	}

	checkAllTasksNormal := func() bool {
		normal := s.tables.CountTableByStatus(util.RunningTable) == len(currentTables) &&
			s.tables.CountTableByStatus(util.AddingTable) == 0 &&
			s.tables.CountTableByStatus(util.RemovingTable) == 0
		// when draining capture, tables in the `AddingTable` status,
		// once all tables become `Running`, treat it as the signal that the capture
		// draining process finished.
		if normal && s.drainTarget != captureIDNotDraining {
			log.Info("DrainCapture: Done", zap.String("target", s.drainTarget))
			s.drainTarget = captureIDNotDraining
		}
		return normal
	}
	if !checkAllTasksNormal() {
		return CheckpointCannotProceed, CheckpointCannotProceed, nil
	}

	// handleMoveTableJobs tries to execute user-specified manual move table jobs.
	ok, err := s.handleMoveTableJobs(ctx)
	if err != nil {
		return CheckpointCannotProceed, CheckpointCannotProceed, errors.Trace(err)
	}
	if !ok {
		return CheckpointCannotProceed, CheckpointCannotProceed, nil
	}
	if !checkAllTasksNormal() {
		return CheckpointCannotProceed, CheckpointCannotProceed, nil
	}

	if s.needRebalance {
		ok, err := s.rebalance(ctx)
		if err != nil {
			return CheckpointCannotProceed, CheckpointCannotProceed, errors.Trace(err)
		}
		if !ok {
			return CheckpointCannotProceed, CheckpointCannotProceed, nil
		}
		s.needRebalance = false
	}
	if !checkAllTasksNormal() {
		return CheckpointCannotProceed, CheckpointCannotProceed, nil
	}

	newCheckpointTs, resolvedTs = s.calculateTs()
	return
}

func (s *BaseScheduleDispatcher) calculateTs() (checkpointTs, resolvedTs model.Ts) {
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

func (s *BaseScheduleDispatcher) syncCaptures(ctx context.Context) (capturesAllSynced bool, err error) {
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
func (s *BaseScheduleDispatcher) descheduleTablesFromDownCaptures() {
	for _, captureID := range s.tables.GetDistinctCaptures() {
		// If the capture is not in the current list of captures, it means that
		// the capture has been removed from the system.
		if _, ok := s.captures[captureID]; !ok {
			// Remove records for all table previously replicated by the
			// gone capture.
			removed := s.tables.RemoveTableRecordByCaptureID(captureID)
			s.logger.Info("capture down, removing tables",
				zap.String("captureID", captureID),
				zap.Int("count", len(removed)),
				zap.Any("removedTables", removed))
			s.moveTableManager.OnCaptureRemoved(captureID)
		}
	}
}

func (s *BaseScheduleDispatcher) findDiffTables(
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
	for tableID, _ := range s.tables.GetAllTables() {
		if _, ok := shouldReplicateTables[tableID]; !ok {
			// table is not found in `shouldReplicateTables`.
			toRemove = append(toRemove, tableID)
		}
	}

	if s.drainTarget == captureIDNotDraining {
		return
	}

	if s.drainingTable != 0 {
		record, ok := s.tables.GetTableRecord(s.drainingTable)
		if !ok {
			s.logger.Warn("DrainCapture: draining table not found",
				zap.Int64("tableID", s.drainingTable))
			return
		}
		if record.Status == util.AddingTable || record.Status == util.RemovingTable {
			return
		}
		s.logger.Info("DrainCapture: move table finished",
			zap.Int64("tableID", record.TableID))
		s.drainingTable = 0
		return
	}

	for _, record := range s.tables.GetAllTablesGroupedByCaptures()[s.drainTarget] {
		if record.Status == util.RunningTable {
			toRemove = append(toRemove, record.TableID)
			s.drainingTable = record.TableID
			s.logger.Info("DrainCapture: remove table",
				zap.Int64("tableID", record.TableID))
			break
		}
	}

	return
}

func (s *BaseScheduleDispatcher) addTable(
	ctx context.Context,
	tableID model.TableID,
) (done bool, err error) {
	// A user triggered move-table will have had the target recorded.
	target, ok := s.moveTableManager.GetTargetByTableID(tableID)
	isManualMove := ok
	if !ok {
		target, ok = s.balancer.FindTarget(s.tables, s.balancerCandidates)
		if !ok {
			s.logger.Warn("no active capture")
			return true, nil
		}
	}

	epoch := s.captureStatus[target].Epoch
	ok, err = s.communicator.DispatchTable(
		ctx, s.changeFeedID, tableID, target, false, epoch)
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

func (s *BaseScheduleDispatcher) removeTable(
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
	ok, err = s.communicator.DispatchTable(ctx, s.changeFeedID, tableID, captureID, true, epoch)
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
func (s *BaseScheduleDispatcher) MoveTable(tableID model.TableID, target model.CaptureID) error {
	if s.drainTarget != captureIDNotDraining {
		log.Info("Move Table command has been ignored, "+
			"because one capture is draining",
			zap.Int64("tableID", tableID),
			zap.String("targetCapture", target),
			zap.String("drainingTarget", s.drainTarget))
		return cerror.ErrSchedulerMoveTableNotAllowed.GenWithStack("draining capture")
	}
	if !s.moveTableManager.Add(tableID, target) {
		log.Info("Move Table command has been ignored, "+
			"because the last user triggered move has not finished",
			zap.Int64("tableID", tableID),
			zap.String("targetCapture", target))
	}
	return nil
}

func (s *BaseScheduleDispatcher) handleMoveTableJobs(ctx context.Context) (bool, error) {
	removeAllDone, err := s.moveTableManager.DoRemove(ctx,
		func(ctx context.Context, tableID model.TableID, target model.CaptureID) (removeTableResult, error) {
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
func (s *BaseScheduleDispatcher) Rebalance() error {
	if s.drainTarget != captureIDNotDraining {
		log.Info("Rebalance command has been ignored, "+
			"because one capture is draining",
			zap.String("drainingTarget", s.drainTarget))
		return cerror.ErrSchedulerRebalanceNotAllowed.GenWithStack("draining capture")
	}
	s.needRebalance = true
	return nil
}

func (s *BaseScheduleDispatcher) rebalance(ctx context.Context) (done bool, err error) {
	tablesToRemove := s.balancer.FindVictims(s.tables, s.balancerCandidates)
	for _, record := range tablesToRemove {
		if record.Status != util.RunningTable {
			s.logger.DPanic("unexpected table status",
				zap.Any("tableRecord", record))
		}

		epoch := s.captureStatus[record.CaptureID].Epoch
		// Removes the table from the current capture
		ok, err := s.communicator.DispatchTable(
			ctx, s.changeFeedID, record.TableID, record.CaptureID, true, epoch)
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

// DrainCapture implements the interface ScheduleDispatcher.
func (s *BaseScheduleDispatcher) DrainCapture(target model.CaptureID) (int, error) {
	totalTableCount := s.tables.CountTableByCaptureID(target)
	// target capture has no table, `DrainCapture` become a no-op
	if totalTableCount == 0 {
		log.Info("DrainCapture: target capture has no table",
			zap.String("target", target))
		return 0, nil
	}

	// when draining the capture, tables should be dispatched to other capture except the draining one,
	// so at least should have 2 captures alive. otherwise, reject the request.
	if len(s.captures) < 2 {
		log.Warn("DrainCapture: not allowed since less than 2 captures alive",
			zap.Any("captures", s.captures),
			zap.String("target", target),
			zap.Int("totalTableCount", totalTableCount))
		return totalTableCount, cerror.ErrSchedulerDrainCaptureNotAllowed.GenWithStack("captures not enough")
	}

	// there is table adding to the target capture, drain it is not allowed at the moment.
	// Drain the capture would remove all tables from the target capture, so if there is any
	// at `RemovingTable` status should be acceptable, treat it as a front-runner.
	addingTableCount := s.tables.CountTableByCaptureIDAndStatus(target, util.AddingTable)
	if addingTableCount != 0 {
		log.Warn("DrainCapture: not allowed since some table is adding to the target",
			zap.String("target", target),
			zap.Int("totalTableCount", totalTableCount),
			zap.Int("addingTableCount", addingTableCount))
		return totalTableCount, cerror.ErrSchedulerDrainCaptureNotAllowed.GenWithStack("adding tables")
	}

	// there is at least one `MoveTable` job which target is identical to the draining target not finished yet,
	// a new table will be dispatched to the target capture, before the whole process finished, reject the request.
	if s.moveTableManager.HaveJobsByCaptureID(target) {
		log.Warn("DrainCapture: not allowed since have processing move table jobs",
			zap.String("target", target),
			zap.Int("totalTableCount", totalTableCount))
		return totalTableCount, cerror.ErrSchedulerDrainCaptureNotAllowed.GenWithStack("processing move table jobs")
	}

	if s.drainTarget != captureIDNotDraining {
		log.Warn("DrainCapture: not allowed since other capture is draining now",
			zap.String("drainingCapture", s.drainTarget),
			zap.String("target", target),
			zap.Int("totalTableCount", totalTableCount))
		return totalTableCount, cerror.ErrSchedulerDrainCaptureNotAllowed.GenWithStack("other capture is draining")
	}

	s.drainTarget = target
	// disable rebalance to prevent unnecessary table scheduling.
	s.needRebalance = false
	return totalTableCount, nil
}

// OnAgentFinishedTableOperation is called when a table operation has been finished by
// the processor.
func (s *BaseScheduleDispatcher) OnAgentFinishedTableOperation(
	captureID model.CaptureID,
	tableID model.TableID,
	epoch model.ProcessorEpoch,
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
func (s *BaseScheduleDispatcher) OnAgentSyncTaskStatuses(
	captureID model.CaptureID,
	epoch model.ProcessorEpoch,
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
		s.tables.AddTableRecord(&util.TableRecord{TableID: tableID, CaptureID: captureID, Status: util.AddingTable})
	}
	for _, tableID := range running {
		if record, ok := s.tables.GetTableRecord(tableID); ok {
			logger.Panic("duplicate table tasks",
				zap.Int64("tableID", tableID),
				zap.String("actualCaptureID", record.CaptureID))
		}
		s.tables.AddTableRecord(&util.TableRecord{TableID: tableID, CaptureID: captureID, Status: util.RunningTable})
	}
	for _, tableID := range removing {
		if record, ok := s.tables.GetTableRecord(tableID); ok {
			logger.Panic("duplicate table tasks",
				zap.Int64("tableID", tableID),
				zap.String("actualCaptureID", record.CaptureID))
		}
		s.tables.AddTableRecord(&util.TableRecord{TableID: tableID, CaptureID: captureID, Status: util.RemovingTable})
	}

	status := s.captureStatus[captureID]
	status.SyncStatus = captureSyncFinished
	status.Epoch = epoch
}

// OnAgentCheckpoint is called when the processor sends a checkpoint.
func (s *BaseScheduleDispatcher) OnAgentCheckpoint(captureID model.CaptureID, checkpointTs model.Ts, resolvedTs model.Ts) {
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
