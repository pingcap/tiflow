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
	"sync"
	"sync/atomic"

	cerrors "github.com/pingcap/ticdc/pkg/errors"

	"github.com/edwingeng/deque"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/context"
	"go.uber.org/zap"
)

type Agent interface {
	// Tick is called periodically by the processor to query all unfinished operations.
	// The returned map should not be modified by the caller.
	Tick(ctx context.Context) error

	// OnOwnerDispatchedTask is called when the processor node receives a table operation
	// from the owner.
	OnOwnerDispatchedTask(
		ownerCaptureID model.CaptureID,
		ownerRev int64,
		tableID model.TableID,
		boundaryTs model.Ts,
		isDelete bool)

	// OnOwnerAnnounce is called when the processor node receives an owner request to send
	// all its statuses, usually when a new owner has just come up.
	OnOwnerAnnounce(
		ownerCaptureID model.CaptureID,
		ownerRev int64,
	)

	CanUpdateCheckpoint(ctx context.Context) bool
}

type TableExecutor interface {
	AddTable(ctx context.Context, tableID model.TableID, boundaryTs model.Ts) error
	RemoveTable(ctx context.Context, tableID model.TableID, boundaryTs model.Ts) (bool, error)
	IsAddTableFinished(ctx context.Context, tableID model.TableID) (bool, error)
	IsRemoveTableFinished(ctx context.Context, tableID model.TableID) (bool, error)

	GetAllCurrentTables() []model.TableID
}

// ProcessorMessenger implements how messages should be sent to the owner,
// and should be able to know whether there are any messages not yet acknowledged
// by the owner.
type ProcessorMessenger interface {
	// FinishTableOperation notifies the owner that a table operation has finished.
	FinishTableOperation(ctx context.Context, tableID model.TableID) (bool, error)
	SyncTaskStatuses(ctx context.Context, running, adding, removing []model.TableID) (bool, error)
	SendReset(ctx context.Context) (bool, error)
	Barrier(ctx context.Context) (done bool)
	OnOwnerChanged(ctx context.Context, newOwnerCaptureID model.CaptureID)
	Close() error
}

type BaseAgent struct {
	agentMu               sync.Mutex
	pendingOps            deque.Deque // stores *AgentOperation
	hasOwnerRequestedSync bool

	tableOperations map[model.TableID]*AgentOperation

	logger *zap.Logger

	callbacks TableExecutor

	ownerInfoMu sync.RWMutex
	ownerInfo   *ownerInfo

	communicator          ProcessorMessenger
	needResetCommunicator int32

	isInitialized bool
}

func NewBaseAgent(
	changeFeedID model.ChangeFeedID,
	executor TableExecutor,
	messenger ProcessorMessenger,
) Agent {
	logger := log.L().With(zap.String("changefeed-id", changeFeedID))
	return &BaseAgent{
		pendingOps:      deque.NewDeque(),
		tableOperations: map[model.TableID]*AgentOperation{},
		logger:          logger,
		callbacks:       executor,
		ownerInfo:       &ownerInfo{},
		communicator:    messenger,
	}
}

type AgentOperation struct {
	TableID    model.TableID
	BoundaryTs model.Ts
	IsDelete   bool

	// for internal use by scheduler
	processed bool
	finished  bool
}

type ownerInfo struct {
	OwnerCaptureID model.CaptureID
	// OwnerRev is needed in order to know who is the latest owner,
	// whenever there is a possibility of confusion, usually when the
	// old owner has just gone down but its gRPC messages have yet to be
	// processed. Since messages from the old and new owner could interleave,
	// we need a way to tell.
	OwnerRev int64
}

func (a *BaseAgent) Tick(ctx context.Context) error {
	if atomic.SwapInt32(&a.needResetCommunicator, 0) == 1 {
		a.communicator.OnOwnerChanged(ctx, a.currentOwner())
	}

	var opsToApply []*AgentOperation

	a.agentMu.Lock()
	if a.hasOwnerRequestedSync {
		var adding, removing, running []model.TableID
		for _, op := range a.tableOperations {
			if !op.IsDelete {
				adding = append(adding, op.TableID)
			} else {
				removing = append(removing, op.TableID)
			}
		}
		for _, tableID := range a.callbacks.GetAllCurrentTables() {
			if _, ok := a.tableOperations[tableID]; !ok {
				continue
			}
			running = append(running, tableID)
		}

		done, err := a.communicator.SyncTaskStatuses(ctx, running, adding, removing)
		if err != nil {
			a.agentMu.Unlock()
			return errors.Trace(err)
		}
		if !done {
			a.agentMu.Unlock()
			return nil
		}
		a.hasOwnerRequestedSync = false
	}

	for !a.pendingOps.Empty() {
		opsBatch := a.pendingOps.PopManyFront(128)
		for _, op := range opsBatch {
			opsToApply = append(opsToApply, op.(*AgentOperation))
		}
	}
	a.agentMu.Unlock()

	for _, op := range opsToApply {
		if _, ok := a.tableOperations[op.TableID]; ok {
			a.logger.DPanic("duplicate operation", zap.Any("op", op))
			return cerrors.ErrProcessorDuplicateOperations.GenWithStackByArgs(op.TableID)
		}
		a.tableOperations[op.TableID] = op
	}

	for _, op := range a.tableOperations {
		if !op.processed {
			if !op.IsDelete {
				// add table
				if err := a.callbacks.AddTable(ctx, op.TableID, op.BoundaryTs); err != nil {
					return errors.Trace(err)
				}
				op.processed = true
			} else {
				// delete table
				done, err := a.callbacks.RemoveTable(ctx, op.TableID, op.BoundaryTs)
				if err != nil {
					return errors.Trace(err)
				}
				op.processed = done
			}
		}
		if op.processed && !op.finished {
			var (
				done bool
				err  error
			)
			if !op.IsDelete {
				done, err = a.callbacks.IsAddTableFinished(ctx, op.TableID)
			} else {
				done, err = a.callbacks.IsRemoveTableFinished(ctx, op.TableID)
			}
			if err != nil {
				return errors.Trace(err)
			}
			op.finished = done
		}
		if op.processed && op.finished {
			done, err := a.communicator.FinishTableOperation(ctx, op.TableID)
			if err != nil {
				return errors.Trace(err)
			}
			if done {
				delete(a.tableOperations, op.TableID)
			}
		}
	}
	return nil
}

func (a *BaseAgent) OnOwnerDispatchedTask(
	ownerCaptureID model.CaptureID,
	ownerRev int64,
	tableID model.TableID,
	boundaryTs model.Ts,
	isDelete bool,
) {
	if !a.checkOwnerInfo(ownerCaptureID, ownerRev) {
		a.logger.Info("task from stale owner ignored",
			zap.Int64("table-id", tableID),
			zap.Uint64("boundary-ts", boundaryTs),
			zap.Bool("is-delete", isDelete))
		return
	}

	a.agentMu.Lock()
	defer a.agentMu.Unlock()

	op := &AgentOperation{
		TableID:    tableID,
		BoundaryTs: boundaryTs,
		IsDelete:   isDelete,
	}
	a.pendingOps.PushBack(op)

	a.logger.Debug("OnOwnerDispatchedTask",
		zap.String("owner-capture-id", ownerCaptureID),
		zap.Int64("owner-rev", ownerRev),
		zap.Any("op", op))
}

func (a *BaseAgent) OnOwnerAnnounce(
	ownerCaptureID model.CaptureID,
	ownerRev int64,
) {
	if !a.checkOwnerInfo(ownerCaptureID, ownerRev) {
		a.logger.Info("sync request from stale owner ignored")
		return
	}

	a.agentMu.Lock()
	defer a.agentMu.Unlock()

	a.hasOwnerRequestedSync = true

	a.logger.Debug("OnOwnerAnnounce",
		zap.String("owner-capture-id", ownerCaptureID),
		zap.Int64("owner-rev", ownerRev))
}

func (a *BaseAgent) CanUpdateCheckpoint(ctx context.Context) bool {
	if atomic.SwapInt32(&a.needResetCommunicator, 0) == 1 {
		a.communicator.OnOwnerChanged(ctx, a.currentOwner())
	}

	a.agentMu.Lock()
	defer a.agentMu.Unlock()

	if a.hasOwnerRequestedSync {
		return false
	}
	return a.communicator.Barrier(ctx)
}

// checkOwnerInfo tries to update the stored ownerInfo, and returns false if the
// arguments are stale, in which case the incoming message should be ignored since
// it has come from an owner that for sure is dead.
func (a *BaseAgent) checkOwnerInfo(ownerCaptureID model.CaptureID, ownerRev int64) bool {
	a.ownerInfoMu.Lock()
	defer a.ownerInfoMu.Unlock()

	if a.ownerInfo.OwnerRev < ownerRev {
		// the stored ownerInfo is stale, we update it
		a.ownerInfo.OwnerRev = ownerRev
		a.ownerInfo.OwnerCaptureID = ownerCaptureID
		// the communicator need to be rebuilt asynchronously and lazily
		// to avoid having to lock every time we need to send a message.
		atomic.StoreInt32(&a.needResetCommunicator, 1)
		a.logger.Info("owner updated",
			zap.Any("new-owner-info", a.ownerInfo))
		return true
	}
	if a.ownerInfo.OwnerRev > ownerRev {
		// the owner where the message just came from is stale.
		a.logger.Info("message received from stale owner",
			zap.Any("old-owner", ownerInfo{
				OwnerCaptureID: ownerCaptureID,
				OwnerRev:       ownerRev,
			}),
			zap.Any("current-owner", a.ownerInfo))
		return false
	}
	if a.ownerInfo.OwnerCaptureID != ownerCaptureID {
		a.logger.Panic("owner IDs do not match",
			zap.String("expected", a.ownerInfo.OwnerCaptureID),
			zap.String("actual", ownerCaptureID))
	}
	return true
}

func (a *BaseAgent) currentOwner() model.CaptureID {
	a.ownerInfoMu.RLock()
	defer a.ownerInfoMu.RUnlock()

	return a.ownerInfo.OwnerCaptureID
}
