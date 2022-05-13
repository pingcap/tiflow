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
	"time"

	"github.com/edwingeng/deque"
	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/scheduler/internal"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/base/protocol"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/util"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	"github.com/uber-go/atomic"
	"go.uber.org/zap"
)

// ProcessorMessenger implements how messages should be sent to the owner,
// and should be able to know whether there are any messages not yet acknowledged
// by the owner.
type ProcessorMessenger interface {
	// FinishTableOperation notifies the owner that a table operation has finished.
	FinishTableOperation(
		ctx context.Context, tableID model.TableID, epoch protocol.ProcessorEpoch,
	) (done bool, err error)
	// SyncTaskStatuses informs the owner of the processor's current internal state.
	SyncTaskStatuses(
		ctx context.Context, epoch protocol.ProcessorEpoch,
		adding, removing, running []model.TableID,
	) (done bool, err error)
	// SendCheckpoint sends the owner the processor's local watermarks,
	// i.e., checkpoint-ts and resolved-ts.
	SendCheckpoint(
		ctx context.Context, checkpointTs model.Ts, resolvedTs model.Ts,
	) (done bool, err error)
	// Barrier returns whether there is a pending message not yet acknowledged by the owner.
	Barrier(ctx context.Context) (done bool)
	// OnOwnerChanged is called when the owner is changed.
	OnOwnerChanged(
		ctx context.Context, newOwnerCaptureID model.CaptureID, newOwnerRevision int64,
	)
	// Close closes the messenger and does the necessary cleanup.
	Close() error
}

// AgentConfig stores configurations for BaseAgent
type AgentConfig struct {
	// SendCheckpointTsInterval is the interval to send checkpoint-ts to the owner.
	SendCheckpointTsInterval time.Duration
}

// Agent is an implementation of Agent.
// It implements the basic logic and is useful only if the Processor
// implements its own TableExecutor and ProcessorMessenger.
type Agent struct {
	executor     internal.TableExecutor
	communicator ProcessorMessenger

	epochMu sync.RWMutex
	// epoch is reset on each Sync message.
	epoch protocol.ProcessorEpoch

	// pendingOpsMu protects pendingOps.
	// Note that we need a mutex because some methods are expected
	// to be called from a message handler goroutine.
	pendingOpsMu sync.Mutex
	// pendingOps is a queue of operations yet to be processed.
	// the Deque stores *agentOperation.
	pendingOps deque.Deque

	// tableOperations is a map from tableID to the operation
	// that is currently being processed.
	tableOperations map[model.TableID]*agentOperation

	// needSyncNow indicates that the agent needs to send the
	// current owner a sync message as soon as possible.
	needSyncNow *atomic.Bool

	// checkpointSender is used to send checkpoint-ts to the owner.
	checkpointSender checkpointSender

	ownerInfoMu sync.RWMutex
	ownerInfo   *ownerInfo

	// ownerHasChanged indicates that the owner has changed and
	// the communicator needs to be reset.
	ownerHasChanged *atomic.Bool

	// read-only fields
	config *AgentConfig
	logger *zap.Logger
}

// NewBaseAgent creates a new BaseAgent.
func NewBaseAgent(
	changeFeedID model.ChangeFeedID,
	executor internal.TableExecutor,
	messenger ProcessorMessenger,
	config *AgentConfig,
) *Agent {
	logger := log.L().With(
		zap.String("namespace", changeFeedID.Namespace),
		zap.String("changefeed", changeFeedID.ID))
	ret := &Agent{
		pendingOps:       deque.NewDeque(),
		tableOperations:  map[model.TableID]*agentOperation{},
		logger:           logger,
		executor:         executor,
		ownerInfo:        &ownerInfo{},
		communicator:     messenger,
		needSyncNow:      atomic.NewBool(true),
		checkpointSender: newCheckpointSender(messenger, logger, config.SendCheckpointTsInterval),
		ownerHasChanged:  atomic.NewBool(false),
		config:           config,
	}
	ret.resetEpoch()
	return ret
}

type agentOperationStatus int32

const (
	operationReceived = agentOperationStatus(iota + 1)
	operationProcessed
	operationFinished
)

type agentOperation struct {
	TableID  model.TableID
	StartTs  model.Ts
	IsDelete bool
	Epoch    protocol.ProcessorEpoch

	// FromOwnerID is for debugging purposesFromOwnerID
	FromOwnerID model.CaptureID

	status agentOperationStatus
}

type ownerInfo struct {
	OwnerCaptureID model.CaptureID
	// OwnerRev is needed in order to know who is the latest owner
	// whenever there is a possibility of confusion, usually when the
	// old owner has just gone down but its gRPC messages have yet to be
	// processed. Since messages from the old and new owner could interleave,
	// we need a way to tell.
	OwnerRev int64
}

// Tick implements the interface Agent.
func (a *Agent) Tick(ctx context.Context) error {
	if a.ownerHasChanged.Swap(false) {
		// We need to notify the communicator if the owner has changed.
		// This is necessary because the communicator might be waiting for
		// messages to be received by the previous owner.
		ownerID, ownerRev := a.currentOwner()
		a.communicator.OnOwnerChanged(ctx, ownerID, ownerRev)
	}

	if a.needSyncNow.Load() {
		a.resetEpoch()
		done, err := a.sendSync(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		if !done {
			// We need to send a sync successfully before proceeding.
			return nil
		}
		a.needSyncNow.Store(false)
	}

	// We send checkpoints only after a required Sync to make the protocol
	// easier to reason about.
	if err := a.sendCheckpoint(ctx); err != nil {
		return errors.Trace(err)
	}

	opsToApply := a.popPendingOps()
	for _, op := range opsToApply {
		if op.Epoch != a.getEpoch() {
			a.logger.Info("dispatch request epoch does not match",
				zap.String("epoch", op.Epoch),
				zap.String("expectedEpoch", a.getEpoch()))
			continue
		}
		if _, ok := a.tableOperations[op.TableID]; ok {
			a.logger.DPanic("duplicate operation", zap.Any("op", op))
			return cerrors.ErrProcessorDuplicateOperations.GenWithStackByArgs(op.TableID)
		}
		a.tableOperations[op.TableID] = op
	}

	if err := a.processOperations(ctx); err != nil {
		return errors.Trace(err)
	}

	return nil
}

// GetLastSentCheckpointTs implements the interface Agent.
func (a *Agent) GetLastSentCheckpointTs() model.Ts {
	return a.checkpointSender.LastSentCheckpointTs()
}

func (a *Agent) popPendingOps() (opsToApply []*agentOperation) {
	a.pendingOpsMu.Lock()
	defer a.pendingOpsMu.Unlock()

	for !a.pendingOps.Empty() {
		opsBatch := a.pendingOps.PopManyFront(128 /* batch size */)
		for _, op := range opsBatch {
			opsToApply = append(opsToApply, op.(*agentOperation))
		}
	}
	return
}

// sendSync needs to be called with a.pendingOpsMu held.
func (a *Agent) sendSync(ctx context.Context) (bool, error) {
	var adding, removing, running []model.TableID
	for _, op := range a.tableOperations {
		if !op.IsDelete {
			adding = append(adding, op.TableID)
		} else {
			removing = append(removing, op.TableID)
		}
	}
	for _, tableID := range a.executor.GetAllCurrentTables() {
		if _, ok := a.tableOperations[tableID]; ok {
			// Tables with a pending operation is not in the Running state.
			continue
		}
		running = append(running, tableID)
	}

	// We are sorting these so that there content can be predictable in tests.
	// TODO try to find a better way.
	util.SortTableIDs(running)
	util.SortTableIDs(adding)
	util.SortTableIDs(removing)
	done, err := a.communicator.SyncTaskStatuses(ctx, a.getEpoch(), adding, removing, running)
	if err != nil {
		return false, errors.Trace(err)
	}
	return done, nil
}

// processOperations tries to make progress on each pending table operations.
// It queries the executor for the current status of each table.
func (a *Agent) processOperations(ctx context.Context) error {
	for tableID, op := range a.tableOperations {
		switch op.status {
		case operationReceived:
			a.logger.Info("Agent start processing operation", zap.Any("op", op))
			if !op.IsDelete {
				// add table
				done, err := a.executor.AddTable(ctx, op.TableID, op.StartTs)
				if err != nil {
					return errors.Trace(err)
				}
				if !done {
					break
				}
			} else {
				// delete table
				done, err := a.executor.RemoveTable(ctx, op.TableID)
				if err != nil {
					return errors.Trace(err)
				}
				if !done {
					break
				}
			}
			op.status = operationProcessed
			fallthrough
		case operationProcessed:
			var done bool
			if !op.IsDelete {
				done = a.executor.IsAddTableFinished(ctx, op.TableID)
			} else {
				done = a.executor.IsRemoveTableFinished(ctx, op.TableID)
			}
			if !done {
				break
			}
			op.status = operationFinished
			fallthrough
		case operationFinished:
			a.logger.Info("Agent finish processing operation", zap.Any("op", op))
			done, err := a.communicator.FinishTableOperation(ctx, op.TableID, a.getEpoch())
			if err != nil {
				return errors.Trace(err)
			}
			if done {
				delete(a.tableOperations, tableID)
			}
		}
	}
	return nil
}

func (a *Agent) sendCheckpoint(ctx context.Context) error {
	checkpointProvider := func() (checkpointTs, resolvedTs model.Ts, ok bool) {
		// We cannot have a meaningful checkpoint for a processor running NO table.
		if len(a.executor.GetAllCurrentTables()) == 0 {
			a.logger.Debug("no table is running, skip sending checkpoint")
			return 0, 0, false // false indicates no available checkpoint
		}
		checkpointTs, resolvedTs = a.executor.GetCheckpoint()
		ok = true
		return
	}

	if err := a.checkpointSender.SendCheckpoint(ctx, checkpointProvider); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// OnOwnerDispatchedTask should be called when the Owner sent a new dispatched task.
// The Processor is responsible for calling this function when appropriate.
func (a *Agent) OnOwnerDispatchedTask(
	ownerCaptureID model.CaptureID,
	ownerRev int64,
	tableID model.TableID,
	startTs model.Ts,
	isDelete bool,
	epoch protocol.ProcessorEpoch,
) {
	if !a.updateOwnerInfo(ownerCaptureID, ownerRev) {
		a.logger.Info("task from stale owner ignored",
			zap.Int64("tableID", tableID),
			zap.Bool("isDelete", isDelete))
		return
	}

	a.pendingOpsMu.Lock()
	defer a.pendingOpsMu.Unlock()

	op := &agentOperation{
		TableID:     tableID,
		StartTs:     startTs,
		IsDelete:    isDelete,
		Epoch:       epoch,
		FromOwnerID: ownerCaptureID,
		status:      operationReceived,
	}
	a.pendingOps.PushBack(op)

	a.logger.Info("OnOwnerDispatchedTask",
		zap.String("ownerCaptureID", ownerCaptureID),
		zap.Int64("ownerRev", ownerRev),
		zap.Any("op", op))
}

// OnOwnerAnnounce should be called when a new Owner announces its ownership.
// The Processor is responsible for calling this function when appropriate.
//
// ownerRev is the revision number generated by the election mechanism to
// indicate the order in which owners are elected.
func (a *Agent) OnOwnerAnnounce(
	ownerCaptureID model.CaptureID,
	ownerRev int64,
) {
	if !a.updateOwnerInfo(ownerCaptureID, ownerRev) {
		a.logger.Info("sync request from stale owner ignored",
			zap.String("ownerCaptureID", ownerCaptureID),
			zap.Int64("ownerRev", ownerRev))
		return
	}

	// Sets the needSyncNow flag so that in the next tick,
	// we will try to send a Sync to the Owner.
	a.needSyncNow.Store(true)

	a.logger.Info("OnOwnerAnnounce",
		zap.String("ownerCaptureID", ownerCaptureID),
		zap.Int64("ownerRev", ownerRev))
}

// updateOwnerInfo tries to update the stored ownerInfo, and returns false if the
// owner is stale, in which case the incoming message should be ignored since
// it has come from an owner that for sure is dead.
//
// ownerCaptureID: the incoming owner's capture ID
// ownerRev: the incoming owner's revision as generated by Etcd election.
func (a *Agent) updateOwnerInfo(ownerCaptureID model.CaptureID, ownerRev int64) bool {
	a.ownerInfoMu.Lock()
	defer a.ownerInfoMu.Unlock()

	if a.ownerInfo.OwnerRev < ownerRev {
		// the stored ownerInfo is stale, we update it
		a.ownerInfo.OwnerRev = ownerRev
		a.ownerInfo.OwnerCaptureID = ownerCaptureID

		// We set a flag to indicate that the owner has changed.
		// This flag is needed so that the communicator can be reset in time.
		// It is difficult to reset the communicator here, because this function
		// is called in a separate goroutine (possibly in the message handler),
		// so blocking it for lock will increase the risk of deadlock.
		a.ownerHasChanged.Store(true)

		a.logger.Info("owner updated",
			zap.Any("newOwner", a.ownerInfo))

		// Resets the deque so that pending operations from the previous owner
		// will not be processed.
		// Note: these pending operations have not yet been processed by the agent,
		// so it is okay to lose them.
		a.pendingOpsMu.Lock()
		a.pendingOps = deque.NewDeque()
		a.pendingOpsMu.Unlock()
		return true
	}
	if a.ownerInfo.OwnerRev > ownerRev {
		// the owner where the message just came from is stale.
		a.logger.Info("message received from stale owner",
			zap.Any("oldOwner", ownerInfo{
				OwnerCaptureID: ownerCaptureID,
				OwnerRev:       ownerRev,
			}),
			zap.Any("currentOwner", a.ownerInfo))

		// Returning false indicates that we should reject the owner,
		// because it is stale.
		return false
	}
	if a.ownerInfo.OwnerCaptureID != ownerCaptureID {
		// This panic will happen only if two messages have been received
		// with the same ownerRev but with different ownerIDs.
		// This should never happen unless the election via Etcd is buggy.
		a.logger.Panic("owner IDs do not match",
			zap.String("expected", a.ownerInfo.OwnerCaptureID),
			zap.String("actual", ownerCaptureID))
	}
	return true
}

func (a *Agent) currentOwner() (model.CaptureID, int64 /* revision */) {
	a.ownerInfoMu.RLock()
	defer a.ownerInfoMu.RUnlock()

	return a.ownerInfo.OwnerCaptureID, a.ownerInfo.OwnerRev
}

func (a *Agent) resetEpoch() {
	a.epochMu.Lock()
	defer a.epochMu.Unlock()

	// We are using UUIDs because we only need uniqueness guarantee for the epoch,
	// BUT NOT ordering guarantees. The reason is that the Sync messages are themselves
	// barriers, so there is no need to accommodate messages from future epochs.
	a.epoch = uuid.New().String()
}

func (a *Agent) getEpoch() protocol.ProcessorEpoch {
	a.epochMu.RLock()
	defer a.epochMu.RUnlock()

	return a.epoch
}

// CurrentEpoch is a public function used in unit tests for
// checking epoch-related invariants.
func (a *Agent) CurrentEpoch() protocol.ProcessorEpoch {
	return a.getEpoch()
}
