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

package owner

import (
	"math"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/context"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/p2p"
	"go.uber.org/zap"
)

type schedulerV2 struct {
	state         *model.ChangefeedReactorState
	messageServer *p2p.MessageServer
	messageRouter p2p.MessageRouter

	mu                sync.Mutex
	tableToCaptureMap map[model.TableID]*tableRecord

	moveTableJobQueue []*moveTableJob
	moveTableTarget   map[model.TableID]model.CaptureID

	changefeedID model.ChangeFeedID
}

type tableStatus = int32

const (
	addingTable = tableStatus(iota)
	removingTable
	runningTable
)

type tableRecord struct {
	capture model.CaptureID
	status  tableStatus
}

func NewSchedulerV2(ctx context.Context, changefeedID model.ChangeFeedID) *schedulerV2 {
	ret := &schedulerV2{
		messageServer:     ctx.GlobalVars().MessageServer,
		messageRouter:     ctx.GlobalVars().MessageRouter,
		tableToCaptureMap: map[model.TableID]*tableRecord{},
		changefeedID:      changefeedID,
	}

	doneCh, _, err := ret.messageServer.AddHandler(ctx,
		string(model.DispatchTableResponseTopic(changefeedID)),
		&model.DispatchTableResponseMessage{}, func(senderID string, data interface{}) error {
			captureID := senderID
			message := data.(*model.DispatchTableResponseMessage)

			ret.mu.Lock()
			defer ret.mu.Unlock()

			record, ok := ret.tableToCaptureMap[message.ID]
			if !ok {
				log.Panic("response to invalid dispatch message",
					zap.String("changefeed-id", changefeedID),
					zap.String("source", captureID),
					zap.Int64("table-id", message.ID))
			}

			switch record.status {
			case addingTable:
				record.status = runningTable
				delete(ret.moveTableTarget, message.ID)
			case removingTable:
				delete(ret.tableToCaptureMap, message.ID)
			case runningTable:
				log.Panic("response to invalid dispatch message",
					zap.String("changefeed-id", changefeedID),
					zap.String("source", captureID),
					zap.Int64("table-id", message.ID))
			}

			return nil
		})
	if err != nil {
		return nil
	}

	select {
	case <-ctx.Done():
		return nil
	case <-doneCh:
	}

	return ret
}

func (s *schedulerV2) Close(ctx context.Context) error {
	doneCh, err := s.messageServer.RemoveHandler(ctx, string(model.DispatchTableResponseTopic(s.changefeedID)))
	if err != nil {
		return errors.Trace(err)
	}
	select {
	case <-ctx.Done():
		return errors.Trace(err)
	case <-doneCh:
	}

	return nil
}

func (s *schedulerV2) Tick(
	ctx context.Context,
	state *model.ChangefeedReactorState,
	currentTables []model.TableID) (shouldUpdateState bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.state = state

	shouldReplicateTableSet := make(map[model.TableID]struct{})
	for _, tableID := range currentTables {
		shouldReplicateTableSet[tableID] = struct{}{}
	}

	for tableID := range shouldReplicateTableSet {
		_, ok := s.tableToCaptureMap[tableID]
		if ok {
			continue
		}
		// table not found
		target, ok := s.moveTableTarget[tableID]
		if !ok {
			target, ok = s.findTargetCapture()
			if !ok {
				log.Warn("no active capture", zap.String("changefeed-id", s.changefeedID))
				return false, nil
			}
		}

		ok, err = s.dispatchTable(ctx, tableID, target, false)
		if err != nil {
			return false, errors.Trace(err)
		}

		if !ok {
			log.Warn("dispatching table failed, client congested, will try again",
				zap.String("changefeed-id", s.changefeedID),
				zap.String("target", target),
				zap.Int64("table-id", tableID))
			return false, nil
		}

		s.tableToCaptureMap[tableID] = &tableRecord{
			capture: target,
			status:  addingTable,
		}
	}

	for tableID, record := range s.tableToCaptureMap {
		if _, ok := shouldReplicateTableSet[tableID]; ok {
			continue
		}
		if record.status != runningTable {
			// another operation is in progress
			continue
		}

		// need to delete table
		captureID := record.capture
		ok, err := s.dispatchTable(ctx, tableID, captureID, true)
		if err != nil {
			return false, errors.Trace(err)
		}

		if !ok {
			log.Warn("removing table failed, client congested, will try again",
				zap.String("changefeed-id", s.changefeedID),
				zap.String("target", captureID),
				zap.Int64("table-id", tableID))
			return false, nil
		}
		record.status = removingTable
	}

	if err := s.handleMoveTableJobs(ctx); err != nil {
		return false, errors.Trace(err)
	}

	hasPendingJob := false
	for _, record := range s.tableToCaptureMap {
		if record.status != runningTable {
			hasPendingJob = true
			break
		}
	}

	shouldUpdateState = !hasPendingJob
	return
}

func (s *schedulerV2) MoveTable(tableID model.TableID, target model.CaptureID) {
	s.moveTableJobQueue = append(s.moveTableJobQueue, &moveTableJob{
		tableID: tableID,
		target:  target,
	})
}

func (s *schedulerV2) handleMoveTableJobs(ctx context.Context) error {
	for len(s.moveTableJobQueue) > 0 {
		job := s.moveTableJobQueue[0]

		record, ok := s.tableToCaptureMap[job.tableID]
		if !ok {
			log.Warn("table to be move does not exist", zap.Any("job", job))
			s.moveTableJobQueue = s.moveTableJobQueue[1:]
			continue
		}

		if !s.state.Active(job.target) {
			log.Warn("tried to move table to a non-existent capture", zap.Any("job", job))
			s.moveTableJobQueue = s.moveTableJobQueue[1:]
			continue
		}

		if job.target == record.capture {
			log.Info("try to move table to its current capture, doing nothing", zap.Any("job", job))
			s.moveTableJobQueue = s.moveTableJobQueue[1:]
			continue
		}

		// Records the target so that when we redispatch the table,
		// it goes to the desired capture.
		s.moveTableTarget[job.tableID] = job.target

		// Removes the table from the current capture
		ok, err := s.dispatchTable(ctx, job.tableID, record.capture, true)
		if err != nil {
			return errors.Trace(err)
		}
		if !ok {
			log.Warn("dispatching table failed, client congested, will try again",
				zap.String("changefeed-id", s.changefeedID),
				zap.String("target", record.capture),
				zap.Int64("table-id", job.tableID))
			return nil
		}

		record.status = removingTable
		s.moveTableJobQueue = s.moveTableJobQueue[1:]
	}

	return nil
}

func (s *schedulerV2) Rebalance() {
	// TODO
}

func (s *schedulerV2) dispatchTable(ctx context.Context, tableID model.TableID, target model.CaptureID, isDelete bool) (bool, error) {
	client := s.messageRouter.GetClient(p2p.SenderID(target))
	if client == nil {
		log.Warn("No gRPC client to send to capture", zap.String("capture-id", target))
		return false, nil
	}

	_, err := client.TrySendMessage(ctx, model.DispatchTableTopic(s.changefeedID), &model.DispatchTableMessage{
		OwnerRev:   ctx.GlobalVars().OwnerRev,
		ID:         tableID,
		IsDelete:   isDelete,
		BoundaryTs: s.state.Status.CheckpointTs,
	})
	if err != nil {
		if cerrors.ErrPeerMessageSendTryAgain.Equal(err) {
			return false, nil
		}
		return false, errors.Trace(err)
	}

	return true, nil
}

func (s *schedulerV2) findTargetCapture() (model.CaptureID, bool) {
	if len(s.tableToCaptureMap) == 0 {
		return "", false
	}

	captureWorkload := make(map[model.CaptureID]int)
	for _, record := range s.tableToCaptureMap {
		captureWorkload[record.capture]++
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
		log.Panic("unexpected minWorkerload == math.MaxInt64")
	}

	return candidate, true
}
