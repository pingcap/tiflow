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
	state    *model.ChangefeedReactorState
	captures map[model.CaptureID]*model.CaptureInfo

	messageServer *p2p.MessageServer
	messageRouter p2p.MessageRouter

	mu                sync.Mutex
	tableToCaptureMap map[model.TableID]*tableRecord

	moveTableJobQueue []*moveTableJob
	moveTableTarget   map[model.TableID]model.CaptureID

	lastTickCaptureCount int
	needRebalance bool

	changefeedID model.ChangeFeedID
}

type tableStatus = int32

const (
	addingTable = tableStatus(iota)
	removingTable
	runningTable
)

type tableRecord struct {
	Capture model.CaptureID
	Status  tableStatus
}

// NewSchedulerV2 creates a new schedulerV2
// TODO return an error
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

			log.Info("owner received dispatch finished",
				zap.String("capture", captureID),
				zap.Int64("table-id", message.ID))
			switch record.Status {
			case addingTable:
				record.Status = runningTable
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
		log.Error("Failed to add handler", zap.Error(err))
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
	currentTables []model.TableID,
	captures map[model.CaptureID]*model.CaptureInfo) (shouldUpdateState bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.state = state
	s.captures = captures

	if s.needRebalance {
		ok, err := s.rebalance(ctx)
		if err != nil {
			return false, errors.Trace(err)
		}
		if ok {
			s.needRebalance = false
		}
	}


	for tableID, record := range s.tableToCaptureMap {
		if _, ok := s.captures[record.Capture]; !ok {
			log.Info("capture down, redispatching table",
				zap.String("capture-id", record.Capture),
				zap.Int64("table-id", tableID))
			delete(s.tableToCaptureMap, tableID)
		}
	}

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
			Capture: target,
			Status:  addingTable,
		}
	}

	for tableID, record := range s.tableToCaptureMap {
		if _, ok := shouldReplicateTableSet[tableID]; ok {
			continue
		}
		if record.Status != runningTable {
			// another operation is in progress
			continue
		}

		// need to delete table
		captureID := record.Capture
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
		record.Status = removingTable
	}

	if err := s.handleMoveTableJobs(ctx); err != nil {
		return false, errors.Trace(err)
	}

	if !s.needRebalance && s.lastTickCaptureCount != len(captures) {
		s.needRebalance = true
	}

	hasPendingJob := false
	for _, record := range s.tableToCaptureMap {
		if record.Status != runningTable {
			hasPendingJob = true
			break
		}
	}

	if hasPendingJob {
		log.Debug("scheduler has pending jobs", zap.Any("jobs", s.tableToCaptureMap))
	}

	s.lastTickCaptureCount = len(captures)
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

		if _, ok := s.captures[record.Capture]; !ok {
			log.Warn("tried to move table to a non-existent capture", zap.Any("job", job))
			s.moveTableJobQueue = s.moveTableJobQueue[1:]
			continue
		}

		if job.target == record.Capture {
			log.Info("try to move table to its current capture, doing nothing", zap.Any("job", job))
			s.moveTableJobQueue = s.moveTableJobQueue[1:]
			continue
		}

		// Records the target so that when we redispatch the table,
		// it goes to the desired capture.
		s.moveTableTarget[job.tableID] = job.target

		// Removes the table from the current capture
		ok, err := s.dispatchTable(ctx, job.tableID, record.Capture, true)
		if err != nil {
			return errors.Trace(err)
		}
		if !ok {
			log.Warn("dispatching table failed, client congested, will try again",
				zap.String("changefeed-id", s.changefeedID),
				zap.String("target", record.Capture),
				zap.Int64("table-id", job.tableID))
			return nil
		}

		record.Status = removingTable
		s.moveTableJobQueue = s.moveTableJobQueue[1:]
	}

	return nil
}

func (s *schedulerV2) Rebalance() {
	s.needRebalance = true
}

func (s *schedulerV2) rebalance(ctx context.Context) (bool, error) {
	totalTableNum := len(s.tableToCaptureMap)
	captureNum := len(s.captures)
	upperLimitPerCapture := int(math.Ceil(float64(totalTableNum) / float64(captureNum)))

	log.Info("Start rebalancing",
		zap.String("changefeed", s.state.ID),
		zap.Int("table-num", totalTableNum),
		zap.Int("capture-num", captureNum),
		zap.Int("target-limit", upperLimitPerCapture))

	captureToTablesMap := make(map[model.CaptureID][]model.TableID)
	for tableID, record := range s.tableToCaptureMap {
		captureToTablesMap[record.Capture] = append(captureToTablesMap[record.Capture], tableID)
	}

	for captureID, tableIDs := range captureToTablesMap {
		tableNum2Remove := len(tableIDs) - upperLimitPerCapture
		if tableNum2Remove <= 0 {
			continue
		}

		// here we pick `tableNum2Remove` tables to delete,
		// and then the removed tables will be dispatched by `Tick` function in the next tick
		for _, tableID := range tableIDs {
			tableID := tableID
			if tableNum2Remove <= 0 {
				break
			}

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

			log.Info("Rebalance: Move table",
				zap.Int64("table-id", tableID),
				zap.String("capture", captureID),
				zap.String("changefeed-id", s.state.ID))

			s.tableToCaptureMap[tableID].Status = removingTable

			tableNum2Remove--
		}
	}
	return true, nil
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
	if len(s.captures) == 0 {
		return "", false
	}

	captureWorkload := make(map[model.CaptureID]int)
	for captureID := range s.captures {
		captureWorkload[captureID] = 0
	}

	for _, record := range s.tableToCaptureMap {
		captureWorkload[record.Capture]++
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
