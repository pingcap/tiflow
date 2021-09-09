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
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/p2p"
	"go.uber.org/zap"
)

type schedulerV2 struct {
	state         *model.ChangefeedReactorState
	messageServer *p2p.MessageServer
	messageRouter p2p.MessageRouter

	tableToCaptureMap map[model.TableID]*tableRecord

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

func (s *schedulerV2) Tick(state *model.ChangefeedReactorState, currentTables []model.TableID, captures map[model.CaptureID]*model.CaptureInfo) error {
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
		// if s.dispatchTable(tableID, "", false)
	}
	return nil
}

func (s *schedulerV2) MoveTable(tableID model.TableID, target model.CaptureID) {
}

func (s *schedulerV2) Rebalance() {
}

func (s *schedulerV2) dispatchTable(tableID model.TableID, target model.CaptureID, isDelete bool) (bool, error) {
	client := s.messageRouter.GetClient(p2p.SenderID(target))
	if client == nil {
		log.Warn("No gRPC client to send to capture", zap.String("capture-id", target))
		return false, nil
	}

	_, err := client.TrySendMessage(context.Background(), model.DispatchTableTopic(s.changefeedID), &model.DispatchTableMessage {
		ID: tableID,
		IsDelete: isDelete,
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
	// TODO
	return "", false
}
