// Copyright 2020 PingCAP, Inc.
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

package main

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"go.uber.org/zap"
)

type cdcMonitReactor struct {
	state *cdcReactorState
}

func (r *cdcMonitReactor) Tick(_ context.Context, state orchestrator.ReactorState) (orchestrator.ReactorState, error) {
	r.state = state.(*cdcReactorState)

	err := r.verifyTs()
	if err != nil {
		log.Error("Verifying Ts failed", zap.Error(err))
		return r.state, err
	}

	err = r.verifyStartTs()
	if err != nil {
		log.Error("Verifying startTs failed", zap.Error(err))
		return r.state, err
	}

	return r.state, nil
}

func (r *cdcMonitReactor) verifyTs() error {
	for changfeedID, positions := range r.state.TaskPositions {
		status, ok := r.state.ChangefeedStatuses[changfeedID]
		if !ok {
			log.Warn("changefeed status not found", zap.String("cfid", changfeedID))
			return nil
		}

		actualCheckpointTs := status.CheckpointTs

		for captureID, position := range positions {
			if _, ok := r.state.Captures[captureID]; !ok {
				// ignore positions whose capture is no longer present
				continue
			}

			if position.CheckPointTs < actualCheckpointTs {
				return errors.Errorf("checkpointTs too large, globalCkpt = %d, localCkpt = %d, capture = %s, cfid = %s",
					actualCheckpointTs, position.CheckPointTs, captureID, changfeedID)
			}
		}
	}

	return nil
}

func (r *cdcMonitReactor) verifyStartTs() error {
	for changfeedID, statuses := range r.state.TaskStatuses {
		cStatus, ok := r.state.ChangefeedStatuses[changfeedID]
		if !ok {
			log.Warn("changefeed status not found", zap.String("cfid", changfeedID))
			return nil
		}

		actualCheckpointTs := cStatus.CheckpointTs

		for captureID, status := range statuses {
			for tableID, operation := range status.Operation {
				if operation.Status != model.OperFinished && !operation.Delete {
					startTs := status.Tables[tableID].StartTs
					if startTs < actualCheckpointTs {
						return errors.Errorf("startTs too small, globalCkpt = %d, startTs = %d, table = %d, capture = %s, cfid = %s",
							actualCheckpointTs, startTs, tableID, captureID, changfeedID)
					}
				}
			}
		}
	}

	return nil
}
