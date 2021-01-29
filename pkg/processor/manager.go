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

package processor

import (
	"context"

	"github.com/pingcap/log"
	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc/model"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/orchestrator"
	"github.com/pingcap/ticdc/pkg/security"
	pd "github.com/tikv/pd/client"
)

// Manager is a manager of processor, which maintains the state and behavior of processors
type Manager struct {
	Processors map[model.ChangeFeedID]*processor

	pdCli       pd.Client
	credential  *security.Credential
	captureInfo *model.CaptureInfo
}

// NewManager creates a new processor manager
func NewManager(pdCli pd.Client, credential *security.Credential, captureInfo *model.CaptureInfo) *Manager {
	return &Manager{
		Processors:  make(map[model.ChangeFeedID]*processor),
		pdCli:       pdCli,
		credential:  credential,
		captureInfo: captureInfo,
	}
}

// Tick implements the `orchestrator.State` interface
func (m *Manager) Tick(ctx context.Context, state orchestrator.ReactorState) (nextState orchestrator.ReactorState, err error) {
	globalState := state.(*globalState)
	closeProcessor := func(changefeedID model.ChangeFeedID) {
		if processor, exist := m.Processors[changefeedID]; exist {
			err := processor.Close()
			if err != nil {
				log.Warn("failed to close processor", zap.Error(err))
			}
			delete(m.Processors, changefeedID)
		}
	}
	for changefeedID, changefeedState := range globalState.Changefeeds {
		if !changefeedState.Active() {
			closeProcessor(changefeedID)
			continue
		}
		processor, exist := m.Processors[changefeedID]
		if !exist {
			processor = NewProcessor(m.pdCli, m.credential, m.captureInfo)
			m.Processors[changefeedID] = processor
		}
		if _, err := processor.Tick(ctx, changefeedState); err != nil {
			if cerrors.ErrReactorFinished.Equal(err) {
				closeProcessor(changefeedID)
				continue
			}
			return state, errors.Trace(err)
		}
	}
	for _, changefeedID := range globalState.GetRemovedChangefeeds() {
		closeProcessor(changefeedID)
	}
	return state, nil
}

func (m *Manager) writeDebugInfo() {}
