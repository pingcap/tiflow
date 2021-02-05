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
	"io"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/orchestrator"
	"github.com/pingcap/ticdc/pkg/security"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

// Manager is a manager of processor, which maintains the state and behavior of processors
type Manager struct {
	Processors map[model.ChangeFeedID]*processor

	pdCli       pd.Client
	credential  *security.Credential
	captureInfo *model.CaptureInfo

	close int32
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
	if atomic.LoadInt32(&m.close) != 0 {
		for changefeedID := range m.Processors {
			m.closeProcessor(changefeedID)
		}
		return state, cerrors.ErrReactorFinished
	}
	var inactiveChangefeedCount int
	for changefeedID, changefeedState := range globalState.Changefeeds {
		if !changefeedState.Active() {
			inactiveChangefeedCount++
			m.closeProcessor(changefeedID)
			continue
		}
		processor, exist := m.Processors[changefeedID]
		if !exist {
			failpoint.Inject("processorManagerHandleNewChangefeedDelay", nil)
			processor = newProcessor(m.pdCli, m.credential, m.captureInfo)
			m.Processors[changefeedID] = processor
		}
		if _, err := processor.Tick(ctx, changefeedState); err != nil {
			m.closeProcessor(changefeedID)
			if cerrors.ErrReactorFinished.Equal(err) {
				continue
			}
			return state, errors.Trace(err)
		}
	}
	// check if the processors in memory is leaked
	if len(globalState.Changefeeds)-inactiveChangefeedCount != len(m.Processors) {
		for changefeedID := range m.Processors {
			if _, exist := globalState.Changefeeds[changefeedID]; !exist {
				m.closeProcessor(changefeedID)
			}
		}
	}
	return state, nil
}

func (m *Manager) closeProcessor(changefeedID model.ChangeFeedID) {
	if processor, exist := m.Processors[changefeedID]; exist {
		err := processor.Close()
		if err != nil {
			log.Warn("failed to close processor", zap.Error(err))
		}
		delete(m.Processors, changefeedID)
	}
}

// AsyncClose sends a close signal to Manager and closing all processors
func (m *Manager) AsyncClose() {
	atomic.StoreInt32(&m.close, 1)
}

// WriteDebugInfo write the debug info to Writer
func (m *Manager) WriteDebugInfo(w io.Writer) {
	// TODO: implement this function
	// fmt.Fprintf(w, "** active changefeeds **:\n")
	// for _, info := range o.changeFeeds {
	// 	fmt.Fprintf(w, "%s\n", info)
	// }
	// fmt.Fprintf(w, "** stopped changefeeds **:\n")
	// for _, feedStatus := range o.stoppedFeeds {
	// 	fmt.Fprintf(w, "%+v\n", *feedStatus)
	// }
	// fmt.Fprintf(w, "\n** captures **:\n")
	// for _, capture := range o.captures {
	// 	fmt.Fprintf(w, "%+v\n", *capture)
	// }
}
