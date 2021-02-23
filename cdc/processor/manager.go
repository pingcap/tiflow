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
	"fmt"
	"io"
	"time"

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

type commandTp int

const (
	commandTpUnknow commandTp = iota //nolint:varcheck,deadcode
	commandTpClose
	commandTpWriteDebugInfo
)

type command struct {
	tp      commandTp
	payload interface{}
	done    chan struct{}
}

// Manager is a manager of processor, which maintains the state and behavior of processors
type Manager struct {
	processors map[model.ChangeFeedID]*processor

	pdCli       pd.Client
	credential  *security.Credential
	captureInfo *model.CaptureInfo

	commandQueue chan *command

	newProcessor func(
		pdCli pd.Client,
		changefeedID model.ChangeFeedID,
		credential *security.Credential,
		captureInfo *model.CaptureInfo,
	) *processor
}

// NewManager creates a new processor manager
func NewManager(pdCli pd.Client, credential *security.Credential, captureInfo *model.CaptureInfo) *Manager {
	return &Manager{
		processors:  make(map[model.ChangeFeedID]*processor),
		pdCli:       pdCli,
		credential:  credential,
		captureInfo: captureInfo,

		commandQueue: make(chan *command, 4),
		newProcessor: newProcessor,
	}
}

// Tick implements the `orchestrator.State` interface
// the `state` parameter is sent by the etcd worker, the `state` must be a snapshot of KVs in etcd
// the Tick function of Manager create or remove processor instances according to the specified `state`, or pass the `state` to processor instances
func (m *Manager) Tick(ctx context.Context, state orchestrator.ReactorState) (nextState orchestrator.ReactorState, err error) {
	globalState := state.(*globalState)
	if err := m.handleCommand(); err != nil {
		return state, err
	}
	var inactiveChangefeedCount int
	for changefeedID, changefeedState := range globalState.Changefeeds {
		if !changefeedState.Active() {
			inactiveChangefeedCount++
			m.closeProcessor(changefeedID)
			continue
		}
		processor, exist := m.processors[changefeedID]
		if !exist {
			if changefeedState.TaskStatus.AdminJobType.IsStopState() {
				continue
			}
			failpoint.Inject("processorManagerHandleNewChangefeedDelay", nil)
			processor = m.newProcessor(m.pdCli, changefeedID, m.credential, m.captureInfo)
			m.processors[changefeedID] = processor
		}
		if _, err := processor.Tick(ctx, changefeedState); err != nil {
			m.closeProcessor(changefeedID)
			if cerrors.ErrReactorFinished.Equal(errors.Cause(err)) {
				continue
			}
			return state, errors.Trace(err)
		}
	}
	// check if the processors in memory is leaked
	if len(globalState.Changefeeds)-inactiveChangefeedCount != len(m.processors) {
		for changefeedID := range m.processors {
			if _, exist := globalState.Changefeeds[changefeedID]; !exist {
				m.closeProcessor(changefeedID)
			}
		}
	}
	return state, nil
}

func (m *Manager) closeProcessor(changefeedID model.ChangeFeedID) {
	if processor, exist := m.processors[changefeedID]; exist {
		err := processor.Close()
		if err != nil {
			log.Warn("failed to close processor", zap.Error(err))
		}
		delete(m.processors, changefeedID)
	}
}

// AsyncClose sends a close signal to Manager and closing all processors
func (m *Manager) AsyncClose() {
	m.sendCommand(commandTpClose, nil)
}

// WriteDebugInfo write the debug info to Writer
func (m *Manager) WriteDebugInfo(w io.Writer) {
	timeout := time.Second * 3
	done := m.sendCommand(commandTpWriteDebugInfo, w)
	// wait the debug info printed
	select {
	case <-done:
	case <-time.After(timeout):
		fmt.Fprintf(w, "failed to print debug info\n")
	}
}

func (m *Manager) sendCommand(tp commandTp, payload interface{}) chan struct{} {
	timeout := time.Second * 3
	cmd := &command{tp: tp, payload: payload, done: make(chan struct{})}
	select {
	case m.commandQueue <- cmd:
	case <-time.After(timeout):
		close(cmd.done)
		log.Warn("the command queue is full, ignore this command", zap.Any("command", cmd))
	}
	return cmd.done
}

func (m *Manager) handleCommand() error {
	var cmd *command
	select {
	case cmd = <-m.commandQueue:
	default:
		return nil
	}
	defer close(cmd.done)
	switch cmd.tp {
	case commandTpClose:
		for changefeedID := range m.processors {
			m.closeProcessor(changefeedID)
		}
		return cerrors.ErrReactorFinished
	case commandTpWriteDebugInfo:
		w := cmd.payload.(io.Writer)
		m.writeDebugInfo(w)
	default:
		log.Warn("Unknown command in processor manager", zap.Any("command", cmd))
	}
	return nil
}

func (m *Manager) writeDebugInfo(w io.Writer) {
	for changefeedID, processor := range m.processors {
		fmt.Fprintf(w, "changefeedID: %s\n", changefeedID)
		processor.WriteDebugInfo(w)
		fmt.Fprintf(w, "\n")
	}
}
