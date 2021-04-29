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
	stdContext "context"
	"fmt"
	"io"
	"time"

	tablepipeline "github.com/pingcap/ticdc/cdc/processor/pipeline"

	"go.etcd.io/etcd/clientv3"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/context"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/orchestrator"
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

	commandQueue chan *command
	leaseID      clientv3.LeaseID

	newProcessor func(context.Context) *processor
}

// NewManager creates a new processor manager
func NewManager() *Manager {
	return &Manager{
		processors:   make(map[model.ChangeFeedID]*processor),
		commandQueue: make(chan *command, 4),
		newProcessor: newProcessor,
	}
}

func NewManager4Test(
	lazyInit func(ctx context.Context) error,
	createTablePipeline func(ctx context.Context, tableID model.TableID, replicaInfo *model.TableReplicaInfo) (tablepipeline.TablePipeline, error),
) *Manager {
	m := NewManager()
	m.newProcessor = func(ctx context.Context) *processor {
		p := newProcessor(ctx)
		p.lazyInit = lazyInit
		p.createTablePipeline = createTablePipeline
		return p
	}
	return m
}

// Tick implements the `orchestrator.State` interface
// the `state` parameter is sent by the etcd worker, the `state` must be a snapshot of KVs in etcd
// the Tick function of Manager create or remove processor instances according to the specified `state`, or pass the `state` to processor instances
func (m *Manager) Tick(stdCtx stdContext.Context, state orchestrator.ReactorState) (nextState orchestrator.ReactorState, err error) {
	ctx := stdCtx.(context.Context)
	globalState := state.(*model.GlobalReactorState)
	globalState.CheckLeaseExpired(m.leaseID)
	if err := m.handleCommand(); err != nil {
		return state, err
	}
	captureID := ctx.GlobalVars().CaptureInfo.ID
	var inactiveChangefeedCount int
	for changefeedID, changefeedState := range globalState.Changefeeds {
		if !changefeedState.Active(captureID) {
			inactiveChangefeedCount++
			m.closeProcessor(changefeedID)
			continue
		}
		ctx := context.WithChangefeedVars(ctx, &context.ChangefeedVars{
			ID:   changefeedID,
			Info: changefeedState.Info,
		})
		processor, exist := m.processors[changefeedID]
		if !exist {
			if changefeedState.Status.AdminJobType.IsStopState() || changefeedState.TaskStatuses[captureID].AdminJobType.IsStopState() {
				continue
			}
			failpoint.Inject("processorManagerHandleNewChangefeedDelay", nil)
			processor = m.newProcessor(ctx)
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
