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
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cdcContext "github.com/pingcap/tiflow/pkg/context"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/orchestrator"
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

<<<<<<< HEAD
	newProcessor func(cdcContext.Context) *processor
=======
	newProcessor func(
		*orchestrator.ChangefeedReactorState,
		*model.CaptureInfo,
		model.ChangeFeedID,
		*upstream.Upstream,
		*model.Liveness,
		uint64,
		*config.SchedulerConfig,
	) *processor
	cfg *config.SchedulerConfig
>>>>>>> 0867f80e5f (cdc: add changefeed epoch to prevent unexpected state (#8268))

	enableNewScheduler bool
}

// NewManager creates a new processor manager
func NewManager() *Manager {
	conf := config.GetGlobalServerConfig()
	return &Manager{
		processors:         make(map[model.ChangeFeedID]*processor),
		commandQueue:       make(chan *command, 4),
		newProcessor:       newProcessor,
		enableNewScheduler: conf.Debug.EnableNewScheduler,
	}
}

// Tick implements the `orchestrator.State` interface
// the `state` parameter is sent by the etcd worker, the `state` must be a snapshot of KVs in etcd
// the Tick function of Manager create or remove processor instances according to the specified `state`, or pass the `state` to processor instances
func (m *Manager) Tick(stdCtx context.Context, state orchestrator.ReactorState) (nextState orchestrator.ReactorState, err error) {
	ctx := stdCtx.(cdcContext.Context)
	globalState := state.(*orchestrator.GlobalReactorState)
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
<<<<<<< HEAD
=======
		currentChangefeedEpoch := changefeedState.Info.Epoch
		p, exist := m.processors[changefeedID]
		if !exist {
			up, ok := m.upstreamManager.Get(changefeedState.Info.UpstreamID)
			if !ok {
				upstreamInfo := globalState.Upstreams[changefeedState.Info.UpstreamID]
				up = m.upstreamManager.AddUpstream(upstreamInfo)
			}
			failpoint.Inject("processorManagerHandleNewChangefeedDelay", nil)

			cfg := *m.cfg
			cfg.ChangefeedSettings = changefeedState.Info.Config.Scheduler
			p = m.newProcessor(
				changefeedState, m.captureInfo, changefeedID, up, m.liveness,
				currentChangefeedEpoch, &cfg)
			m.processors[changefeedID] = p
		}
>>>>>>> 0867f80e5f (cdc: add changefeed epoch to prevent unexpected state (#8268))
		ctx := cdcContext.WithChangefeedVars(ctx, &cdcContext.ChangefeedVars{
			ID:   changefeedID,
			Info: changefeedState.Info,
		})
<<<<<<< HEAD
		processor, exist := m.processors[changefeedID]
		if !exist {
			if m.enableNewScheduler {
				failpoint.Inject("processorManagerHandleNewChangefeedDelay", nil)
				processor = m.newProcessor(ctx)
				m.processors[changefeedID] = processor
			} else {
				if changefeedState.Status.AdminJobType.IsStopState() || changefeedState.TaskStatuses[captureID].AdminJobType.IsStopState() {
					continue
				}
				// the processor should start after at least one table has been added to this capture
				taskStatus := changefeedState.TaskStatuses[captureID]
				if taskStatus == nil || (len(taskStatus.Tables) == 0 && len(taskStatus.Operation) == 0) {
					continue
				}
				failpoint.Inject("processorManagerHandleNewChangefeedDelay", nil)
				processor = m.newProcessor(ctx)
				m.processors[changefeedID] = processor
			}
		}
		if _, err := processor.Tick(ctx, changefeedState); err != nil {
			m.closeProcessor(changefeedID)
			if cerrors.ErrReactorFinished.Equal(errors.Cause(err)) {
				continue
			}
			return state, errors.Trace(err)
=======
		if currentChangefeedEpoch != p.changefeedEpoch {
			// Changefeed has restarted due to error, the processor is stale.
			m.closeProcessor(changefeedID, ctx)
			continue
		}
		if err := p.Tick(ctx); err != nil {
			// processor have already patched its error to tell the owner
			// manager can just close the processor and continue to tick other processors
			m.closeProcessor(changefeedID, ctx)
>>>>>>> 0867f80e5f (cdc: add changefeed epoch to prevent unexpected state (#8268))
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
			log.Warn("failed to close processor",
				zap.String("changefeed", changefeedID),
				zap.Error(err))
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
		fmt.Fprintf(w, "failed to print debug info for processor\n")
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
