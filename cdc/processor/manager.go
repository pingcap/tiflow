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
	cdcContext "github.com/pingcap/tiflow/pkg/context"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"github.com/pingcap/tiflow/pkg/upstream"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

type commandTp int

const (
	commandTpUnknow commandTp = iota //nolint:varcheck,deadcode
	commandTpClose
	commandTpWriteDebugInfo
	processorLogsWarnDuration = 1 * time.Second
)

type command struct {
	tp      commandTp
	payload interface{}
	done    chan<- error
}

// Manager is a manager of processor, which maintains the state and behavior of processors
type Manager struct {
	processors      map[model.ChangeFeedID]*processor
	commandQueue    chan *command
	upstreamManager *upstream.Manager

	newProcessor func(cdcContext.Context, *upstream.Upstream) *processor

	metricProcessorCloseDuration prometheus.Observer
}

// NewManager creates a new processor manager
func NewManager(upstreamManager *upstream.Manager) *Manager {
	return &Manager{
		processors:                   make(map[model.ChangeFeedID]*processor),
		commandQueue:                 make(chan *command, 4),
		upstreamManager:              upstreamManager,
		newProcessor:                 newProcessor,
		metricProcessorCloseDuration: processorCloseDuration,
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
		ctx := cdcContext.WithChangefeedVars(ctx, &cdcContext.ChangefeedVars{
			ID:   changefeedID,
			Info: changefeedState.Info,
		})
		processor, exist := m.processors[changefeedID]
		if !exist {
			upStream := m.upstreamManager.Get(changefeedState.Info.UpstreamID)
			failpoint.Inject("processorManagerHandleNewChangefeedDelay", nil)
			processor = m.newProcessor(ctx, upStream)
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
		startTime := time.Now()
		captureID := processor.captureInfo.ID
		err := processor.Close()
		costTime := time.Since(startTime)
		if costTime > processorLogsWarnDuration {
			log.Warn("processor close took too long",
				zap.String("namespace", changefeedID.Namespace),
				zap.String("changefeed", changefeedID.ID),
				zap.String("capture", captureID), zap.Duration("duration", costTime))
		}
		m.metricProcessorCloseDuration.Observe(costTime.Seconds())
		if err != nil {
			log.Warn("failed to close processor",
				zap.String("namespace", changefeedID.Namespace),
				zap.String("changefeed", changefeedID.ID),
				zap.Error(err))
		}
		delete(m.processors, changefeedID)
	}
}

// AsyncClose sends a signal to Manager to close all processors.
func (m *Manager) AsyncClose() {
	timeout := 3 * time.Second
	ctx, cancel := context.WithTimeout(context.TODO(), timeout)
	defer cancel()
	done := make(chan error, 1)
	err := m.sendCommand(ctx, commandTpClose, nil, done)
	if err != nil {
		log.Warn("async close failed", zap.Error(err))
	}
}

// WriteDebugInfo write the debug info to Writer
func (m *Manager) WriteDebugInfo(
	ctx context.Context, w io.Writer, done chan<- error,
) {
	err := m.sendCommand(ctx, commandTpWriteDebugInfo, w, done)
	if err != nil {
		log.Warn("send command commandTpWriteDebugInfo failed", zap.Error(err))
	}
}

// sendCommands sends command to manager.
// `done` is closed upon command completion or sendCommand returns error.
func (m *Manager) sendCommand(
	ctx context.Context, tp commandTp, payload interface{}, done chan<- error,
) error {
	cmd := &command{tp: tp, payload: payload, done: done}
	select {
	case <-ctx.Done():
		close(done)
		return errors.Trace(ctx.Err())
	case m.commandQueue <- cmd:
		// FIXME: signal EtcdWorker to handle commands ASAP.
	}
	return nil
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
		// FIXME: we should drain command queue and signal callers an error.
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
