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
	commandTpUnknown commandTp = iota
	commandTpClose
	commandTpWriteDebugInfo
	// Query the number of tables in the manager.
	// command payload is a buffer channel of int, make(chan int, 1).
	commandTpQueryTableCount
	processorLogsWarnDuration = 1 * time.Second
)

type command struct {
	tp      commandTp
	payload interface{}
	done    chan<- error
}

// Manager is a manager of processor, which maintains the state and behavior of processors
type Manager interface {
	orchestrator.Reactor
	QueryTableCount(ctx context.Context, tableCh chan int, done chan<- error)
	WriteDebugInfo(ctx context.Context, w io.Writer, done chan<- error)
	AsyncClose()
}

// managerImpl is a manager of processor, which maintains the state and behavior of processors
type managerImpl struct {
	captureInfo     *model.CaptureInfo
	liveness        *model.Liveness
	processors      map[model.ChangeFeedID]*processor
	commandQueue    chan *command
	upstreamManager *upstream.Manager

	newProcessor func(
		*orchestrator.ChangefeedReactorState,
		*model.CaptureInfo,
		model.ChangeFeedID,
		*upstream.Upstream,
		*model.Liveness,
	) *processor

	metricProcessorCloseDuration prometheus.Observer
}

// NewManager creates a new processor manager
func NewManager(
	captureInfo *model.CaptureInfo,
	upstreamManager *upstream.Manager,
	liveness *model.Liveness,
) Manager {
	return &managerImpl{
		captureInfo:                  captureInfo,
		liveness:                     liveness,
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
func (m *managerImpl) Tick(stdCtx context.Context, state orchestrator.ReactorState) (nextState orchestrator.ReactorState, err error) {
	ctx := stdCtx.(cdcContext.Context)
	globalState := state.(*orchestrator.GlobalReactorState)
	if err := m.handleCommand(ctx); err != nil {
		return state, err
	}

	var inactiveChangefeedCount int
	for changefeedID, changefeedState := range globalState.Changefeeds {
		if !changefeedState.Active(m.captureInfo.ID) {
			inactiveChangefeedCount++
			m.closeProcessor(changefeedID, ctx)
			continue
		}
		p, exist := m.processors[changefeedID]
		if !exist {
			up, ok := m.upstreamManager.Get(changefeedState.Info.UpstreamID)
			if !ok {
				upstreamInfo := globalState.Upstreams[changefeedState.Info.UpstreamID]
				up = m.upstreamManager.AddUpstream(upstreamInfo)
			}
			failpoint.Inject("processorManagerHandleNewChangefeedDelay", nil)
			p = m.newProcessor(changefeedState, m.captureInfo, changefeedID, up, m.liveness)
			m.processors[changefeedID] = p
		}
		ctx := cdcContext.WithChangefeedVars(ctx, &cdcContext.ChangefeedVars{
			ID:   changefeedID,
			Info: changefeedState.Info,
		})
		if err := p.Tick(ctx); err != nil {
			// processor have already patched its error to tell the owner
			// manager can just close the processor and continue to tick other processors
			m.closeProcessor(changefeedID, ctx)
		}
	}
	// check if the processors in memory is leaked
	if len(globalState.Changefeeds)-inactiveChangefeedCount != len(m.processors) {
		for changefeedID := range m.processors {
			if _, exist := globalState.Changefeeds[changefeedID]; !exist {
				m.closeProcessor(changefeedID, ctx)
			}
		}
	}

	// close upstream
	if err := m.upstreamManager.Tick(stdCtx, globalState); err != nil {
		return state, errors.Trace(err)
	}
	return state, nil
}

func (m *managerImpl) closeProcessor(changefeedID model.ChangeFeedID, ctx cdcContext.Context) {
	processor, exist := m.processors[changefeedID]
	if exist {
		startTime := time.Now()
		err := processor.Close(ctx)
		costTime := time.Since(startTime)
		if costTime > processorLogsWarnDuration {
			log.Warn("processor close took too long",
				zap.String("namespace", changefeedID.Namespace),
				zap.String("changefeed", changefeedID.ID),
				zap.String("capture", m.captureInfo.ID),
				zap.Duration("duration", costTime))
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
func (m *managerImpl) AsyncClose() {
	timeout := 3 * time.Second
	ctx, cancel := context.WithTimeout(context.TODO(), timeout)
	defer cancel()
	done := make(chan error, 1)
	err := m.sendCommand(ctx, commandTpClose, nil, done)
	if err != nil {
		log.Warn("async close failed", zap.Error(err))
	}
}

// QueryTableCount query the number of tables in the manager.
func (m *managerImpl) QueryTableCount(
	ctx context.Context, tableCh chan int, done chan<- error,
) {
	err := m.sendCommand(ctx, commandTpQueryTableCount, tableCh, done)
	if err != nil {
		log.Warn("send command commandTpQueryTableCount failed", zap.Error(err))
	}
}

// WriteDebugInfo write the debug info to Writer
func (m *managerImpl) WriteDebugInfo(
	ctx context.Context, w io.Writer, done chan<- error,
) {
	err := m.sendCommand(ctx, commandTpWriteDebugInfo, w, done)
	if err != nil {
		log.Warn("send command commandTpWriteDebugInfo failed", zap.Error(err))
	}
}

// sendCommands sends command to manager.
// `done` is closed upon command completion or sendCommand returns error.
func (m *managerImpl) sendCommand(
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

func (m *managerImpl) handleCommand(ctx cdcContext.Context) error {
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
			m.closeProcessor(changefeedID, ctx)
		}
		// FIXME: we should drain command queue and signal callers an error.
		return cerrors.ErrReactorFinished
	case commandTpWriteDebugInfo:
		w := cmd.payload.(io.Writer)
		err := m.writeDebugInfo(w)
		if err != nil {
			cmd.done <- err
		}
	case commandTpQueryTableCount:
		count := 0
		for _, p := range m.processors {
			count += len(p.GetAllCurrentTables())
		}
		select {
		case cmd.payload.(chan int) <- count:
		default:
		}
	default:
		log.Warn("Unknown command in processor manager", zap.Any("command", cmd))
	}
	return nil
}

func (m *managerImpl) writeDebugInfo(w io.Writer) error {
	for changefeedID, processor := range m.processors {
		fmt.Fprintf(w, "changefeedID: %s\n", changefeedID)
		err := processor.WriteDebugInfo(w)
		if err != nil {
			return errors.Trace(err)
		}
		fmt.Fprintf(w, "\n")
	}

	return nil
}
