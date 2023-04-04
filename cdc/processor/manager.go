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
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"github.com/pingcap/tiflow/pkg/upstream"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

type commandTp int

const (
	commandTpUnknown commandTp = iota
	commandTpWriteDebugInfo
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

	// Close the manager itself and all processors. Can't be called with `Tick` concurrently.
	// After it's called, all other methods shouldn't be called any more.
	Close()

	WriteDebugInfo(ctx context.Context, w io.Writer, done chan<- error)
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
		uint64,
		*config.SchedulerConfig,
	) *processor
	cfg *config.SchedulerConfig

	metricProcessorCloseDuration prometheus.Observer
}

// NewManager creates a new processor manager
func NewManager(
	captureInfo *model.CaptureInfo,
	upstreamManager *upstream.Manager,
	liveness *model.Liveness,
	cfg *config.SchedulerConfig,
) Manager {
	return &managerImpl{
		captureInfo:                  captureInfo,
		liveness:                     liveness,
		processors:                   make(map[model.ChangeFeedID]*processor),
		commandQueue:                 make(chan *command, 4),
		upstreamManager:              upstreamManager,
		newProcessor:                 newProcessor,
		metricProcessorCloseDuration: processorCloseDuration,
		cfg:                          cfg,
	}
}

// Tick implements the `orchestrator.State` interface
// the `state` parameter is sent by the etcd worker, the `state` must be a snapshot of KVs in etcd
// the Tick function of Manager create or remove processor instances according to the specified `state`, or pass the `state` to processor instances
func (m *managerImpl) Tick(stdCtx context.Context, state orchestrator.ReactorState) (nextState orchestrator.ReactorState, err error) {
	ctx := stdCtx.(cdcContext.Context)
	globalState := state.(*orchestrator.GlobalReactorState)
	m.handleCommand()

	var inactiveChangefeedCount int
	for changefeedID, changefeedState := range globalState.Changefeeds {
		if !changefeedState.Active(m.captureInfo.ID) {
			inactiveChangefeedCount++
			m.closeProcessor(changefeedID)
			continue
		}
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
		ctx := cdcContext.WithChangefeedVars(ctx, &cdcContext.ChangefeedVars{
			ID:   changefeedID,
			Info: changefeedState.Info,
		})
		if currentChangefeedEpoch != p.changefeedEpoch {
			// Changefeed has restarted due to error, the processor is stale.
			m.closeProcessor(changefeedID)
			continue
		}
		if err := p.Tick(ctx); err != nil {
			// processor have already patched its error to tell the owner
			// manager can just close the processor and continue to tick other processors
			m.closeProcessor(changefeedID)
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

	if err := m.upstreamManager.Tick(stdCtx, globalState); err != nil {
		return state, errors.Trace(err)
	}
	return state, nil
}

func (m *managerImpl) closeProcessor(changefeedID model.ChangeFeedID) {
	processor, exist := m.processors[changefeedID]
	if exist {
		startTime := time.Now()
		err := processor.Close()
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

// Close the manager itself and all processors.
// Note: This method must not be called with `Tick`. Please be careful.
func (m *managerImpl) Close() {
	log.Info("processor.Manager is closing")
	for changefeedID := range m.processors {
		m.closeProcessor(changefeedID)
	}
	// FIXME: we should drain command queue and signal callers an error.
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

func (m *managerImpl) handleCommand() {
	var cmd *command
	select {
	case cmd = <-m.commandQueue:
	default:
		return
	}
	defer close(cmd.done)
	switch cmd.tp {
	case commandTpWriteDebugInfo:
		w := cmd.payload.(io.Writer)
		err := m.writeDebugInfo(w)
		if err != nil {
			cmd.done <- err
		}
	default:
		log.Warn("Unknown command in processor manager", zap.Any("command", cmd))
	}
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
