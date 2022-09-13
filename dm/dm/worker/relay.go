// Copyright 2019 PingCAP, Inc.
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

package worker

import (
	"context"
	"sync"

	"github.com/pingcap/errors"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/streamer"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/relay"
)

// RelayHolder for relay unit.
type RelayHolder interface {
	// Init initializes the holder
	Init(ctx context.Context, interceptors []relay.PurgeInterceptor) (relay.Purger, error)
	// Start starts run the relay
	Start()
	// Close closes the holder
	Close()
	// Status returns relay unit's status
	Status(sourceStatus *binlog.SourceStatus) *pb.RelayStatus
	// Stage returns the stage of the relay
	Stage() pb.Stage
	// Error returns relay unit's status
	Error() *pb.RelayError
	// Operate operates relay unit
	Operate(ctx context.Context, op pb.RelayOp) error
	// Result returns the result of the relay
	Result() *pb.ProcessResult
	// Update updates relay config online
	Update(ctx context.Context, cfg *config.SourceConfig) error
	// Relay returns relay object
	Relay() relay.Process
}

// NewRelayHolder is relay holder initializer
// it can be used for testing.
var NewRelayHolder = NewRealRelayHolder

// realRelayHolder used to hold the relay unit.
type realRelayHolder struct {
	sync.RWMutex
	wg sync.WaitGroup

	relay relay.Process
	cfg   *config.SourceConfig

	ctx    context.Context
	cancel context.CancelFunc

	l log.Logger

	closed atomic.Bool
	stage  pb.Stage
	result *pb.ProcessResult // the process result, nil when is processing
}

// NewRealRelayHolder creates a new RelayHolder.
func NewRealRelayHolder(sourceCfg *config.SourceConfig) RelayHolder {
	cfg := relay.FromSourceCfg(sourceCfg)

	h := &realRelayHolder{
		cfg:   sourceCfg,
		stage: pb.Stage_New,
		relay: relay.NewRelay(cfg),
		l:     log.With(zap.String("component", "relay holder")),
	}
	h.closed.Store(true)
	return h
}

// Init initializes the holder.
func (h *realRelayHolder) Init(ctx context.Context, interceptors []relay.PurgeInterceptor) (relay.Purger, error) {
	h.closed.Store(false)

	// initial relay purger
	operators := []relay.Operator{
		h,
		streamer.GetReaderHub(),
	}

	if err := h.relay.Init(ctx); err != nil {
		return nil, terror.Annotate(err, "initial relay unit")
	}

	return relay.NewPurger(h.cfg.Purge, h.cfg.RelayDir, operators, interceptors), nil
}

// Start starts run the relay.
func (h *realRelayHolder) Start() {
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		h.run()
	}()
}

// Close closes the holder.
func (h *realRelayHolder) Close() {
	if !h.closed.CAS(false, true) {
		return
	}

	if h.cancel != nil {
		h.cancel()
	}
	h.wg.Wait() // wait process return

	h.relay.Close()
}

func (h *realRelayHolder) run() {
	if !h.setStageIfNot(pb.Stage_Running, pb.Stage_Running) {
		return
	}
	h.ctx, h.cancel = context.WithCancel(context.Background())
	h.setResult(nil) // clear previous result

	r := h.relay.Process(h.ctx)
	h.cancel()

	h.setResult(&r)
	for _, err := range r.Errors {
		h.l.Error("process error", zap.Stringer("type", err))
	}

	h.setStageIfNot(pb.Stage_Stopped, pb.Stage_Paused)
}

// Status returns relay unit's status.
func (h *realRelayHolder) Status(sourceStatus *binlog.SourceStatus) *pb.RelayStatus {
	if h.closed.Load() || h.relay.IsClosed() {
		return &pb.RelayStatus{
			Stage: pb.Stage_Stopped,
		}
	}

	s := h.relay.Status(sourceStatus).(*pb.RelayStatus)
	s.Stage = h.Stage()
	s.Result = h.Result()

	return s
}

// Error returns relay unit's status.
func (h *realRelayHolder) Error() *pb.RelayError {
	if h.closed.Load() || h.relay.IsClosed() {
		return &pb.RelayError{
			Msg: "relay stopped",
		}
	}

	s := h.relay.Error().(*pb.RelayError)
	return s
}

// Operate operates relay unit.
func (h *realRelayHolder) Operate(ctx context.Context, op pb.RelayOp) error {
	switch op {
	case pb.RelayOp_PauseRelay:
		return h.pauseRelay(ctx, op)
	case pb.RelayOp_ResumeRelay:
		return h.resumeRelay(ctx, op)
	case pb.RelayOp_StopRelay:
		return h.stopRelay(ctx, op)
	}
	return terror.ErrWorkerRelayOperNotSupport.Generate(op.String())
}

func (h *realRelayHolder) pauseRelay(_ context.Context, op pb.RelayOp) error {
	h.Lock()
	if h.stage != pb.Stage_Running {
		h.Unlock()
		return terror.ErrWorkerRelayStageNotValid.Generate(h.stage, pb.Stage_Running, op)
	}
	h.stage = pb.Stage_Paused

	if h.cancel != nil {
		h.cancel()
	}
	h.Unlock()  // unlock to make `run` can return
	h.wg.Wait() // wait process return

	h.relay.Pause()

	return nil
}

func (h *realRelayHolder) resumeRelay(_ context.Context, op pb.RelayOp) error {
	h.Lock()
	defer h.Unlock()
	if h.stage != pb.Stage_Paused {
		return terror.ErrWorkerRelayStageNotValid.Generate(h.stage, pb.Stage_Paused, op)
	}

	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		h.run()
	}()
	return nil
}

func (h *realRelayHolder) stopRelay(_ context.Context, op pb.RelayOp) error {
	h.Lock()
	if h.stage == pb.Stage_Stopped {
		h.Unlock()
		return terror.ErrWorkerRelayStageNotValid.Generatef("current stage is already stopped not valid, relayop %s", op)
	}
	h.stage = pb.Stage_Stopped
	h.Unlock() // unlock to make `run` can return

	// now, when try to stop relay unit, we close relay holder
	h.Close()
	return nil
}

// Stage returns the stage of the relay.
func (h *realRelayHolder) Stage() pb.Stage {
	h.RLock()
	defer h.RUnlock()
	return h.stage
}

// setStageIfNot sets stage to newStage if its current value is not oldStage, similar to CAS.
func (h *realRelayHolder) setStageIfNot(oldStage, newStage pb.Stage) bool {
	h.Lock()
	defer h.Unlock()
	if h.stage != oldStage {
		h.stage = newStage
		return true
	}
	return false
}

func (h *realRelayHolder) setResult(result *pb.ProcessResult) {
	h.Lock()
	defer h.Unlock()
	if result == nil {
		h.result = nil
	} else {
		clone := *result
		h.result = &clone
	}
}

// Result returns the result of the relay
// Note this method will omit the `Error` field in `pb.ProcessError`, so no duplicated
// error message information will be displayed in `query-status`, as the `Msg` field
// contains enough error information.
func (h *realRelayHolder) Result() *pb.ProcessResult {
	h.RLock()
	defer h.RUnlock()
	return h.result
}

// Update update relay config online.
func (h *realRelayHolder) Update(ctx context.Context, sourceCfg *config.SourceConfig) error {
	relayCfg := relay.FromSourceCfg(sourceCfg)

	if stage := h.Stage(); stage == pb.Stage_Paused {
		err := h.relay.Reload(relayCfg)
		if err != nil {
			return err
		}
	} else if stage == pb.Stage_Running {
		err := h.Operate(ctx, pb.RelayOp_PauseRelay)
		if err != nil {
			return err
		}

		err = h.relay.Reload(relayCfg)
		if err != nil {
			return err
		}

		err = h.Operate(ctx, pb.RelayOp_ResumeRelay)
		if err != nil {
			return err
		}
	}

	return nil
}

// EarliestActiveRelayLog implements Operator.EarliestActiveRelayLog.
func (h *realRelayHolder) EarliestActiveRelayLog() *streamer.RelayLogInfo {
	return h.relay.ActiveRelayLog()
}

func (h *realRelayHolder) Relay() relay.Process {
	return h.relay
}

/******************** dummy relay holder ********************/

type dummyRelayHolder struct {
	sync.RWMutex
	initError   error
	stage       pb.Stage
	relayBinlog string

	cfg    *config.SourceConfig
	relay2 relay.Process
}

// NewDummyRelayHolder creates a new RelayHolder.
func NewDummyRelayHolder(cfg *config.SourceConfig) RelayHolder {
	return &dummyRelayHolder{
		cfg:    cfg,
		stage:  pb.Stage_New,
		relay2: &relay.Relay{},
	}
}

// NewDummyRelayHolderWithRelayBinlog creates a new RelayHolder with relayBinlog in relayStatus.
func NewDummyRelayHolderWithRelayBinlog(cfg *config.SourceConfig, relayBinlog string) RelayHolder {
	return &dummyRelayHolder{
		cfg:         cfg,
		relayBinlog: relayBinlog,
	}
}

// NewDummyRelayHolderWithInitError creates a new RelayHolder with init error.
func NewDummyRelayHolderWithInitError(cfg *config.SourceConfig) RelayHolder {
	return &dummyRelayHolder{
		initError: errors.New("init error"),
		cfg:       cfg,
	}
}

// Init implements interface of RelayHolder.
func (d *dummyRelayHolder) Init(ctx context.Context, interceptors []relay.PurgeInterceptor) (relay.Purger, error) {
	// initial relay purger
	operators := []relay.Operator{
		d,
	}

	return relay.NewDummyPurger(d.cfg.Purge, d.cfg.RelayDir, operators, interceptors), d.initError
}

// Start implements interface of RelayHolder.
func (d *dummyRelayHolder) Start() {
	d.Lock()
	defer d.Unlock()
	d.stage = pb.Stage_Running
}

// Close implements interface of RelayHolder.
func (d *dummyRelayHolder) Close() {
	d.Lock()
	defer d.Unlock()
	d.stage = pb.Stage_Stopped
}

// Status implements interface of RelayHolder.
func (d *dummyRelayHolder) Status(sourceStatus *binlog.SourceStatus) *pb.RelayStatus {
	d.Lock()
	defer d.Unlock()
	return &pb.RelayStatus{
		Stage:       d.stage,
		RelayBinlog: d.relayBinlog,
	}
}

// Error implements interface of RelayHolder.
func (d *dummyRelayHolder) Error() *pb.RelayError {
	return nil
}

// Operate implements interface of RelayHolder.
func (d *dummyRelayHolder) Operate(ctx context.Context, op pb.RelayOp) error {
	d.Lock()
	defer d.Unlock()
	switch op {
	case pb.RelayOp_PauseRelay:
		if d.stage != pb.Stage_Running {
			return terror.ErrWorkerRelayStageNotValid.Generate(d.stage, pb.Stage_Running, op)
		}
		d.stage = pb.Stage_Paused
	case pb.RelayOp_ResumeRelay:
		if d.stage != pb.Stage_Paused {
			return terror.ErrWorkerRelayStageNotValid.Generate(d.stage, pb.Stage_Paused, op)
		}
		d.stage = pb.Stage_Running
	case pb.RelayOp_StopRelay:
		if d.stage == pb.Stage_Stopped {
			return terror.ErrWorkerRelayStageNotValid.Generatef("current stage is already stopped not valid, relayop %s", op)
		}
		d.stage = pb.Stage_Stopped
	}
	return nil
}

// Result implements interface of RelayHolder.
func (d *dummyRelayHolder) Result() *pb.ProcessResult {
	return nil
}

// Update implements interface of RelayHolder.
func (d *dummyRelayHolder) Update(ctx context.Context, cfg *config.SourceConfig) error {
	return nil
}

func (d *dummyRelayHolder) EarliestActiveRelayLog() *streamer.RelayLogInfo {
	return nil
}

func (d *dummyRelayHolder) Stage() pb.Stage {
	d.Lock()
	defer d.Unlock()
	return d.stage
}

func (d *dummyRelayHolder) Relay() relay.Process {
	return d.relay2
}
