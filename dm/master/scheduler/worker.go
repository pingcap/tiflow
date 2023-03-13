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

package scheduler

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/config/security"
	"github.com/pingcap/tiflow/dm/master/metrics"
	"github.com/pingcap/tiflow/dm/master/workerrpc"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/ha"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"go.uber.org/zap"
)

// WorkerStage represents the stage of a DM-worker instance.
type WorkerStage string

// the stage of DM-worker instances.
// valid transformation:
//   - Offline -> Free, receive keep-alive.
//   - Free -> Offline, lost keep-alive.
//   - Free -> Bound, bind source.
//   - Free -> Relay, start relay for a source.
//   - Bound -> Offline, lost keep-alive, when receive keep-alive again, it should become Free.
//   - Bound -> Free, unbind source.
//   - Bound -> Relay, commands like transfer-source that gracefully unbind a worker which has started relay.
//   - Relay -> Offline, lost keep-alive.
//   - Relay -> Free, stop relay.
//   - Relay -> Bound, old bound worker becomes offline so bind source to this worker, which has started relay.
//
// invalid transformation:
//   - Offline -> Bound, must become Free first.
//   - Offline -> Relay, must become Free first.
//
// in Bound stage relay can be turned on/off, the difference with Bound-Relay transformation is
//   - Bound stage turning on/off represents a bound DM worker receives start-relay/stop-relay, source bound relation is
//     not changed.
//   - Bound-Relay transformation represents source bound relation is changed.
//
// caller should ensure the correctness when invoke below transformation methods successively. For example, call ToBound
//
//	twice with different arguments.
const (
	WorkerOffline WorkerStage = "offline" // the worker is not online yet.
	WorkerFree    WorkerStage = "free"    // the worker is online, but no upstream source assigned to it yet.
	WorkerBound   WorkerStage = "bound"   // the worker is online, and one upstream source already assigned to it.
	WorkerRelay   WorkerStage = "relay"   // the worker is online, pulling relay log but not responsible for migrating.
)

var (
	nullBound ha.SourceBound

	workerStage2Num = map[WorkerStage]float64{
		WorkerOffline: 0.0,
		WorkerFree:    1.0,
		WorkerBound:   2.0,
		WorkerRelay:   1.5,
	}
	unrecognizedState = -1.0
)

// Worker is an agent for a DM-worker instance.
type Worker struct {
	mu sync.RWMutex

	cli workerrpc.Client // the gRPC client proxy.

	baseInfo ha.WorkerInfo  // the base information of the DM-worker instance.
	bound    ha.SourceBound // the source bound relationship, null value if not bound
	stage    WorkerStage    // the current stage.

	// the source ID from which the worker is pulling relay log. should keep consistent with Scheduler.relayWorkers
	relaySource string
}

// NewWorker creates a new Worker instance with Offline stage.
func NewWorker(baseInfo ha.WorkerInfo, securityCfg security.Security) (*Worker, error) {
	cli, err := workerrpc.NewGRPCClient(baseInfo.Addr, securityCfg)
	if err != nil {
		return nil, err
	}

	w := &Worker{
		cli:      cli,
		baseInfo: baseInfo,
		stage:    WorkerOffline,
	}
	w.reportMetrics()
	return w, nil
}

// Close closes the worker and release resources.
func (w *Worker) Close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.cli.Close()
}

// ToOffline transforms to Offline.
// All available transitions can be found at the beginning of this file.
func (w *Worker) ToOffline() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.stage = WorkerOffline
	w.reportMetrics()
	w.bound = nullBound
}

// ToFree transforms to Free and clears the bound and relay information.
// All available transitions can be found at the beginning of this file.
func (w *Worker) ToFree() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.stage = WorkerFree
	w.reportMetrics()
	w.bound = nullBound
	w.relaySource = ""
}

// ToBound transforms to Bound.
// All available transitions can be found at the beginning of this file.
func (w *Worker) ToBound(bound ha.SourceBound) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stage == WorkerOffline {
		return terror.ErrSchedulerWorkerInvalidTrans.Generate(w.BaseInfo(), WorkerOffline, WorkerBound)
	}
	if w.stage == WorkerRelay {
		if w.relaySource != bound.Source {
			return terror.ErrSchedulerBoundDiffWithStartedRelay.Generate(w.BaseInfo().Name, bound.Source, w.relaySource)
		}
	}

	w.stage = WorkerBound
	w.reportMetrics()
	w.bound = bound
	return nil
}

// Unbound changes worker's stage from Bound to Free or Relay.
func (w *Worker) Unbound() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stage != WorkerBound {
		// caller should not do this.
		return terror.ErrSchedulerWorkerInvalidTrans.Generatef("can't unbind a worker that is not in bound stage.")
	}

	w.bound = nullBound
	if w.relaySource != "" {
		w.stage = WorkerRelay
	} else {
		w.stage = WorkerFree
	}
	w.reportMetrics()
	return nil
}

// StartRelay adds relay source information to a bound worker and calculates the stage.
func (w *Worker) StartRelay(sourceID string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	switch w.stage {
	case WorkerOffline, WorkerRelay:
	case WorkerFree:
		w.stage = WorkerRelay
		w.reportMetrics()
	case WorkerBound:
		if w.bound.Source != sourceID {
			return terror.ErrSchedulerRelayWorkersWrongBound.Generatef(
				"can't turn on relay of source %s for worker %s, because the worker is bound to source %s",
				sourceID, w.BaseInfo().Name, w.bound.Source)
		}
	}
	w.relaySource = sourceID

	return nil
}

// StopRelay clears relay source information of a bound worker and calculates the stage.
func (w *Worker) StopRelay() {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.relaySource = ""
	switch w.stage {
	case WorkerOffline, WorkerBound:
	case WorkerFree:
		log.L().DPanic("StopRelay for a Free worker should not happen",
			zap.String("worker name", w.baseInfo.Name))
	case WorkerRelay:
		w.stage = WorkerFree
		w.reportMetrics()
	}
}

// BaseInfo returns the base info of the worker.
// No lock needed because baseInfo should not be modified after the instance created.
func (w *Worker) BaseInfo() ha.WorkerInfo {
	return w.baseInfo
}

// Stage returns the current stage.
func (w *Worker) Stage() WorkerStage {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.stage
}

// Bound returns the current source ID bound to,
// returns null value if not be bound.
func (w *Worker) Bound() ha.SourceBound {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.bound
}

// RelaySourceID returns the source ID from which this worker is pulling relay log,
// returns empty string if not started relay.
func (w *Worker) RelaySourceID() string {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.relaySource
}

// SendRequest sends request to the DM-worker instance.
func (w *Worker) SendRequest(ctx context.Context, req *workerrpc.Request, d time.Duration) (*workerrpc.Response, error) {
	return w.cli.SendRequest(ctx, req, d)
}

func (w *Worker) reportMetrics() {
	s := unrecognizedState
	if n, ok := workerStage2Num[w.stage]; ok {
		s = n
	}
	metrics.ReportWorkerStage(w.baseInfo.Name, s)
}

func (w *Worker) queryStatus(ctx context.Context) (*workerrpc.Response, error) {
	rpcTimeOut := time.Second * 10 // we relay on ctx.Done() to cancel the rpc, so just set a very long timeout
	req := &workerrpc.Request{Type: workerrpc.CmdQueryStatus, QueryStatus: &pb.QueryStatusRequest{}}
	failpoint.Inject("operateWorkerQueryStatus", func(v failpoint.Value) {
		resp := &workerrpc.Response{Type: workerrpc.CmdQueryStatus, QueryStatus: &pb.QueryStatusResponse{}}
		switch v.(string) {
		case "notInSyncUnit":
			resp.QueryStatus.SubTaskStatus = append(
				resp.QueryStatus.SubTaskStatus, &pb.SubTaskStatus{Unit: pb.UnitType_Dump})
			failpoint.Return(resp, nil)
		case "allTaskIsPaused":
			resp.QueryStatus.SubTaskStatus = append(
				resp.QueryStatus.SubTaskStatus, &pb.SubTaskStatus{Stage: pb.Stage_Paused, Unit: pb.UnitType_Sync})
			failpoint.Return(resp, nil)
		default:
			failpoint.Return(nil, errors.New("query error"))
		}
	})
	return w.SendRequest(ctx, req, rpcTimeOut)
}

func (w *Worker) checkSubtasksCanUpdate(ctx context.Context, cfg *config.SubTaskConfig) (*workerrpc.Response, error) {
	rpcTimeOut := time.Second * 10 // we relay on ctx.Done() to cancel the rpc, so just set a very long timeout
	tomlStr, err := cfg.Toml()
	if err != nil {
		return nil, err
	}
	req := &workerrpc.Request{
		Type:                   workerrpc.CmdCheckSubtasksCanUpdate,
		CheckSubtasksCanUpdate: &pb.CheckSubtasksCanUpdateRequest{SubtaskCfgTomlString: tomlStr},
	}
	failpoint.Inject("operateCheckSubtasksCanUpdate", func(v failpoint.Value) {
		resp := &workerrpc.Response{
			Type: workerrpc.CmdCheckSubtasksCanUpdate, CheckSubtasksCanUpdate: &pb.CheckSubtasksCanUpdateResponse{},
		}
		switch v.(string) {
		case "success":
			resp.CheckSubtasksCanUpdate.Success = true
			failpoint.Return(resp, nil)
		case "failed":
			resp.CheckSubtasksCanUpdate.Success = false
			resp.CheckSubtasksCanUpdate.Msg = "error happened"
			failpoint.Return(resp, nil)
		default:
			failpoint.Return(nil, errors.New("query error"))
		}
	})

	return w.SendRequest(ctx, req, rpcTimeOut)
}

// NewMockWorker is used in tests.
func NewMockWorker(cli workerrpc.Client) *Worker {
	return &Worker{cli: cli}
}
