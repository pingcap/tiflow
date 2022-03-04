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
	"go.uber.org/zap"

	"github.com/pingcap/failpoint"

	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/dm/master/metrics"
	"github.com/pingcap/tiflow/dm/dm/master/workerrpc"
	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/ha"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
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
// invalid transformation:
//   - Offline -> Bound, must become Free first.
//   - Offline -> Relay, must become Free first.
// in Bound stage relay can be turned on/off, the difference with Bound-Relay transformation is
//   - Bound stage turning on/off represents a bound DM worker receives start-relay/stop-relay, source bound relation is
//     not changed.
//   - Bound-Relay transformation represents source bound relation is changed.
// caller should ensure the correctness when invoke below transformation methods successively. For example, call ToBound
//   twice with different arguments.
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

	baseInfo ha.WorkerInfo             // the base information of the DM-worker instance.
	bound    ha.SourceBound            // the source bound relationship, null value if not bounded.
	bounds   map[string]ha.SourceBound // the source bound relationship, source-id -> bound.
	stage    WorkerStage               // the current stage.

	// the sources from which the worker is pulling relay log. should keep consistent with Scheduler.relayWorkers
	relaySources map[string]struct{}
}

// NewWorker creates a new Worker instance with Offline stage.
func NewWorker(baseInfo ha.WorkerInfo, securityCfg config.Security) (*Worker, error) {
	cli, err := workerrpc.NewGRPCClient(baseInfo.Addr, securityCfg)
	if err != nil {
		return nil, err
	}

	w := &Worker{
		cli:      cli,
		baseInfo: baseInfo,
		stage:    WorkerOffline,
		bounds:   make(map[string]ha.SourceBound),
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
	w.bounds = make(map[string]ha.SourceBound)
}

// ToFree transforms to Free and clears the bound and relay information.
// All available transitions can be found at the beginning of this file.
func (w *Worker) ToFree() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.stage = WorkerFree
	w.reportMetrics()
	w.bounds = make(map[string]ha.SourceBound)
	w.relaySources = make(map[string]struct{})
}

// ToBound transforms to Bound.
// All available transitions can be found at the beginning of this file.
func (w *Worker) ToBound(bound ha.SourceBound) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stage == WorkerOffline {
		return terror.ErrSchedulerWorkerInvalidTrans.Generate(w.BaseInfo(), WorkerOffline, WorkerBound)
	}

	w.reportMetrics()
	w.bound = bound
	w.bounds[bound.Source] = bound
	return nil
}

// Unbound changes worker's stage from Bound to Free or Relay.
func (w *Worker) Unbound(source string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, ok := w.bounds[source]; !ok {
		// caller should not do this.
		return terror.ErrSchedulerWorkerInvalidTrans.Generatef("can't unbound a source %s that is not bound with worker %s.", source, w.baseInfo.Name)
	}

	w.bound = nullBound
	delete(w.bounds, source)
	w.reportMetrics()
	return nil
}

// StartRelay adds relay source information to a bound worker and calculates the stage.
func (w *Worker) StartRelay(sources ...string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, sourceID := range sources {
		w.relaySources[sourceID] = struct{}{}
	}

	return nil
}

// StopRelay clears relay source information of a bound worker and calculates the stage.
func (w *Worker) StopRelay(source string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, ok := w.relaySources[source]; !ok {
		log.L().DPanic("StopRelay for a Free worker should not happen",
			zap.String("worker name", w.baseInfo.Name))
	}
	delete(w.relaySources, source)
	w.reportMetrics()
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

// Bound returns the current source ID bounded to,
// returns null value if not bounded.
func (w *Worker) Bound() ha.SourceBound {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.bound
}

// Bounds returns the bounds info of the worker.
func (w *Worker) Bounds() map[string]ha.SourceBound {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.bounds
}

// RelaySources returns the sources from which this worker is pulling relay log,
// returns empty string if not started relay.
func (w *Worker) RelaySources() map[string]struct{} {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.relaySources
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

// NewMockWorker is used in tests.
func NewMockWorker(cli workerrpc.Client) *Worker {
	return &Worker{cli: cli}
}
