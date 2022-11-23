// Copyright 2022 PingCAP, Inc.
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

package fake

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/engine/framework"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	dcontext "github.com/pingcap/tiflow/engine/pkg/context"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
)

var _ framework.Worker = (*dummyWorker)(nil)

type (
	// Worker is exposed for unit test
	Worker = dummyWorker

	// WorkerConfig defines config of fake worker
	WorkerConfig struct {
		ID         int   `json:"id"`
		TargetTick int64 `json:"target-tick"`

		EtcdWatchEnable     bool          `json:"etcd-watch-enable"`
		EtcdEndpoints       []string      `json:"etcd-endpoints"`
		EtcdWatchPrefix     string        `json:"etcd-watch-prefix"`
		InjectErrorInterval time.Duration `json:"inject-error-interval"`

		Checkpoint workerCheckpoint `json:"checkpoint"`
	}

	dummyWorker struct {
		framework.BaseWorker

		init      bool
		cancel    context.CancelFunc
		status    *dummyWorkerStatus
		config    *WorkerConfig
		errCh     chan error
		closed    *atomic.Bool
		canceling *atomic.Bool

		statusRateLimiter *rate.Limiter

		statusCode struct {
			sync.RWMutex
			code frameModel.WorkerState
		}

		startTime time.Time
	}
)

type dummyWorkerStatus struct {
	rwm        sync.RWMutex
	BusinessID int               `json:"business-id"`
	Tick       int64             `json:"tick"`
	Checkpoint *workerCheckpoint `json:"checkpoint"`
}

func (s *dummyWorkerStatus) tick() {
	s.rwm.Lock()
	defer s.rwm.Unlock()
	s.Tick++
}

func (s *dummyWorkerStatus) getEtcdCheckpoint() workerCheckpoint {
	s.rwm.RLock()
	defer s.rwm.RUnlock()
	return *s.Checkpoint
}

func (s *dummyWorkerStatus) setEtcdCheckpoint(ckpt *workerCheckpoint) {
	s.rwm.Lock()
	defer s.rwm.Unlock()
	s.Checkpoint = ckpt
}

func (s *dummyWorkerStatus) Marshal() ([]byte, error) {
	s.rwm.RLock()
	defer s.rwm.RUnlock()
	return json.Marshal(s)
}

func (s *dummyWorkerStatus) Unmarshal(data []byte) error {
	return json.Unmarshal(data, s)
}

func (d *dummyWorker) InitImpl(_ context.Context) error {
	if !d.init {
		if d.config.EtcdWatchEnable {
			// Don't use the ctx from the caller, because it may be canceled by the caller after InitImpl() returns.
			ctx, cancel := context.WithCancel(context.Background())
			d.bgRunEtcdWatcher(ctx)
			d.cancel = cancel
		}
		d.init = true
		d.setState(frameModel.WorkerStateNormal)
		d.startTime = time.Now()
		return nil
	}
	return errors.New("repeated init")
}

func (d *dummyWorker) Tick(ctx context.Context) error {
	if !d.init {
		return errors.New("not yet init")
	}

	select {
	case err := <-d.errCh:
		return err
	default:
	}

	d.status.tick()

	if d.statusRateLimiter.Allow() {
		log.Info("FakeWorker: Tick", zap.String("worker-id", d.ID()), zap.Int64("tick", d.status.Tick))
		err := d.BaseWorker.UpdateStatus(ctx, d.Status())
		if err != nil {
			if errors.Is(err, errors.ErrWorkerUpdateStatusTryAgain) {
				log.Warn("update status try again later", zap.String("error", err.Error()))
				return nil
			}
			return err
		}
	}

	if d.closed.Load() {
		return nil
	}

	extMsg, err := d.status.Marshal()
	if err != nil {
		return err
	}

	if d.canceling.Load() {
		d.setState(frameModel.WorkerStateStopped)
		return d.Exit(ctx, framework.ExitReasonCanceled, nil, extMsg)
	}

	if d.status.Tick >= d.config.TargetTick {
		d.setState(frameModel.WorkerStateFinished)
		return d.Exit(ctx, framework.ExitReasonFinished, nil, extMsg)
	}

	if d.config.InjectErrorInterval != 0 {
		if time.Since(d.startTime) > d.config.InjectErrorInterval {
			return errors.Errorf("injected error by worker: %d", d.config.ID)
		}
	}
	return nil
}

func (d *dummyWorker) Status() frameModel.WorkerStatus {
	if d.init {
		extBytes, err := d.status.Marshal()
		if err != nil {
			log.Panic("unexpected error", zap.Error(err))
		}
		return frameModel.WorkerStatus{
			State:    d.getState(),
			ExtBytes: extBytes,
		}
	}
	return frameModel.WorkerStatus{State: frameModel.WorkerStateCreated}
}

func (d *dummyWorker) OnMasterMessage(ctx context.Context, topic p2p.Topic, message p2p.MessageValue) error {
	log.Info("fakeWorker: OnMasterMessage", zap.Any("message", message))
	switch msg := message.(type) {
	case *frameModel.StatusChangeRequest:
		switch msg.ExpectState {
		case frameModel.WorkerStateStopped:
			d.canceling.Store(true)
		default:
			log.Info("FakeWorker: ignore status change state", zap.Int32("state", int32(msg.ExpectState)))
		}
	default:
		log.Info("unsupported message", zap.Any("message", message))
	}

	return nil
}

func (d *dummyWorker) CloseImpl(ctx context.Context) {
	if d.closed.CompareAndSwap(false, true) {
		if d.cancel != nil {
			d.cancel()
		}
	}
}

func (d *dummyWorker) setState(code frameModel.WorkerState) {
	d.statusCode.Lock()
	defer d.statusCode.Unlock()
	d.statusCode.code = code
}

func (d *dummyWorker) getState() frameModel.WorkerState {
	d.statusCode.RLock()
	defer d.statusCode.RUnlock()
	return d.statusCode.code
}

func (d *dummyWorker) bgRunEtcdWatcher(ctx context.Context) {
	go func() {
		if err := d.createEtcdWatcher(ctx); err != nil {
			select {
			case d.errCh <- err:
			default:
				log.Warn("duplicated error", zap.Error(err))
			}
		}
	}()
}

func (d *dummyWorker) createEtcdWatcher(ctx context.Context) error {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   d.config.EtcdEndpoints,
		Context:     ctx,
		DialTimeout: 3 * time.Second,
		DialOptions: []grpc.DialOption{},
	})
	if err != nil {
		return errors.Trace(err)
	}
	key := fmt.Sprintf("%s%d", d.config.EtcdWatchPrefix, d.config.ID)
watchLoop:
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		default:
		}
		opts := make([]clientv3.OpOption, 0)
		revision := d.status.getEtcdCheckpoint().Revision
		if revision > 0 {
			opts = append(opts, clientv3.WithRev(revision+1))
		}
		ch := cli.Watch(clientv3.WithRequireLeader(ctx), key, opts...)
		log.Info("start to watch etcd", zap.String("key", key),
			zap.Int64("revision", revision),
			zap.Strings("endpoints", d.config.EtcdEndpoints))
		for resp := range ch {
			if resp.Err() != nil {
				log.Warn("watch met error", zap.Error(resp.Err()))
				continue watchLoop
			}
			for _, event := range resp.Events {
				// no concurrent write of this checkpoint, so it is safe to read
				// old value, change it and overwrite.
				ckpt := d.status.getEtcdCheckpoint()
				ckpt.MvccCount++
				ckpt.Revision = event.Kv.ModRevision
				switch event.Type {
				case mvccpb.PUT:
					ckpt.Value = string(event.Kv.Value)
				case mvccpb.DELETE:
					ckpt.Value = ""
				}
				d.status.setEtcdCheckpoint(&ckpt)
			}
		}
	}
}

// NewDummyWorker creates a new dummy worker instance
func NewDummyWorker(
	ctx *dcontext.Context,
	id frameModel.WorkerID, masterID frameModel.MasterID,
	wcfg *WorkerConfig,
) framework.WorkerImpl {
	status := &dummyWorkerStatus{
		BusinessID: wcfg.ID,
		Tick:       wcfg.Checkpoint.Tick,
		Checkpoint: &workerCheckpoint{
			Revision:  wcfg.Checkpoint.Revision,
			MvccCount: wcfg.Checkpoint.MvccCount,
			Value:     wcfg.Checkpoint.Value,
		},
	}
	return &dummyWorker{
		statusRateLimiter: rate.NewLimiter(rate.Every(100*time.Millisecond), 1),
		status:            status,
		config:            wcfg,
		errCh:             make(chan error, 1),
		closed:            atomic.NewBool(false),
		canceling:         atomic.NewBool(false),
	}
}
