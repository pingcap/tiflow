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
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"

	"github.com/pingcap/tiflow/engine/lib"
	libModel "github.com/pingcap/tiflow/engine/lib/model"
	"github.com/pingcap/tiflow/engine/model"
	dcontext "github.com/pingcap/tiflow/engine/pkg/context"
	derrors "github.com/pingcap/tiflow/engine/pkg/errors"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
)

var _ lib.Worker = (*dummyWorker)(nil)

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
		lib.BaseWorker

		init   bool
		closed int32
		status *dummyWorkerStatus
		config *WorkerConfig
		errCh  chan error

		statusRateLimiter *rate.Limiter

		statusCode struct {
			sync.RWMutex
			code libModel.WorkerStatusCode
		}

		startTime time.Time
	}
)

type dummyWorkerStatus struct {
	sync.RWMutex
	BusinessID int               `json:"business-id"`
	Tick       int64             `json:"tick"`
	Checkpoint *workerCheckpoint `json:"checkpoint"`
}

func (s *dummyWorkerStatus) tick() {
	s.Lock()
	defer s.Unlock()
	s.Tick++
}

func (s *dummyWorkerStatus) getEtcdCheckpoint() workerCheckpoint {
	s.RLock()
	defer s.RUnlock()
	return *s.Checkpoint
}

func (s *dummyWorkerStatus) setEtcdCheckpoint(ckpt *workerCheckpoint) {
	s.Lock()
	defer s.Unlock()
	s.Checkpoint = ckpt
}

func (s *dummyWorkerStatus) Marshal() ([]byte, error) {
	s.RLock()
	defer s.RUnlock()
	return json.Marshal(s)
}

func (s *dummyWorkerStatus) Unmarshal(data []byte) error {
	return json.Unmarshal(data, s)
}

func (d *dummyWorker) InitImpl(ctx context.Context) error {
	if !d.init {
		if d.config.EtcdWatchEnable {
			d.bgRunEtcdWatcher(ctx)
		}
		d.init = true
		d.setStatusCode(libModel.WorkerStatusNormal)
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
		log.L().Info("FakeWorker: Tick", zap.String("worker-id", d.ID()), zap.Int64("tick", d.status.Tick))
		err := d.BaseWorker.UpdateStatus(ctx, d.Status())
		if derrors.ErrWorkerUpdateStatusTryAgain.Equal(err) {
			log.L().Warn("update status try again later", zap.String("error", err.Error()))
			return nil
		}
		return err
	}

	if atomic.LoadInt32(&d.closed) == 1 {
		return nil
	}

	if d.getStatusCode() == libModel.WorkerStatusStopped {
		d.setStatusCode(libModel.WorkerStatusStopped)
		return d.Exit(ctx, d.Status(), nil)
	}

	if d.status.Tick >= d.config.TargetTick {
		d.setStatusCode(libModel.WorkerStatusFinished)
		return d.Exit(ctx, d.Status(), nil)
	}

	if d.config.InjectErrorInterval != 0 {
		if time.Since(d.startTime) > d.config.InjectErrorInterval {
			return errors.Errorf("injected error by worker: %d", d.config.ID)
		}
	}
	return nil
}

func (d *dummyWorker) Status() libModel.WorkerStatus {
	if d.init {
		extBytes, err := d.status.Marshal()
		if err != nil {
			log.L().Panic("unexpected error", zap.Error(err))
		}
		return libModel.WorkerStatus{
			Code:     d.getStatusCode(),
			ExtBytes: extBytes,
		}
	}
	return libModel.WorkerStatus{Code: libModel.WorkerStatusCreated}
}

func (d *dummyWorker) Workload() model.RescUnit {
	return model.RescUnit(10)
}

func (d *dummyWorker) OnMasterMessage(topic p2p.Topic, message p2p.MessageValue) error {
	log.L().Info("fakeWorker: OnMasterMessage", zap.Any("message", message))
	switch msg := message.(type) {
	case *libModel.StatusChangeRequest:
		switch msg.ExpectState {
		case libModel.WorkerStatusStopped:
			d.setStatusCode(libModel.WorkerStatusStopped)
		default:
			log.L().Info("FakeWorker: ignore status change state", zap.Int32("state", int32(msg.ExpectState)))
		}
	default:
		log.L().Info("unsupported message", zap.Any("message", message))
	}

	return nil
}

func (d *dummyWorker) CloseImpl(ctx context.Context) error {
	atomic.StoreInt32(&d.closed, 1)
	return nil
}

func (d *dummyWorker) setStatusCode(code libModel.WorkerStatusCode) {
	d.statusCode.Lock()
	defer d.statusCode.Unlock()
	d.statusCode.code = code
}

func (d *dummyWorker) getStatusCode() libModel.WorkerStatusCode {
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
				log.L().Warn("duplicated error", zap.Error(err))
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
		ch := cli.Watch(ctx, key, opts...)
		log.L().Info("start to watch etcd", zap.String("key", key),
			zap.Int64("revision", revision),
			zap.Strings("endpoints", d.config.EtcdEndpoints))
		for resp := range ch {
			if resp.Err() != nil {
				log.L().Warn("watch met error", zap.Error(resp.Err()))
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
	id libModel.WorkerID, masterID libModel.MasterID,
	cfg lib.WorkerConfig,
) lib.WorkerImpl {
	wcfg := cfg.(*WorkerConfig)
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
	}
}
