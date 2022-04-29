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

	"github.com/hanfei1991/microcosm/lib"
	libModel "github.com/hanfei1991/microcosm/lib/model"
	"github.com/hanfei1991/microcosm/model"
	dcontext "github.com/hanfei1991/microcosm/pkg/context"
	derrors "github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/p2p"
)

var _ lib.Worker = (*dummyWorker)(nil)

type (
	Worker = dummyWorker

	WorkerConfig struct {
		ID         int   `json:"id"`
		TargetTick int64 `json:"target-tick"`
		StartTick  int64 `json:"start-tick"`

		EtcdWatchEnable   bool     `json:"etcd-watch-enable"`
		EtcdEndpoints     []string `json:"etcd-endpoints"`
		EtcdWatchPrefix   string   `json:"etcd-watch-prefix"`
		EtcdWatchRevision int64    `json:"etcd-watch-revision"`
	}

	EtcdCheckpoint struct {
		Revision int64  `json:"revision"`
		Mvcc     int    `json:"mvcc"` // record mvcc version count
		Value    string `json:"value"`
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
	}
)

type dummyWorkerStatus struct {
	sync.RWMutex
	BusinessID     int             `json:"business-id"`
	Tick           int64           `json:"tick"`
	EtcdCheckpoint *EtcdCheckpoint `json:"etcd-checkpoint"`
}

func (s *dummyWorkerStatus) tick() {
	s.Lock()
	defer s.Unlock()
	s.Tick++
}

func (s *dummyWorkerStatus) getEtcdCheckpoint() EtcdCheckpoint {
	s.RLock()
	defer s.RUnlock()
	return *s.EtcdCheckpoint
}

func (s *dummyWorkerStatus) setEtcdCheckpoint(ckpt *EtcdCheckpoint) {
	s.Lock()
	defer s.Unlock()
	s.EtcdCheckpoint = ckpt
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

func (d *dummyWorker) OnMasterFailover(_ lib.MasterFailoverReason) error {
	return nil
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
		ch := cli.Watch(ctx, key, clientv3.WithRev(d.status.getEtcdCheckpoint().Revision))
		for resp := range ch {
			if resp.Err() != nil {
				log.L().Warn("watch met error", zap.Error(resp.Err()))
				continue watchLoop
			}
			for _, event := range resp.Events {
				// no concurrent write of this checkpoint, so it is safe to read
				// old value, change it and overwrite.
				ckpt := d.status.getEtcdCheckpoint()
				ckpt.Mvcc++
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

func NewDummyWorker(
	ctx *dcontext.Context,
	id libModel.WorkerID, masterID libModel.MasterID,
	cfg lib.WorkerConfig,
) lib.WorkerImpl {
	wcfg := cfg.(*WorkerConfig)
	status := &dummyWorkerStatus{
		BusinessID:     wcfg.ID,
		Tick:           wcfg.StartTick,
		EtcdCheckpoint: &EtcdCheckpoint{Revision: wcfg.EtcdWatchRevision},
	}
	return &dummyWorker{
		statusRateLimiter: rate.NewLimiter(rate.Every(time.Second*3), 1),
		status:            status,
		config:            wcfg,
		errCh:             make(chan error, 1),
	}
}
