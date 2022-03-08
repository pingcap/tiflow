package fake

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"time"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"
	"golang.org/x/time/rate"

	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/model"
	dcontext "github.com/hanfei1991/microcosm/pkg/context"
	derrors "github.com/hanfei1991/microcosm/pkg/errors"
)

var _ lib.Worker = (*dummyWorker)(nil)

type (
	Worker = dummyWorker

	WorkerConfig struct {
		TargetTick int64 `json:"target-tick"`
	}

	dummyWorker struct {
		lib.BaseWorker

		init   bool
		closed int32
		status *dummyWorkerStatus
		config *WorkerConfig

		statusRateLimiter *rate.Limiter
	}
)

type dummyWorkerStatus struct {
	Tick int64 `json:"tick"`
}

func (s *dummyWorkerStatus) tick() {
	s.Tick++
}

func (s *dummyWorkerStatus) Marshal() ([]byte, error) {
	return json.Marshal(s)
}

func (s *dummyWorkerStatus) Unmarshal(data []byte) error {
	return json.Unmarshal(data, s)
}

func (d *dummyWorker) InitImpl(ctx context.Context) error {
	if !d.init {
		d.init = true
		return nil
	}
	return errors.New("repeated init")
}

func (d *dummyWorker) Tick(ctx context.Context) error {
	if !d.init {
		return errors.New("not yet init")
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

	if d.status.Tick >= d.config.TargetTick {
		return d.Exit(ctx, d.Status(), nil)
	}

	return nil
}

func (d *dummyWorker) Status() lib.WorkerStatus {
	if d.init {
		extBytes, err := d.status.Marshal()
		if err != nil {
			log.L().Panic("unexpected error", zap.Error(err))
		}
		return lib.WorkerStatus{
			Code:     lib.WorkerStatusNormal,
			ExtBytes: extBytes,
		}
	}
	return lib.WorkerStatus{Code: lib.WorkerStatusCreated}
}

func (d *dummyWorker) Workload() model.RescUnit {
	return model.RescUnit(10)
}

func (d *dummyWorker) OnMasterFailover(_ lib.MasterFailoverReason) error {
	return nil
}

func (d *dummyWorker) CloseImpl(ctx context.Context) error {
	atomic.StoreInt32(&d.closed, 1)
	return nil
}

func NewDummyWorker(
	ctx *dcontext.Context,
	id lib.WorkerID, masterID lib.MasterID,
	cfg lib.WorkerConfig,
) lib.WorkerImpl {
	return &dummyWorker{
		statusRateLimiter: rate.NewLimiter(rate.Every(time.Second*3), 1),
		status:            &dummyWorkerStatus{},
		config:            cfg.(*WorkerConfig),
	}
}
