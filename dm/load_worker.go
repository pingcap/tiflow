package dm

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/dm/unit"
	"github.com/pingcap/tiflow/dm/loader"
	"github.com/pingcap/tiflow/dm/pkg/log"

	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/model"
)

var _ lib.WorkerImpl = &loadWorker{}

type loadWorker struct {
	*lib.DefaultBaseWorker

	cfg        *config.SubTaskConfig
	unitHolder *unitHolder
}

func newLoadWorker(cfg lib.WorkerConfig) lib.Worker {
	subtaskCfg := cfg.(*config.SubTaskConfig)
	return &loadWorker{
		cfg: subtaskCfg,
	}
}

func (d *loadWorker) InitImpl(ctx context.Context) error {
	// `workerName` and `etcdClient` of `NewLightning` are not used in dataflow
	// scenario, we just use readable values here.
	workerName := "dataflow-worker"
	d.unitHolder = newUnitHolder(loader.NewLightning(d.cfg, nil, workerName))
	return errors.Trace(d.unitHolder.init(ctx))
}

func (d *loadWorker) Tick(ctx context.Context) error {
	d.unitHolder.lazyProcess()

	return nil
}

func (d *loadWorker) Status() lib.WorkerStatus {
	hasResult, result := d.unitHolder.getResult()
	if !hasResult {
		return lib.WorkerStatus{Code: lib.WorkerStatusNormal}
	}
	if len(result.Errors) > 0 {
		return lib.WorkerStatus{
			Code:         lib.WorkerStatusError,
			ErrorMessage: unit.JoinProcessErrors(result.Errors),
		}
	}
	return lib.WorkerStatus{Code: lib.WorkerStatusFinished}
}

func (d *loadWorker) Workload() model.RescUnit {
	log.L().Info("loadWorker.Workload")
	return 0
}

func (d *loadWorker) OnMasterFailover(reason lib.MasterFailoverReason) error {
	log.L().Info("loadWorker.OnMasterFailover")
	return nil
}

func (d *loadWorker) CloseImpl(ctx context.Context) error {
	d.unitHolder.close()
	return nil
}
