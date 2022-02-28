package dm

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/loader"
	"github.com/pingcap/tiflow/dm/pkg/log"

	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/model"
)

var _ lib.Worker = &loadWorker{}

type loadWorker struct {
	lib.BaseWorker

	cfg        *config.SubTaskConfig
	unitHolder *unitHolder
}

func newLoadWorker(cfg lib.WorkerConfig) lib.WorkerImpl {
	subtaskCfg := cfg.(*config.SubTaskConfig)
	return &loadWorker{
		cfg: subtaskCfg,
	}
}

func (l *loadWorker) InitImpl(ctx context.Context) error {
	log.L().Info("init load worker")
	// `workerName` and `etcdClient` of `NewLightning` are not used in dataflow
	// scenario, we just use readable values here.
	workerName := "dataflow-worker"
	l.unitHolder = newUnitHolder(loader.NewLightning(l.cfg, nil, workerName))
	return errors.Trace(l.unitHolder.init(ctx))
}

func (l *loadWorker) Tick(ctx context.Context) error {
	l.unitHolder.lazyProcess()
	return l.unitHolder.tryUpdateStatus(ctx, l.BaseWorker)
}

func (l *loadWorker) Workload() model.RescUnit {
	log.L().Info("loadWorker.Workload")
	return 0
}

func (l *loadWorker) OnMasterFailover(reason lib.MasterFailoverReason) error {
	log.L().Info("loadWorker.OnMasterFailover")
	return nil
}

func (l *loadWorker) CloseImpl(ctx context.Context) error {
	l.unitHolder.close()
	return nil
}
