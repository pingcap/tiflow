package dm

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/dumpling"
	"github.com/pingcap/tiflow/dm/pkg/log"

	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/model"
)

var _ lib.Worker = &dumpWorker{}

type dumpWorker struct {
	lib.BaseWorker

	cfg        *config.SubTaskConfig
	unitHolder *unitHolder
}

func newDumpWorker(cfg lib.WorkerConfig) lib.WorkerImpl {
	subtaskCfg := cfg.(*config.SubTaskConfig)
	return &dumpWorker{
		cfg: subtaskCfg,
	}
}

func (d *dumpWorker) InitImpl(ctx context.Context) error {
	log.L().Info("init dump worker")
	d.unitHolder = newUnitHolder(dumpling.NewDumpling(d.cfg))
	return errors.Trace(d.unitHolder.init(ctx))
}

func (d *dumpWorker) Tick(ctx context.Context) error {
	d.unitHolder.lazyProcess()
	return d.unitHolder.tryUpdateStatus(ctx, d.BaseWorker)
}

func (d *dumpWorker) Workload() model.RescUnit {
	log.L().Info("dumpWorker.Workload")
	return 0
}

func (d *dumpWorker) OnMasterFailover(reason lib.MasterFailoverReason) error {
	log.L().Info("dumpWorker.OnMasterFailover")
	return nil
}

func (d *dumpWorker) CloseImpl(ctx context.Context) error {
	d.unitHolder.close()
	return nil
}
