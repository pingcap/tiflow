package dm

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/dm/unit"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/syncer"

	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/model"
)

var _ lib.Worker = &syncWorker{}

type syncWorker struct {
	lib.BaseWorker

	cfg        *config.SubTaskConfig
	unitHolder *unitHolder
}

func newSyncWorker(base lib.BaseWorker, cfg lib.WorkerConfig) lib.WorkerImpl {
	subtaskCfg := cfg.(*config.SubTaskConfig)
	return &syncWorker{
		BaseWorker: base,
		cfg:        subtaskCfg,
	}
}

func (s *syncWorker) InitImpl(ctx context.Context) error {
	s.unitHolder = newUnitHolder(syncer.NewSyncer(s.cfg, nil, nil))
	return errors.Trace(s.unitHolder.init(ctx))
}

func (s *syncWorker) Tick(ctx context.Context) error {
	s.unitHolder.lazyProcess()

	return nil
}

func (s *syncWorker) Status() lib.WorkerStatus {
	hasResult, result := s.unitHolder.getResult()
	if !hasResult {
		return lib.WorkerStatus{Code: lib.WorkerStatusNormal}
	}
	if len(result.Errors) > 0 {
		return lib.WorkerStatus{
			Code:         lib.WorkerStatusError,
			ErrorMessage: unit.JoinProcessErrors(result.Errors),
		}
	}
	// should not happen, syncer will run continuously.
	return lib.WorkerStatus{Code: lib.WorkerStatusFinished}
}

func (s *syncWorker) Workload() model.RescUnit {
	log.L().Info("syncWorker.Workload")
	return 0
}

func (s *syncWorker) OnMasterFailover(reason lib.MasterFailoverReason) error {
	log.L().Info("syncWorker.OnMasterFailover")
	return nil
}

func (s *syncWorker) CloseImpl(ctx context.Context) error {
	s.unitHolder.close()
	return nil
}
