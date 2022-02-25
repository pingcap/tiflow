package dm

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/dm/unit"
	"github.com/pingcap/tiflow/dm/dumpling"
	"github.com/pingcap/tiflow/dm/loader"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/syncer"
	"go.uber.org/zap"

	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/pkg/p2p"
)

var _ lib.JobMasterImpl = &SubTaskMaster{}

// TODO: this is not the final jobmaster! I just want to make a runnable test.
type SubTaskMaster struct {
	lib.BaseJobMaster

	cfg *config.SubTaskConfig
	// workerSeq are different WorkerTypes that should be run one after another
	workerSeq    []lib.WorkerType
	currWorkerID lib.WorkerID
}

func newSubTaskMaster(
	base lib.BaseJobMaster,
	cfg lib.WorkerConfig,
) *SubTaskMaster {
	subtaskCfg := cfg.(*config.SubTaskConfig)
	return &SubTaskMaster{
		BaseJobMaster: base,
		cfg:           subtaskCfg,
	}
}

func (s *SubTaskMaster) InitImpl(ctx context.Context) error {
	switch s.cfg.Mode {
	case config.ModeAll:
		s.workerSeq = []lib.WorkerType{
			lib.WorkerDMDump,
			lib.WorkerDMLoad,
			lib.WorkerDMSync,
		}
	case config.ModeFull:
		s.workerSeq = []lib.WorkerType{
			lib.WorkerDMDump,
			lib.WorkerDMLoad,
		}
	case config.ModeIncrement:
		s.workerSeq = []lib.WorkerType{
			lib.WorkerDMSync,
		}
	default:
		return errors.Errorf("unknown mode: %s", s.cfg.Mode)
	}

	// build DM Units to make use of IsFresh, to skip finished units
	unitSeq := make([]unit.Unit, 0, len(s.workerSeq))
	for _, tp := range s.workerSeq {
		u := s.buildDMUnit(tp)
		err := u.Init(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		unitSeq = append(unitSeq, u)
	}
	defer func() {
		for _, u := range unitSeq {
			u.Close()
		}
	}()

	lastNotFresh := 0
	for i := len(unitSeq) - 1; i >= 0; i-- {
		isFresh, err := unitSeq[i].IsFreshTask(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		if !isFresh {
			lastNotFresh = i
		}
	}

	s.workerSeq = s.workerSeq[lastNotFresh:]
	if len(s.workerSeq) == 0 {
		return nil
	}
	log.L().Info("workerSequence", zap.Any("workerSeq", s.workerSeq))
	var err error
	s.currWorkerID, err = s.CreateWorker(s.workerSeq[0], s.cfg, 0)
	return errors.Trace(err)
}

func (s *SubTaskMaster) buildDMUnit(tp lib.WorkerType) unit.Unit {
	switch tp {
	case lib.WorkerDMDump:
		return dumpling.NewDumpling(s.cfg)
	case lib.WorkerDMLoad:
		if s.cfg.NeedUseLightning() {
			return loader.NewLightning(s.cfg, nil, "subtask-master")
		}
		return loader.NewLoader(s.cfg, nil, "subtask-master")
	case lib.WorkerDMSync:
		return syncer.NewSyncer(s.cfg, nil, nil)
	}
	return nil
}

func (s *SubTaskMaster) Tick(ctx context.Context) error {
	status := s.GetWorkers()[s.currWorkerID].Status()
	switch status.Code {
	case lib.WorkerStatusFinished:
		log.L().Info("worker finished", zap.String("currWorkerID", s.currWorkerID))
		if len(s.workerSeq) > 0 {
			s.workerSeq = s.workerSeq[1:]
			if len(s.workerSeq) > 0 {
				var err error
				s.currWorkerID, err = s.CreateWorker(s.workerSeq[0], s.cfg, 0)
				if err != nil {
					return errors.Trace(err)
				}
			} else {
				return lib.StopAfterTick
			}
		}
	case lib.WorkerStatusError:
		// TODO: will print lots of logs, should find a way to expose the error
		log.L().Info("worker error", zap.String("message", status.ErrorMessage))
	}
	return nil
}

func (s *SubTaskMaster) OnMasterRecovered(ctx context.Context) error {
	log.L().Info("on master recovered")
	return nil
}

func (s *SubTaskMaster) OnWorkerDispatched(worker lib.WorkerHandle, result error) error {
	log.L().Info("on worker dispatched")
	return nil
}

func (s *SubTaskMaster) OnWorkerOnline(worker lib.WorkerHandle) error {
	log.L().Info("on worker online")
	return nil
}

func (s *SubTaskMaster) OnWorkerOffline(worker lib.WorkerHandle, reason error) error {
	log.L().Info("on worker offline")
	return nil
}

func (s *SubTaskMaster) OnWorkerMessage(worker lib.WorkerHandle, topic p2p.Topic, message interface{}) error {
	log.L().Info("on worker message")
	return nil
}

func (s *SubTaskMaster) CloseImpl(ctx context.Context) error {
	log.L().Info("close")
	return nil
}

func (s *SubTaskMaster) OnJobManagerFailover(reason lib.MasterFailoverReason) error {
	log.L().Info("on job manager failover")
	return nil
}

func (s *SubTaskMaster) IsJobMasterImpl() {
	log.L().Info("is job master impl")
}
