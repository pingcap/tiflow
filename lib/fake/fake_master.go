package fake

import (
	"context"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"

	"github.com/hanfei1991/microcosm/executor/worker"
	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/model"
	dcontext "github.com/hanfei1991/microcosm/pkg/context"
	"github.com/hanfei1991/microcosm/pkg/p2p"
)

// Config represents the job config of fake master
type Config struct {
	JobName     string `json:"job-name"`
	WorkerCount int    `json:"worker-count"`
}

var _ lib.BaseJobMaster = (*Master)(nil)

type Master struct {
	lib.BaseJobMaster

	// workerID stores the ID of the Master AS A WORKER.
	workerID lib.WorkerID

	workerListMu     sync.Mutex
	workerList       []lib.WorkerHandle
	pendingWorkerSet map[lib.WorkerID]int
	tick             int64
	config           *Config
}

func (m *Master) OnJobManagerFailover(reason lib.MasterFailoverReason) error {
	log.L().Info("FakeMaster: OnJobManagerFailover", zap.Any("reason", reason))
	return nil
}

func (m *Master) IsJobMasterImpl() {
	panic("unreachable")
}

func (m *Master) ID() worker.RunnableID {
	return m.workerID
}

func (m *Master) Workload() model.RescUnit {
	return 0
}

func (m *Master) InitImpl(ctx context.Context) error {
	log.L().Info("FakeMaster: Init", zap.Any("config", m.config))
	m.workerList = make([]lib.WorkerHandle, m.config.WorkerCount)
	return nil
}

func (m *Master) Tick(ctx context.Context) error {
	m.tick++
	if m.tick%200 == 0 {
		log.L().Info("FakeMaster: Tick", zap.Int64("tick", m.tick))
	}

	m.workerListMu.Lock()
	defer m.workerListMu.Unlock()
OUT:
	for i, handle := range m.workerList {
		if handle == nil {
			for _, idx := range m.pendingWorkerSet {
				if idx == i {
					continue OUT
				}
			}

			workerID, err := m.CreateWorker(lib.FakeTask, &Config{}, 1)
			if err != nil {
				return errors.Trace(err)
			}
			log.L().Info("CreateWorker called",
				zap.Int("index", i),
				zap.String("worker-id", workerID))
			m.pendingWorkerSet[workerID] = i
		}
	}

	return nil
}

func (m *Master) OnMasterRecovered(ctx context.Context) error {
	log.L().Info("FakeMaster: OnMasterRecovered")
	return nil
}

func (m *Master) OnWorkerDispatched(worker lib.WorkerHandle, result error) error {
	if result != nil {
		log.L().Error("FakeMaster: OnWorkerDispatched", zap.Error(result))
		return errors.Trace(result)
	}

	log.L().Info("FakeMaster: OnWorkerDispatched",
		zap.String("worker-id", worker.ID()),
		zap.Error(result))

	m.workerListMu.Lock()
	defer m.workerListMu.Unlock()

	idx, ok := m.pendingWorkerSet[worker.ID()]
	if !ok {
		log.L().Panic("OnWorkerDispatched is called with an unknown workerID",
			zap.String("worker-id", worker.ID()))
	}
	delete(m.pendingWorkerSet, worker.ID())
	m.workerList[idx] = worker

	return nil
}

func (m *Master) OnWorkerOnline(worker lib.WorkerHandle) error {
	log.L().Info("FakeMaster: OnWorkerOnline",
		zap.String("worker-id", worker.ID()))

	return nil
}

func (m *Master) OnWorkerOffline(worker lib.WorkerHandle, reason error) error {
	log.L().Info("FakeMaster: OnWorkerOffline",
		zap.String("worker-id", worker.ID()),
		zap.Error(reason))

	// TODO handle offlined workers
	return nil
}

func (m *Master) OnWorkerMessage(worker lib.WorkerHandle, topic p2p.Topic, message interface{}) error {
	log.L().Info("FakeMaster: OnWorkerMessage",
		zap.String("topic", topic),
		zap.Any("message", message))
	return nil
}

func (m *Master) CloseImpl(ctx context.Context) error {
	log.L().Info("FakeMaster: Close", zap.Stack("stack"))
	return nil
}

func (m *Master) OnMasterFailover(reason lib.MasterFailoverReason) error {
	log.L().Info("FakeMaster: OnMasterFailover", zap.Stack("stack"))
	return nil
}

func (m *Master) Status() lib.WorkerStatus {
	return lib.WorkerStatus{Code: lib.WorkerStatusNormal}
}

func NewFakeMaster(ctx *dcontext.Context, workerID lib.WorkerID, masterID lib.MasterID, config lib.WorkerConfig) *Master {
	log.L().Info("new fake master", zap.Any("config", config))
	ret := &Master{
		pendingWorkerSet: make(map[lib.WorkerID]int),
		config:           config.(*Config),
	}
	base := lib.NewBaseJobMaster(
		ctx,
		ret,
		masterID,
		workerID,
	)
	ret.BaseJobMaster = base
	return ret
}
