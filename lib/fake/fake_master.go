package fake

import (
	"context"
	"sync"

	"github.com/hanfei1991/microcosm/executor/worker"

	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/model"
	dcontext "github.com/hanfei1991/microcosm/pkg/context"
	"github.com/hanfei1991/microcosm/pkg/p2p"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"
)

type Config struct{}

var _ lib.BaseJobMaster = (*Master)(nil)

const (
	fakeWorkerCount = 20
)

type Master struct {
	lib.BaseJobMaster

	// workerID stores the ID of the Master AS A WORKER.
	workerID lib.WorkerID

	workerListMu     sync.Mutex
	workerList       [fakeWorkerCount]lib.WorkerHandle
	pendingWorkerSet map[lib.WorkerID]int
	tick             int64
}

func (m *Master) ID() worker.RunnableID {
	return m.workerID
}

func (m *Master) Workload() model.RescUnit {
	return 0
}

func (m *Master) InitImpl(ctx context.Context) error {
	log.L().Info("FakeMaster: Init")
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

func NewFakeMaster(ctx *dcontext.Context, workerID lib.WorkerID, masterID lib.MasterID, _config lib.WorkerConfig) *Master {
	ret := &Master{
		pendingWorkerSet: make(map[lib.WorkerID]int),
	}
	deps := ctx.Dependencies
	base := lib.NewBaseJobMaster(
		ctx,
		ret,
		ret,
		masterID,
		workerID,
		deps.MessageHandlerManager,
		deps.MessageRouter,
		deps.MetaKVClient,
		deps.ExecutorClientManager,
		deps.ServerMasterClient,
	)
	ret.BaseJobMaster = base
	return ret
}
