package fake

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"
	"golang.org/x/time/rate"

	"github.com/hanfei1991/microcosm/executor/worker"
	"github.com/hanfei1991/microcosm/lib"
	libModel "github.com/hanfei1991/microcosm/lib/model"
	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pkg/clock"
	dcontext "github.com/hanfei1991/microcosm/pkg/context"
	derrors "github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/p2p"
)

// Config represents the job config of fake master
type Config struct {
	JobName     string `json:"job-name"`
	WorkerCount int    `json:"worker-count"`
	TargetTick  int    `json:"target-tick"`
}

type checkpoint struct {
	Ticks map[int]int64 `json:"ticks"`
}

func (cp *checkpoint) String() string {
	data, err := json.Marshal(cp)
	if err != nil {
		log.L().Warn("checkpoint marshal failed", zap.Error(err))
	}
	return string(data)
}

var _ lib.BaseJobMaster = (*Master)(nil)

type Master struct {
	lib.BaseJobMaster

	// workerID stores the ID of the Master AS A WORKER.
	workerID libModel.WorkerID

	workerListMu        sync.Mutex
	workerList          []lib.WorkerHandle
	workerID2BusinessID map[libModel.WorkerID]int
	pendingWorkerSet    map[libModel.WorkerID]int
	statusRateLimiter   *rate.Limiter
	status              map[libModel.WorkerID]int64
	finishedSet         map[libModel.WorkerID]int
	config              *Config
	statusCode          struct {
		sync.RWMutex
		code libModel.WorkerStatusCode
	}
	ctx         context.Context
	clocker     clock.Clock
	initialized bool
}

func (m *Master) OnJobManagerFailover(reason lib.MasterFailoverReason) error {
	log.L().Info("FakeMaster: OnJobManagerFailover", zap.Any("reason", reason))
	return nil
}

func (m *Master) OnJobManagerMessage(topic p2p.Topic, message p2p.MessageValue) error {
	log.L().Info("FakeMaster: OnJobManagerMessage", zap.Any("message", message))
	switch msg := message.(type) {
	case *libModel.StatusChangeRequest:
		switch msg.ExpectState {
		case libModel.WorkerStatusStopped:
			m.setStatusCode(libModel.WorkerStatusStopped)
			m.workerListMu.Lock()
			for _, worker := range m.workerList {
				if worker == nil {
					continue
				}
				wTopic := libModel.WorkerStatusChangeRequestTopic(m.BaseJobMaster.ID(), worker.ID())
				wMessage := &libModel.StatusChangeRequest{
					SendTime:     m.clocker.Mono(),
					FromMasterID: m.BaseJobMaster.ID(),
					Epoch:        m.BaseJobMaster.CurrentEpoch(),
					ExpectState:  libModel.WorkerStatusStopped,
				}
				ctx, cancel := context.WithTimeout(m.ctx, time.Second*2)
				runningHandle := worker.Unwrap()
				if runningHandle == nil {
					cancel()
					continue
				}
				if err := runningHandle.SendMessage(ctx, wTopic, wMessage, false /*nonblocking*/); err != nil {
					cancel()
					m.workerListMu.Unlock()
					return err
				}
				cancel()
			}
			m.workerListMu.Unlock()
		default:
			log.L().Info("FakeMaster: ignore status change state", zap.Int32("state", int32(msg.ExpectState)))
		}
	default:
		log.L().Info("unsupported message", zap.Any("message", message))
	}
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
	return m.createWorkers()
}

func (m *Master) createWorker(wcfg *WorkerConfig) error {
	workerID, err := m.CreateWorker(lib.FakeTask, wcfg, 1)
	if err != nil {
		return errors.Trace(err)
	}
	log.L().Info("CreateWorker called",
		zap.Int("BusinessID", wcfg.ID),
		zap.String("worker-id", workerID))
	m.pendingWorkerSet[workerID] = wcfg.ID
	return nil
}

func (m *Master) createWorkers() error {
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
			wcfg := &WorkerConfig{
				ID:         i,
				TargetTick: int64(m.config.TargetTick),
			}
			err := m.createWorker(wcfg)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *Master) tickedCheckWorkers(ctx context.Context) error {
	m.workerListMu.Lock()
	defer m.workerListMu.Unlock()

	// handle failover if needed
	if !m.initialized {
		if !m.IsMasterReady() {
			if m.statusRateLimiter.Allow() {
				log.L().Info("master is not ready, wait")
			}
			return nil
		}
		m.initialized = true
		for _, worker := range m.GetWorkers() {
			if worker.GetTombstone() != nil {
				continue
			}
			var businessID int
			if _, ok := m.workerID2BusinessID[worker.ID()]; ok {
				businessID = m.workerID2BusinessID[worker.ID()]
			} else {
				ws, err := parseExtBytes(worker.Status().ExtBytes)
				if err != nil {
					return errors.Trace(err)
				}
				businessID = ws.BusinessID
			}
			// found active worker after fake_master failover
			if m.workerList[businessID] == nil {
				m.workerList[businessID] = worker
			}
		}
		// load checkpoint if it exists
		ckpt := &checkpoint{}
		resp, metaErr := m.MetaKVClient().Get(ctx, m.checkpointKey())
		if metaErr != nil {
			log.L().Warn("failed to load checkpoint", zap.Error(metaErr))
		} else {
			if len(resp.Kvs) > 0 {
				if err := json.Unmarshal(resp.Kvs[0].Value, ckpt); err != nil {
					return errors.Trace(err)
				}
			}
		}
		for i, worker := range m.workerList {
			// create new worker for non-active worker
			if worker == nil {
				wcfg := &WorkerConfig{
					ID:         i,
					TargetTick: int64(m.config.TargetTick),
				}
				if tick, ok := ckpt.Ticks[i]; ok {
					wcfg.StartTick = tick
				}
				if err := m.createWorker(wcfg); err != nil {
					return errors.Trace(err)
				}
			}
		}
	}

	// load worker status from Status API
	for _, worker := range m.workerList {
		if worker != nil {
			status := worker.Status()
			dws := &dummyWorkerStatus{}
			if status.ExtBytes != nil {
				var err error
				dws, err = parseExtBytes(status.ExtBytes)
				if err != nil {
					return err
				}
			}
			m.status[worker.ID()] = dws.Tick
		}
	}

	return nil
}

func (m *Master) tickedCheckStatus(ctx context.Context) error {
	if m.statusRateLimiter.Allow() {
		log.L().Info("FakeMaster: Tick", zap.Any("status", m.status))
		// save checkpoint, which is used in business only
		_, metaErr := m.MetaKVClient().Put(ctx, m.checkpointKey(), m.genCheckpoint().String())
		if metaErr != nil {
			log.L().Warn("update checkpoint with error", zap.Error(metaErr))
		}
		// update status via framework provided API
		err := m.BaseJobMaster.UpdateJobStatus(ctx, m.Status())
		if derrors.ErrWorkerUpdateStatusTryAgain.Equal(err) {
			log.L().Warn("update status try again later", zap.String("error", err.Error()))
			return nil
		}
		return err
	}

	// check for special worker status
	if m.getStatusCode() == libModel.WorkerStatusStopped {
		log.L().Info("FakeMaster: received pause command, stop now")
		m.setStatusCode(libModel.WorkerStatusStopped)
		return m.Exit(ctx, m.Status(), nil)
	}
	if len(m.finishedSet) == m.config.WorkerCount {
		log.L().Info("FakeMaster: all worker finished, job master exits now")
		m.setStatusCode(libModel.WorkerStatusFinished)
		return m.Exit(ctx, m.Status(), nil)
	}

	return nil
}

func (m *Master) Tick(ctx context.Context) error {
	if err := m.tickedCheckWorkers(ctx); err != nil {
		return err
	}
	return m.tickedCheckStatus(ctx)
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
	m.workerID2BusinessID[worker.ID()] = idx

	return nil
}

func (m *Master) OnWorkerOnline(worker lib.WorkerHandle) error {
	log.L().Info("FakeMaster: OnWorkerOnline",
		zap.String("worker-id", worker.ID()))

	return nil
}

func (m *Master) OnWorkerOffline(worker lib.WorkerHandle, reason error) error {
	index := -1
	for i, handle := range m.workerList {
		if handle.ID() == worker.ID() {
			index = i
			break
		}
	}
	if index < 0 {
		return errors.Errorf("worker(%s) is not found in worker list", worker.ID())
	}

	if derrors.ErrWorkerFinish.Equal(reason) {
		log.L().Info("FakeMaster: OnWorkerOffline: worker finished", zap.String("worker-id", worker.ID()))
		m.finishedSet[worker.ID()] = index
		return nil
	}

	log.L().Info("FakeMaster: OnWorkerOffline",
		zap.String("worker-id", worker.ID()), zap.Error(reason))
	var startTick int64
	if ws, err := parseExtBytes(worker.Status().ExtBytes); err != nil {
		log.L().Warn("failed to parse worker ext bytes", zap.Error(err))
	} else {
		startTick = ws.Tick
	}
	wcfg := &WorkerConfig{
		ID:         index,
		StartTick:  startTick,
		TargetTick: int64(m.config.TargetTick),
	}
	return m.createWorker(wcfg)
}

func (m *Master) OnWorkerMessage(worker lib.WorkerHandle, topic p2p.Topic, message interface{}) error {
	log.L().Info("FakeMaster: OnWorkerMessage",
		zap.String("topic", topic),
		zap.Any("message", message))
	return nil
}

func (m *Master) OnWorkerStatusUpdated(worker lib.WorkerHandle, newStatus *libModel.WorkerStatus) error {
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

func (m *Master) OnMasterMessage(topic p2p.Topic, message p2p.MessageValue) error {
	log.L().Info("FakeMaster: OnMasterMessage", zap.Any("message", message))
	return nil
}

func (m *Master) Status() libModel.WorkerStatus {
	bytes, err := json.Marshal(m.status)
	if err != nil {
		log.L().Panic("unexpected marshal error", zap.Error(err))
	}
	return libModel.WorkerStatus{
		Code:     m.getStatusCode(),
		ExtBytes: bytes,
	}
}

func (m *Master) setStatusCode(code libModel.WorkerStatusCode) {
	m.statusCode.Lock()
	defer m.statusCode.Unlock()
	m.statusCode.code = code
}

func (m *Master) getStatusCode() libModel.WorkerStatusCode {
	m.statusCode.RLock()
	defer m.statusCode.RUnlock()
	return m.statusCode.code
}

func parseExtBytes(data []byte) (*dummyWorkerStatus, error) {
	dws := &dummyWorkerStatus{}
	err := json.Unmarshal(data, dws)
	return dws, err
}

func (m *Master) checkpointKey() string {
	return strings.Join([]string{"fake-master", "checkpoint", m.workerID}, "/")
}

func (m *Master) genCheckpoint() *checkpoint {
	m.workerListMu.Lock()
	defer m.workerListMu.Unlock()
	cp := &checkpoint{Ticks: make(map[int]int64)}
	for wid, tick := range m.status {
		if businessID, ok := m.workerID2BusinessID[wid]; ok {
			cp.Ticks[businessID] = tick
		}
	}
	return cp
}

func NewFakeMaster(ctx *dcontext.Context, workerID libModel.WorkerID, masterID libModel.MasterID, config lib.WorkerConfig) *Master {
	log.L().Info("new fake master", zap.Any("config", config))
	masterConfig := config.(*Config)
	ret := &Master{
		workerID:            workerID,
		pendingWorkerSet:    make(map[libModel.WorkerID]int),
		workerList:          make([]lib.WorkerHandle, masterConfig.WorkerCount),
		workerID2BusinessID: make(map[libModel.WorkerID]int),
		config:              masterConfig,
		statusRateLimiter:   rate.NewLimiter(rate.Every(time.Second*3), 1),
		status:              make(map[libModel.WorkerID]int64),
		finishedSet:         make(map[libModel.WorkerID]int),
		ctx:                 ctx.Context,
		clocker:             clock.New(),
		initialized:         false,
	}
	ret.setStatusCode(libModel.WorkerStatusNormal)
	return ret
}
