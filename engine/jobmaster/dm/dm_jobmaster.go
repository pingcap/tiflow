package dm

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"

	"github.com/hanfei1991/microcosm/executor/worker"
	"github.com/hanfei1991/microcosm/jobmaster/dm/checkpoint"
	"github.com/hanfei1991/microcosm/jobmaster/dm/config"
	"github.com/hanfei1991/microcosm/jobmaster/dm/metadata"
	"github.com/hanfei1991/microcosm/jobmaster/dm/runtime"
	"github.com/hanfei1991/microcosm/lib"
	libModel "github.com/hanfei1991/microcosm/lib/model"
	"github.com/hanfei1991/microcosm/lib/registry"
	"github.com/hanfei1991/microcosm/model"
	dcontext "github.com/hanfei1991/microcosm/pkg/context"
	dmpkg "github.com/hanfei1991/microcosm/pkg/dm"
	"github.com/hanfei1991/microcosm/pkg/p2p"
)

type JobMaster struct {
	lib.BaseJobMaster

	workerID libModel.WorkerID
	jobCfg   *config.JobCfg
	wg       sync.WaitGroup
	closeCh  chan struct{}

	metadata              *metadata.MetaData
	workerManager         *WorkerManager
	taskManager           *TaskManager
	messageAgent          *MessageAgent
	messageHandlerManager p2p.MessageHandlerManager
	checkpointAgent       checkpoint.Agent
}

type dmJobMasterFactory struct{}

func RegisterWorker() {
	registry.GlobalWorkerRegistry().MustRegisterWorkerType(lib.DMJobMaster, dmJobMasterFactory{})
}

func (j dmJobMasterFactory) DeserializeConfig(configBytes []byte) (registry.WorkerConfig, error) {
	cfg := &config.JobCfg{}
	err := cfg.Decode(configBytes)
	return cfg, err
}

func (j dmJobMasterFactory) NewWorkerImpl(ctx *dcontext.Context, workerID libModel.WorkerID, masterID libModel.MasterID, conf lib.WorkerConfig) (lib.WorkerImpl, error) {
	log.L().Info("new dm jobmaster", zap.String("id", workerID))
	jm := &JobMaster{
		workerID:        workerID,
		jobCfg:          conf.(*config.JobCfg),
		closeCh:         make(chan struct{}),
		checkpointAgent: checkpoint.NewAgentImpl(conf.(*config.JobCfg)),
	}

	// TODO: we should expose the message handler register Func in base master.
	// nolint:errcheck
	ctx.Deps().Construct(func(m p2p.MessageHandlerManager) (p2p.MessageHandlerManager, error) {
		jm.messageHandlerManager = m
		return m, nil
	})
	return jm, nil
}

func (jm *JobMaster) createComponents() error {
	log.L().Debug("create components", zap.String("id", jm.workerID))
	taskStatus, workerStatus, workerHandles, err := jm.getInitStatus()
	if err != nil {
		return err
	}
	jm.metadata = metadata.NewMetaData(jm.ID(), jm.MetaKVClient())
	jm.messageAgent = NewMessageAgent(workerHandles, jm.ID(), jm.BaseJobMaster)
	jm.taskManager = NewTaskManager(taskStatus, jm.metadata.JobStore(), jm.messageAgent)
	jm.workerManager = NewWorkerManager(workerStatus, jm.metadata.JobStore(), jm.messageAgent, jm.checkpointAgent)
	return nil
}

func (jm *JobMaster) InitImpl(ctx context.Context) error {
	log.L().Info("initializing the dm jobmaster", zap.String("id", jm.workerID), zap.String("jobmaster_id", jm.JobMasterID()))
	if err := jm.createComponents(); err != nil {
		return err
	}
	if err := jm.registerMessageHandler(ctx); err != nil {
		return err
	}
	if err := jm.checkpointAgent.Init(ctx); err != nil {
		return err
	}
	return jm.taskManager.OperateTask(ctx, Create, jm.jobCfg, nil)
}

func (jm *JobMaster) Tick(ctx context.Context) error {
	jm.workerManager.Tick(ctx)
	jm.taskManager.Tick(ctx)
	return nil
}

func (jm *JobMaster) OnMasterRecovered(ctx context.Context) error {
	log.L().Info("recovering the dm jobmaster", zap.String("id", jm.workerID))
	return jm.createComponents()
}

func (jm *JobMaster) OnWorkerDispatched(worker lib.WorkerHandle, result error) error {
	log.L().Info("on worker dispatched", zap.String("id", jm.workerID), zap.String("worker_id", worker.ID()))
	if result != nil {
		log.L().Error("failed to create worker", zap.String("worker_id", worker.ID()), zap.Error(result))
		jm.workerManager.removeWorkerStatusByWorkerID(worker.ID())
		jm.workerManager.SetNextCheckTime(time.Now())
	}
	return nil
}

func (jm *JobMaster) OnWorkerOnline(worker lib.WorkerHandle) error {
	log.L().Debug("on worker online", zap.String("id", jm.workerID), zap.String("worker_id", worker.ID()))
	taskStatus, err := runtime.UnmarshalTaskStatus(worker.Status().ExtBytes)
	if err != nil {
		return err
	}

	jm.taskManager.UpdateTaskStatus(taskStatus)
	jm.workerManager.UpdateWorkerStatus(runtime.NewWorkerStatus(taskStatus.GetTask(), taskStatus.GetUnit(), worker.ID(), runtime.WorkerOnline))
	jm.messageAgent.UpdateWorkerHandle(taskStatus.GetTask(), worker.Unwrap())
	return nil
}

func (jm *JobMaster) OnWorkerOffline(worker lib.WorkerHandle, reason error) error {
	log.L().Info("on worker offline", zap.String("id", jm.workerID), zap.String("worker_id", worker.ID()))
	taskStatus, err := runtime.UnmarshalTaskStatus(worker.Status().ExtBytes)
	if err != nil {
		return err
	}

	if taskStatus.GetStage() == metadata.StageFinished {
		return jm.onWorkerFinished(taskStatus, worker)
	}
	jm.taskManager.UpdateTaskStatus(runtime.NewOfflineStatus(taskStatus.GetTask()))
	jm.workerManager.UpdateWorkerStatus(runtime.NewWorkerStatus(taskStatus.GetTask(), taskStatus.GetUnit(), worker.ID(), runtime.WorkerOffline))
	jm.messageAgent.UpdateWorkerHandle(taskStatus.GetTask(), nil)
	jm.workerManager.SetNextCheckTime(time.Now())
	return nil
}

func (jm *JobMaster) onWorkerFinished(taskStatus runtime.TaskStatus, worker lib.WorkerHandle) error {
	log.L().Info("on worker finished", zap.String("id", jm.workerID), zap.String("worker_id", worker.ID()))
	jm.taskManager.UpdateTaskStatus(taskStatus)
	jm.workerManager.UpdateWorkerStatus(runtime.NewWorkerStatus(taskStatus.GetTask(), taskStatus.GetUnit(), worker.ID(), runtime.WorkerFinished))
	jm.messageAgent.UpdateWorkerHandle(taskStatus.GetTask(), nil)
	jm.workerManager.SetNextCheckTime(time.Now())
	return nil
}

func (jm *JobMaster) OnWorkerStatusUpdated(worker lib.WorkerHandle, newStatus *libModel.WorkerStatus) error {
	// No need to do anything here, because we update it in OnWorkerOnline
	return nil
}

func (jm *JobMaster) OnJobManagerMessage(topic p2p.Topic, message interface{}) error {
	// TODO: receive user request
	return nil
}

func (jm *JobMaster) OnWorkerMessage(worker lib.WorkerHandle, topic p2p.Topic, message interface{}) error {
	log.L().Debug("on worker message", zap.String("id", jm.workerID), zap.String("worker_id", worker.ID()))
	// TODO: handle DDL request
	response, ok := message.(dmpkg.MessageWithID)
	if !ok {
		return errors.Errorf("unexpected message type %T", message)
	}
	return jm.messageAgent.OnWorkerMessage(response)
}

func (jm *JobMaster) OnMasterMessage(topic p2p.Topic, message interface{}) error {
	return nil
}

func (jm *JobMaster) CloseImpl(ctx context.Context) error {
	log.L().Info("close the dm jobmaster", zap.String("id", jm.workerID))
	if err := jm.taskManager.OperateTask(ctx, Delete, nil, nil); err != nil {
		return err
	}

outer:
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
			// wait all worker offline
			jm.workerManager.SetNextCheckTime(time.Now())
			// manually call Tick since outer event loop is closed.
			jm.workerManager.Tick(ctx)
			if len(jm.workerManager.WorkerStatus()) == 0 {
				if err := jm.checkpointAgent.Remove(ctx); err != nil {
					log.L().Error("failed to remove checkpoint", zap.Error(err))
				}
				break outer
			}
		}
	}

	// place holder
	close(jm.closeCh)
	jm.wg.Wait()
	return nil
}

func (jm *JobMaster) ID() worker.RunnableID {
	return jm.workerID
}

func (jm *JobMaster) Workload() model.RescUnit {
	// TODO: implement workload
	return 2
}

func (jm *JobMaster) OnMasterFailover(reason lib.MasterFailoverReason) error {
	// No need to do anything here
	return nil
}

func (jm *JobMaster) OnJobManagerFailover(reason lib.MasterFailoverReason) error {
	// No need to do anything here
	return nil
}

func (jm *JobMaster) IsJobMasterImpl() {
	panic("unreachable")
}

func (jm *JobMaster) registerMessageHandler(ctx context.Context) error {
	log.L().Debug("register message handler", zap.String("id", jm.workerID))
	// TODO: register jobmanager request and worker request/response
	//	ok, err := jm.messageHandlerManager.RegisterHandler(
	//		ctx,
	//		"",
	//		nil,
	//		func(sender p2p.NodeID, value p2p.MessageValue) error {
	//			return jm.OnWorkerMessage(workerHandle, topic , message)
	//		},
	//	)
	return nil
}

func (jm *JobMaster) getInitStatus() ([]runtime.TaskStatus, []runtime.WorkerStatus, map[string]SendHandle, error) {
	log.L().Debug("get init status", zap.String("id", jm.workerID))
	// NOTE: GetWorkers should return all online workers,
	// and no further OnWorkerOnline will be received if JobMaster doesn't CreateWorker.
	workerHandles := jm.GetWorkers()
	taskStatusList := make([]runtime.TaskStatus, 0, len(workerHandles))
	workerStatusList := make([]runtime.WorkerStatus, 0, len(workerHandles))
	sendHandleMap := make(map[string]SendHandle, len(workerHandles))
	for _, workerHandle := range workerHandles {
		if workerHandle.GetTombstone() != nil {
			continue
		}
		taskStatus, err := runtime.UnmarshalTaskStatus(workerHandle.Status().ExtBytes)
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}
		taskStatusList = append(taskStatusList, taskStatus)
		workerStatusList = append(workerStatusList, runtime.NewWorkerStatus(taskStatus.GetTask(), taskStatus.GetUnit(), workerHandle.ID(), runtime.WorkerOnline))
		sendHandleMap[taskStatus.GetTask()] = workerHandle.Unwrap()
	}

	return taskStatusList, workerStatusList, sendHandleMap, nil
}
