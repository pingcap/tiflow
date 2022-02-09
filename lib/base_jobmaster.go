package lib

import (
	"context"

	"github.com/hanfei1991/microcosm/client"
	"github.com/hanfei1991/microcosm/executor/worker"
	"github.com/hanfei1991/microcosm/model"
	dcontext "github.com/hanfei1991/microcosm/pkg/context"
	"github.com/hanfei1991/microcosm/pkg/metadata"
	"github.com/hanfei1991/microcosm/pkg/p2p"
	"github.com/pingcap/errors"
)

type BaseJobMaster interface {
	BaseMaster
	BaseWorker
}

type defaultBaseJobMaster struct {
	master BaseMaster
	worker BaseWorker
}

func NewBaseJobMaster(
	ctx *dcontext.Context,
	masterImpl MasterImpl,
	workerImpl WorkerImpl,
	masterID MasterID,
	workerID WorkerID,
	messageHandlerManager p2p.MessageHandlerManager,
	messageRouter p2p.MessageSender,
	metaKVClient metadata.MetaKV,
	executorClientManager client.ClientsManager,
	serverMasterClient client.MasterClient,
) BaseJobMaster {
	baseMaster := NewBaseMaster(ctx, masterImpl, masterID, messageHandlerManager, messageRouter, metaKVClient, executorClientManager, serverMasterClient)
	// TODO change "job-manager" to appropriate masterID.
	baseWorker := NewBaseWorker(workerImpl, messageHandlerManager, messageRouter, metaKVClient, workerID, "job-manager")
	return &defaultBaseJobMaster{
		master: baseMaster,
		worker: baseWorker,
	}
}

func (d *defaultBaseJobMaster) MetaKVClient() metadata.MetaKV {
	return d.master.MetaKVClient()
}

func (d *defaultBaseJobMaster) Init(ctx context.Context) error {
	if err := d.worker.Init(ctx); err != nil {
		return errors.Trace(err)
	}
	if err := d.master.Init(ctx); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (d *defaultBaseJobMaster) Poll(ctx context.Context) error {
	if err := d.worker.Poll(ctx); err != nil {
		return errors.Trace(err)
	}
	if err := d.master.Poll(ctx); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (d *defaultBaseJobMaster) MasterID() MasterID {
	return d.master.MasterID()
}

func (d *defaultBaseJobMaster) GetWorkers() map[WorkerID]WorkerHandle {
	return d.master.GetWorkers()
}

func (d *defaultBaseJobMaster) Close(ctx context.Context) error {
	if err := d.master.Close(ctx); err != nil {
		// TODO should we close the worker anyways even if closing the master has failed?
		return errors.Trace(err)
	}
	if err := d.worker.Close(ctx); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (d *defaultBaseJobMaster) OnError(err error) {
	// TODO refine the OnError logic.
	d.master.OnError(err)
}

func (d *defaultBaseJobMaster) CreateWorker(workerType WorkerType, config WorkerConfig, cost model.RescUnit) (WorkerID, error) {
	return d.master.CreateWorker(workerType, config, cost)
}

func (d *defaultBaseJobMaster) GetWorkerStatusExtTypeInfo() interface{} {
	return d.master.GetWorkerStatusExtTypeInfo()
}

func (d *defaultBaseJobMaster) Workload() model.RescUnit {
	return d.worker.Workload()
}

func (d *defaultBaseJobMaster) ID() worker.RunnableID {
	return d.worker.ID()
}
