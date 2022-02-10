package servermaster

import (
	"context"
	"sync"

	"github.com/hanfei1991/microcosm/client"
	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/lib/registry"
	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pb"
	dcontext "github.com/hanfei1991/microcosm/pkg/context"
	"github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/metadata"
	"github.com/hanfei1991/microcosm/pkg/p2p"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"
)

// JobManager defines manager of job master
type JobManager interface {
	Start(ctx context.Context, metaKV metadata.MetaKV) error
	SubmitJob(ctx context.Context, req *pb.SubmitJobRequest) *pb.SubmitJobResponse
	CancelJob(ctx context.Context, req *pb.CancelJobRequest) *pb.CancelJobResponse
	PauseJob(ctx context.Context, req *pb.PauseJobRequest) *pb.PauseJobResponse
}

const defaultJobMasterCost = 1

// JobManagerImplV2 is a special job master that manages all the job masters, and notify the offline executor to them.
type JobManagerImplV2 struct {
	lib.BaseMaster

	messageHandlerManager p2p.MessageHandlerManager
	messageSender         p2p.MessageSender
	metaKVClient          metadata.MetaKV
	executorClientManager client.ClientsManager
	serverMasterClient    client.MasterClient

	workerMu sync.Mutex
	workers  map[lib.WorkerID]lib.WorkerHandle
}

func (jm *JobManagerImplV2) Start(ctx context.Context, metaKV metadata.MetaKV) error {
	return nil
}

func (jm *JobManagerImplV2) PauseJob(ctx context.Context, req *pb.PauseJobRequest) *pb.PauseJobResponse {
	panic("not implemented")
}

func (jm *JobManagerImplV2) CancelJob(ctx context.Context, req *pb.CancelJobRequest) *pb.CancelJobResponse {
	panic("not implemented")
}

// SubmitJob processes "SubmitJobRequest".
func (jm *JobManagerImplV2) SubmitJob(ctx context.Context, req *pb.SubmitJobRequest) *pb.SubmitJobResponse {
	log.L().Logger.Info("submit job", zap.String("config", string(req.Config)))
	resp := &pb.SubmitJobResponse{}
	var masterConfig *model.JobMaster
	switch req.Tp {
	case pb.JobType_Benchmark:
		masterConfig = &model.JobMaster{
			Tp:     model.Benchmark,
			Config: req.Config,
		}
	default:
		err := errors.ErrBuildJobFailed.GenWithStack("unknown job type", req.Tp)
		resp.Err = errors.ToPBError(err)
		return resp
	}
	// CreateWorker here is to create job master actually
	// TODO: use correct worker type and worker cost
	id, err := jm.BaseMaster.CreateWorker(
		registry.WorkerTypeFakeMaster, masterConfig, defaultJobMasterCost)
	if err != nil {
		log.L().Error("create job master met error", zap.Error(err))
		resp.Err = errors.ToPBError(err)
		return resp
	}
	resp.JobIdStr = id

	return resp
}

// NewJobManagerImplV2 creates a new JobManagerImplV2 instance
func NewJobManagerImplV2(
	dctx *dcontext.Context,
	masterID lib.MasterID,
	id lib.MasterID,
	messageHandlerManager p2p.MessageHandlerManager,
	messageSender p2p.MessageSender,
	clients client.ClientsManager,
	metaKVClient metadata.MetaKV,
) (*JobManagerImplV2, error) {
	impl := &JobManagerImplV2{
		messageHandlerManager: messageHandlerManager,
		messageSender:         messageSender,
		executorClientManager: clients,
		serverMasterClient:    clients.MasterClient(),
		metaKVClient:          metaKVClient,
		workers:               make(map[lib.WorkerID]lib.WorkerHandle),
	}
	impl.BaseMaster = lib.NewBaseMaster(
		dctx,
		impl,
		id,
		impl.messageHandlerManager,
		impl.messageSender,
		impl.metaKVClient,
		impl.executorClientManager,
		impl.serverMasterClient,
	)
	err := impl.BaseMaster.Init(dctx.Context())
	if err != nil {
		return nil, err
	}
	return impl, nil
}

// InitImpl implements lib.MasterImpl.InitImpl
func (jm *JobManagerImplV2) InitImpl(ctx context.Context) error {
	// TODO: recover existing job masters from metastore
	return nil
}

// Tick implements lib.MasterImpl.Tick
func (jm *JobManagerImplV2) Tick(ctx context.Context) error {
	return nil
}

// OnMasterRecovered implements lib.MasterImpl.OnMasterRecovered
func (jm *JobManagerImplV2) OnMasterRecovered(ctx context.Context) error {
	return nil
}

// OnWorkerDispatched implements lib.MasterImpl.OnWorkerDispatched
func (jm *JobManagerImplV2) OnWorkerDispatched(worker lib.WorkerHandle, result error) error {
	if result != nil {
		log.L().Warn("dispatch worker met error", zap.Error(result))
		return nil
	}
	jm.workerMu.Lock()
	defer jm.workerMu.Unlock()
	jm.workers[worker.ID()] = worker
	return nil
}

// OnWorkerOnline implements lib.MasterImpl.OnWorkerOnline
func (jm *JobManagerImplV2) OnWorkerOnline(worker lib.WorkerHandle) error {
	log.L().Info("on worker online", zap.Any("id", worker.ID()))
	return nil
}

// OnWorkerOffline implements lib.MasterImpl.OnWorkerOffline
func (jm *JobManagerImplV2) OnWorkerOffline(worker lib.WorkerHandle, reason error) error {
	log.L().Info("on worker offline", zap.Any("id", worker.ID()), zap.Any("reason", reason))
	return nil
}

// OnWorkerMessage implements lib.MasterImpl.OnWorkerMessage
func (jm *JobManagerImplV2) OnWorkerMessage(worker lib.WorkerHandle, topic p2p.Topic, message interface{}) error {
	log.L().Info("on worker message", zap.Any("id", worker.ID()), zap.Any("topic", topic), zap.Any("message", message))
	return nil
}

// CloseImpl implements lib.MasterImpl.CloseImpl
func (jm *JobManagerImplV2) CloseImpl(ctx context.Context) error {
	return nil
}
