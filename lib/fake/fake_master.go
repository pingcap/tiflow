package fake

import (
	"context"

	"github.com/hanfei1991/microcosm/lib"
	dcontext "github.com/hanfei1991/microcosm/pkg/context"
	"github.com/hanfei1991/microcosm/pkg/p2p"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"
)

type Master struct {
	*lib.BaseMaster
}

func (m *Master) InitImpl(ctx context.Context) error {
	log.L().Info("FakeMaster: Init")
	return nil
}

func (m *Master) Tick(ctx context.Context) error {
	log.L().Info("FakeMaster: Tick")
	return nil
}

func (m *Master) OnWorkerDispatched(worker lib.WorkerHandle, result error) error {
	log.L().Info("FakeMaster: OnWorkerDispatched",
		zap.String("worker-id", string(worker.ID())),
		zap.Error(result))
	return nil
}

func (m *Master) OnWorkerOnline(worker lib.WorkerHandle) error {
	log.L().Info("FakeMaster: OnWorkerOnline",
		zap.String("worker-id", string(worker.ID())))
	return nil
}

func (m *Master) OnWorkerOffline(worker lib.WorkerHandle, reason error) error {
	log.L().Info("FakeMaster: OnWorkerOffline",
		zap.String("worker-id", string(worker.ID())),
		zap.Error(reason))
	return nil
}

func (m *Master) OnWorkerMessage(worker lib.WorkerHandle, topic p2p.Topic, message interface{}) error {
	log.L().Info("FakeMaster: OnWorkerMessage",
		zap.String("topic", topic),
		zap.Any("message", message))
	return nil
}

func (m *Master) CloseImpl(ctx context.Context) error {
	log.L().Info("FakeMaster: Close")
	return nil
}

func NewFakeMaster(ctx dcontext.Context, masterID lib.MasterID) lib.Master {
	ret := &Master{}
	deps := ctx.Dependencies
	base := lib.NewBaseMaster(
		ret,
		masterID,
		deps.MessageHandlerManager,
		deps.MessageRouter,
		deps.MetaKVClient,
		deps.ExecutorClientManager,
		deps.ServerMasterClient)
	ret.BaseMaster = base
	return ret
}
