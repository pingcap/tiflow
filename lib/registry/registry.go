package registry

import (
	"sync"

	"github.com/hanfei1991/microcosm/lib"
	dcontext "github.com/hanfei1991/microcosm/pkg/context"
	derror "github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"
)

type Registry interface {
	MustRegisterWorkerType(tp lib.WorkerType, factory WorkerFactory)
	RegisterWorkerType(tp lib.WorkerType, factory WorkerFactory) (ok bool)
	CreateWorker(ctx *dcontext.Context, tp lib.WorkerType, workerID lib.WorkerID, masterID lib.MasterID) (lib.Worker, error)
}

type registryImpl struct {
	mu         sync.RWMutex
	factoryMap map[lib.WorkerType]WorkerFactory
}

func NewRegistry() Registry {
	return &registryImpl{
		factoryMap: make(map[lib.WorkerType]WorkerFactory),
	}
}

func (r *registryImpl) MustRegisterWorkerType(tp lib.WorkerType, factory WorkerFactory) {
	if ok := r.RegisterWorkerType(tp, factory); !ok {
		log.L().Panic("duplicate worker type", zap.Int64("worker-type", int64(tp)))
	}
}

func (r *registryImpl) RegisterWorkerType(tp lib.WorkerType, factory WorkerFactory) (ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.factoryMap[tp]; exists {
		return false
	}
	r.factoryMap[tp] = factory
	return true
}

func (r *registryImpl) CreateWorker(
	ctx *dcontext.Context,
	tp lib.WorkerType,
	workerID lib.WorkerID,
	masterID lib.MasterID,
) (lib.Worker, error) {
	factory, ok := r.getWorkerFactory(tp)
	if !ok {
		return nil, derror.ErrWorkerTypeNotFound.GenWithStackByArgs(tp)
	}

	worker, err := factory.NewWorker(ctx, workerID, masterID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return worker, nil
}

func (r *registryImpl) getWorkerFactory(tp lib.WorkerType) (factory WorkerFactory, ok bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	factory, ok = r.factoryMap[tp]
	return
}
