// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package registry

import (
	"reflect"
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/engine/framework"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	dcontext "github.com/pingcap/tiflow/engine/pkg/context"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/errorutil"
	"go.uber.org/zap"
)

// WorkerConfig alias to framework.WorkerConfig
type WorkerConfig = framework.WorkerConfig

// Registry defines an interface to worker as worker register bridge. Business
// can register any worker or job master implementation into a registry
type Registry interface {
	MustRegisterWorkerType(tp frameModel.WorkerType, factory WorkerFactory)
	RegisterWorkerType(tp frameModel.WorkerType, factory WorkerFactory) (ok bool)
	CreateWorker(
		ctx *dcontext.Context,
		tp framework.WorkerType,
		workerID frameModel.WorkerID,
		masterID frameModel.MasterID,
		config []byte,
		epoch frameModel.Epoch,
	) (framework.Worker, error)
	// IsRetryableError returns whether error is treated as retryable from the
	// perspective of business logic.
	IsRetryableError(err error, tp framework.WorkerType) (bool, error)
}

type registryImpl struct {
	mu         sync.RWMutex
	factoryMap map[frameModel.WorkerType]WorkerFactory
}

// NewRegistry creates a new registryImpl instance
func NewRegistry() Registry {
	return &registryImpl{
		factoryMap: make(map[frameModel.WorkerType]WorkerFactory),
	}
}

// MustRegisterWorkerType implements Registry.MustRegisterWorkerType
func (r *registryImpl) MustRegisterWorkerType(tp frameModel.WorkerType, factory WorkerFactory) {
	if ok := r.RegisterWorkerType(tp, factory); !ok {
		log.Panic("duplicate worker type", zap.Stringer("worker-type", tp))
	}
	log.Info("register worker", zap.Stringer("worker-type", tp))
}

// RegisterWorkerType implements Registry.RegisterWorkerType
func (r *registryImpl) RegisterWorkerType(tp frameModel.WorkerType, factory WorkerFactory) (ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.factoryMap[tp]; exists {
		return false
	}
	r.factoryMap[tp] = factory
	return true
}

// CreateWorker implements Registry.CreateWorker
func (r *registryImpl) CreateWorker(
	ctx *dcontext.Context,
	tp framework.WorkerType,
	workerID frameModel.WorkerID,
	masterID frameModel.MasterID,
	configBytes []byte,
	epoch frameModel.Epoch,
) (framework.Worker, error) {
	factory, ok := r.getWorkerFactory(tp)
	if !ok {
		return nil, errors.ErrWorkerTypeNotFound.GenWithStackByArgs(tp)
	}

	config, err := factory.DeserializeConfig(configBytes)
	if err != nil {
		// dm will connect upstream to adjust some config, so we need check whether
		// the error is retryable too.
		err2 := errorutil.ConvertErr(tp, err)
		if factory.IsRetryableError(err2) {
			return nil, errors.ErrCreateWorkerNonTerminate.Wrap(err).GenWithStackByArgs()
		}
		return nil, errors.ErrCreateWorkerTerminate.Wrap(err).GenWithStackByArgs()
	}

	impl, err := factory.NewWorkerImpl(ctx, workerID, masterID, config)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if implHasMember(impl, nameOfBaseWorker) {
		base := framework.NewBaseWorker(
			ctx,
			impl,
			workerID,
			masterID,
			tp,
			epoch,
		)
		setImplMember(impl, nameOfBaseWorker, base)
		return base, nil
	}
	// TODO: should jobmaster record worker status
	if implHasMember(impl, nameOfBaseJobMaster) {
		base := framework.NewBaseJobMaster(
			ctx,
			impl.(framework.JobMasterImpl),
			masterID,
			workerID,
			tp,
			epoch,
		)
		setImplMember(impl, nameOfBaseJobMaster, base)
		return base, nil
	}
	log.Panic("wrong use of CreateWorker",
		zap.String("reason", "impl has no member BaseWorker or BaseJobMaster"),
		zap.Any("workerType", tp))
	return nil, nil
}

// IsRetryableError checks whether an error is retryable in business logic
func (r *registryImpl) IsRetryableError(err error, tp frameModel.WorkerType) (bool, error) {
	factory, ok := r.getWorkerFactory(tp)
	if !ok {
		return false, errors.ErrWorkerTypeNotFound.GenWithStackByArgs(tp)
	}
	return factory.IsRetryableError(err), nil
}

func (r *registryImpl) getWorkerFactory(tp frameModel.WorkerType) (factory WorkerFactory, ok bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	factory, ok = r.factoryMap[tp]
	return
}

var (
	nameOfBaseWorker    = getTypeNameOfVarPtr(new(framework.BaseWorker))
	nameOfBaseJobMaster = getTypeNameOfVarPtr(new(framework.BaseJobMaster))
)

func getTypeNameOfVarPtr(v interface{}) string {
	return reflect.TypeOf(v).Elem().Name()
}

func implHasMember(impl interface{}, memberName string) bool {
	defer func() {
		if v := recover(); v != nil {
			log.Panic("wrong use of implHasMember",
				zap.Any("reason", v))
		}
	}()
	return reflect.ValueOf(impl).Elem().FieldByName(memberName) != reflect.Value{}
}

func setImplMember(impl interface{}, memberName string, value interface{}) {
	defer func() {
		if v := recover(); v != nil {
			log.Panic("wrong use of setImplMember",
				zap.Any("reason", v))
		}
	}()
	reflect.ValueOf(impl).Elem().FieldByName(memberName).Set(reflect.ValueOf(value))
}
