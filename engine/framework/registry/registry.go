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

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"

	"github.com/pingcap/tiflow/engine/framework"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	dcontext "github.com/pingcap/tiflow/engine/pkg/context"
	derror "github.com/pingcap/tiflow/pkg/errors"
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
	) (framework.Worker, error)
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
		log.L().Panic("duplicate worker type", zap.Int64("worker-type", int64(tp)))
	}
	log.L().Info("register worker", zap.Int64("worker-type", int64(tp)))
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
) (framework.Worker, error) {
	factory, ok := r.getWorkerFactory(tp)
	if !ok {
		return nil, derror.ErrWorkerTypeNotFound.GenWithStackByArgs(tp)
	}

	config, err := factory.DeserializeConfig(configBytes)
	if err != nil {
		return nil, errors.Trace(err)
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
		)
		setImplMember(impl, nameOfBaseJobMaster, base)
		return base, nil
	}
	log.L().Panic("wrong use of CreateWorker",
		zap.String("reason", "impl has no member BaseWorker or BaseJobMaster"),
		zap.Any("workerType", tp))
	return nil, nil
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
			log.L().Panic("wrong use of implHasMember",
				zap.Any("reason", v))
		}
	}()
	return reflect.ValueOf(impl).Elem().FieldByName(memberName) != reflect.Value{}
}

func setImplMember(impl interface{}, memberName string, value interface{}) {
	defer func() {
		if v := recover(); v != nil {
			log.L().Panic("wrong use of setImplMember",
				zap.Any("reason", v))
		}
	}()
	reflect.ValueOf(impl).Elem().FieldByName(memberName).Set(reflect.ValueOf(value))
}
