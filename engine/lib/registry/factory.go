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
	"encoding/json"
	"reflect"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"

	"github.com/pingcap/tiflow/engine/lib"
	libModel "github.com/pingcap/tiflow/engine/lib/model"
	dcontext "github.com/pingcap/tiflow/engine/pkg/context"
)

// WorkerFactory is an interface that should be implemented by the author of
// WorkerImpl or JobMasterImpl (JobMaster is the worker of JobManager).
// It represents a constructor for a given type of worker.
type WorkerFactory interface {
	// NewWorkerImpl return an implementation of the worker. its BaseWorker
	// or BaseJobMaster field can be left nil, framework will fill it in.
	NewWorkerImpl(
		ctx *dcontext.Context, // We require a `dcontext` here to provide dependencies.
		workerID libModel.WorkerID, // the globally unique workerID for this worker to be created.
		masterID libModel.MasterID, // the masterID that this worker will report to.
		config WorkerConfig, // the config used to initialize the worker.
	) (lib.WorkerImpl, error)
	DeserializeConfig(configBytes []byte) (WorkerConfig, error)
}

// WorkerConstructor alias to the function that can construct a WorkerImpl
type WorkerConstructor func(ctx *dcontext.Context, id libModel.WorkerID, masterID libModel.MasterID, config WorkerConfig) lib.WorkerImpl

// SimpleWorkerFactory is a WorkerFactory with built-in JSON codec for WorkerConfig.
type SimpleWorkerFactory struct {
	constructor WorkerConstructor
	configTpi   interface{}
}

// NewSimpleWorkerFactory creates a new WorkerFactory.
func NewSimpleWorkerFactory(constructor WorkerConstructor, configType interface{}) *SimpleWorkerFactory {
	return &SimpleWorkerFactory{
		constructor: constructor,
		configTpi:   configType,
	}
}

// NewWorkerImpl implements WorkerFactory.NewWorkerImpl
func (f *SimpleWorkerFactory) NewWorkerImpl(
	ctx *dcontext.Context,
	workerID libModel.WorkerID,
	masterID libModel.MasterID,
	config WorkerConfig,
) (lib.WorkerImpl, error) {
	return f.constructor(ctx, workerID, masterID, config), nil
}

// DeserializeConfig implements WorkerFactory.DeserializeConfig
func (f *SimpleWorkerFactory) DeserializeConfig(configBytes []byte) (WorkerConfig, error) {
	config := reflect.New(reflect.TypeOf(f.configTpi).Elem()).Interface()
	if err := json.Unmarshal(configBytes, config); err != nil {
		return nil, errors.Trace(err)
	}
	return config, nil
}

// NewTomlWorkerFactory creates a WorkerFactory with built-in toml codec for WorkerConfig.
func NewTomlWorkerFactory(constructor WorkerConstructor, configType interface{}) *TomlWorkerFactory {
	return &TomlWorkerFactory{
		constructor: constructor,
		configTpi:   configType,
	}
}

// TomlWorkerFactory is a WorkerFactory with built-in TOML codec for WorkerConfig
type TomlWorkerFactory struct {
	constructor WorkerConstructor
	configTpi   interface{}
}

// NewWorkerImpl implements WorkerFactory.NewWorkerImpl
func (f *TomlWorkerFactory) NewWorkerImpl(
	ctx *dcontext.Context,
	workerID libModel.WorkerID,
	masterID libModel.MasterID,
	config WorkerConfig,
) (lib.WorkerImpl, error) {
	return f.constructor(ctx, workerID, masterID, config), nil
}

// DeserializeConfig implements WorkerFactory.DeserializeConfig
func (f *TomlWorkerFactory) DeserializeConfig(configBytes []byte) (WorkerConfig, error) {
	config := reflect.New(reflect.TypeOf(f.configTpi).Elem()).Interface()
	if _, err := toml.Decode(string(configBytes), config); err != nil {
		return nil, errors.Trace(err)
	}
	return config, nil
}
