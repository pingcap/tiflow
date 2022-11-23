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
	"strings"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/engine/framework"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	dcontext "github.com/pingcap/tiflow/engine/pkg/context"
	"github.com/pingcap/tiflow/pkg/errors"
)

// WorkerFactory is an interface that should be implemented by the author of
// WorkerImpl or JobMasterImpl (JobMaster is the worker of JobManager).
// It represents a constructor for a given type of worker.
type WorkerFactory interface {
	// NewWorkerImpl return an implementation of the worker. its BaseWorker
	// or BaseJobMaster field can be left nil, framework will fill it in.
	NewWorkerImpl(
		ctx *dcontext.Context, // We require a `dcontext` here to provide dependencies.
		workerID frameModel.WorkerID, // the globally unique workerID for this worker to be created.
		masterID frameModel.MasterID, // the masterID that this worker will report to.
		config WorkerConfig, // the config used to initialize the worker.
	) (framework.WorkerImpl, error)
	DeserializeConfig(configBytes []byte) (WorkerConfig, error)
	// IsRetryableError passes in an error to business logic, and returns whether
	// job should be re-created or terminated permanently when meeting this error.
	IsRetryableError(err error) bool
}

// WorkerConstructor alias to the function that can construct a WorkerImpl
type WorkerConstructor[T framework.WorkerImpl, C any] func(
	ctx *dcontext.Context, id frameModel.WorkerID, masterID frameModel.MasterID, config C,
) T

// SimpleWorkerFactory is a WorkerFactory with built-in JSON codec for WorkerConfig.
type SimpleWorkerFactory[T framework.WorkerImpl, C any] struct {
	constructor WorkerConstructor[T, C]
}

// NewSimpleWorkerFactory creates a new WorkerFactory.
func NewSimpleWorkerFactory[T framework.WorkerImpl, Config any](
	constructor WorkerConstructor[T, Config],
) *SimpleWorkerFactory[T, Config] {
	// Config must be a pointer
	if !isPtr[Config]() {
		// It's okay to panic here.
		// The developer who used this function mistakenly should
		// be able to figure out what happened.
		log.Panic("expect worker's config type to be a pointer")
	}
	return &SimpleWorkerFactory[T, Config]{
		constructor: constructor,
	}
}

// NewWorkerImpl implements WorkerFactory.NewWorkerImpl
func (f *SimpleWorkerFactory[T, C]) NewWorkerImpl(
	ctx *dcontext.Context,
	workerID frameModel.WorkerID,
	masterID frameModel.MasterID,
	config WorkerConfig,
) (framework.WorkerImpl, error) {
	return f.constructor(ctx, workerID, masterID, config.(C)), nil
}

// DeserializeConfig implements WorkerFactory.DeserializeConfig
func (f *SimpleWorkerFactory[T, C]) DeserializeConfig(configBytes []byte) (WorkerConfig, error) {
	var config C
	config = reflect.New(reflect.TypeOf(config).Elem()).Interface().(C)
	if err := json.Unmarshal(configBytes, config); err != nil {
		return nil, errors.ErrDeserializeConfig.Wrap(err).GenWithStackByArgs()
	}
	return config, nil
}

// IsRetryableError implements WorkerFactory.IsRetryableError
func (f *SimpleWorkerFactory[T, C]) IsRetryableError(err error) bool {
	if errors.Is(err, errors.ErrDeserializeConfig) {
		return false
	}
	if strings.Contains(err.Error(), string(errors.ErrDeserializeConfig.RFCCode())) {
		return false
	}
	return true
}
