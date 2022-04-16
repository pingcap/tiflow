package registry

import (
	"encoding/json"
	"reflect"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"

	"github.com/hanfei1991/microcosm/lib"
	libModel "github.com/hanfei1991/microcosm/lib/model"
	dcontext "github.com/hanfei1991/microcosm/pkg/context"
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

type WorkerConstructor func(ctx *dcontext.Context, id libModel.WorkerID, masterID libModel.MasterID, config WorkerConfig) lib.WorkerImpl

type SimpleWorkerFactory struct {
	constructor WorkerConstructor
	configTpi   interface{}
}

// NewSimpleWorkerFactory creates a WorkerFactory with built-in JSON codec for WorkerConfig.
func NewSimpleWorkerFactory(constructor WorkerConstructor, configType interface{}) *SimpleWorkerFactory {
	return &SimpleWorkerFactory{
		constructor: constructor,
		configTpi:   configType,
	}
}

func (f *SimpleWorkerFactory) NewWorkerImpl(
	ctx *dcontext.Context,
	workerID libModel.WorkerID,
	masterID libModel.MasterID,
	config WorkerConfig,
) (lib.WorkerImpl, error) {
	return f.constructor(ctx, workerID, masterID, config), nil
}

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

type TomlWorkerFactory struct {
	constructor WorkerConstructor
	configTpi   interface{}
}

func (f *TomlWorkerFactory) NewWorkerImpl(
	ctx *dcontext.Context,
	workerID libModel.WorkerID,
	masterID libModel.MasterID,
	config WorkerConfig,
) (lib.WorkerImpl, error) {
	return f.constructor(ctx, workerID, masterID, config), nil
}

func (f *TomlWorkerFactory) DeserializeConfig(configBytes []byte) (WorkerConfig, error) {
	config := reflect.New(reflect.TypeOf(f.configTpi).Elem()).Interface()
	if _, err := toml.Decode(string(configBytes), config); err != nil {
		return nil, errors.Trace(err)
	}
	return config, nil
}
