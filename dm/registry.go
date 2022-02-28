package dm

import (
	"github.com/pingcap/tiflow/dm/dm/config"

	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/lib/registry"
	"github.com/hanfei1991/microcosm/pkg/context"
)

func init() {
	dumpFactory := unitWorkerFactory{constructor: newDumpWorker}
	loadFactory := unitWorkerFactory{constructor: newLoadWorker}
	syncFactory := unitWorkerFactory{constructor: newSyncWorker}

	r := registry.GlobalWorkerRegistry()
	r.MustRegisterWorkerType(lib.WorkerDMDump, dumpFactory)
	r.MustRegisterWorkerType(lib.WorkerDMLoad, loadFactory)
	r.MustRegisterWorkerType(lib.WorkerDMSync, syncFactory)
	r.MustRegisterWorkerType(lib.DMJobMaster, jobMasterFactory{})
}

type workerConstructor func(lib.WorkerConfig) lib.WorkerImpl

type unitWorkerFactory struct {
	constructor workerConstructor
}

func (u unitWorkerFactory) NewWorkerImpl(
	ctx *context.Context,
	workerID lib.WorkerID,
	masterID lib.MasterID,
	config registry.WorkerConfig,
) (lib.WorkerImpl, error) {
	return u.constructor(config), nil
}

func (u unitWorkerFactory) DeserializeConfig(configBytes []byte) (registry.WorkerConfig, error) {
	cfg := &config.SubTaskConfig{}
	err := cfg.Decode(string(configBytes), true)
	return cfg, err
}

type jobMasterFactory struct{}

func (j jobMasterFactory) NewWorkerImpl(
	ctx *context.Context,
	workerID lib.WorkerID,
	masterID lib.MasterID,
	config registry.WorkerConfig,
) (lib.WorkerImpl, error) {
	return newSubTaskMaster(config), nil
}

func (j jobMasterFactory) DeserializeConfig(configBytes []byte) (registry.WorkerConfig, error) {
	cfg := &config.SubTaskConfig{}
	err := cfg.Decode(string(configBytes), true)
	return cfg, err
}
