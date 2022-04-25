package dm

import (
	"github.com/pingcap/tiflow/dm/dm/config"

	"github.com/hanfei1991/microcosm/lib"
	libModel "github.com/hanfei1991/microcosm/lib/model"
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
}

type workerConstructor func(lib.WorkerConfig) lib.WorkerImpl

type unitWorkerFactory struct {
	constructor workerConstructor
}

func (u unitWorkerFactory) NewWorkerImpl(
	ctx *context.Context,
	workerID libModel.WorkerID,
	masterID libModel.MasterID,
	config registry.WorkerConfig,
) (lib.WorkerImpl, error) {
	return u.constructor(config), nil
}

func (u unitWorkerFactory) DeserializeConfig(configBytes []byte) (registry.WorkerConfig, error) {
	cfg := &config.SubTaskConfig{}
	err := cfg.Decode(string(configBytes), true)
	return cfg, err
}
