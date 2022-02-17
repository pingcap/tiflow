package dm

import (
	"github.com/pingcap/tiflow/dm/dm/config"

	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/lib/registry"
	"github.com/hanfei1991/microcosm/pkg/context"
)

const (
	WorkerDMDump lib.WorkerType = 100 + iota
	WorkerDMLoad
)

func init() {
	dumpFactory := unitWorkerFactory{constructor: newDumpWorker}
	loadFactory := unitWorkerFactory{constructor: newLoadWorker}

	r := registry.GlobalWorkerRegistry()
	r.MustRegisterWorkerType(WorkerDMDump, dumpFactory)
	r.MustRegisterWorkerType(WorkerDMLoad, loadFactory)
}

type workerConstructor func(config lib.WorkerConfig) lib.Worker

type unitWorkerFactory struct {
	constructor workerConstructor
}

func (u unitWorkerFactory) NewWorker(ctx *context.Context, workerID lib.WorkerID, masterID lib.MasterID, config registry.WorkerConfig) (lib.Worker, error) {
	return u.constructor(config), nil
}

func (u unitWorkerFactory) DeserializeConfig(configBytes []byte) (registry.WorkerConfig, error) {
	cfg := &config.SubTaskConfig{}
	err := cfg.Decode(string(configBytes), true)
	return cfg, err
}
