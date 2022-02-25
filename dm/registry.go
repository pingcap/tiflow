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

type workerConstructor func(lib.BaseWorker, lib.WorkerConfig) lib.WorkerImpl

type unitWorkerFactory struct {
	constructor workerConstructor
}

func (u unitWorkerFactory) NewWorker(ctx *context.Context, workerID lib.WorkerID, masterID lib.MasterID, config registry.WorkerConfig) (lib.Worker, error) {
	base := lib.NewBaseWorker(
		ctx,
		nil,
		workerID,
		masterID,
	)
	worker := u.constructor(base, config)
	base.(*lib.DefaultBaseWorker).Impl = worker
	return worker.(lib.Worker), nil
}

func (u unitWorkerFactory) DeserializeConfig(configBytes []byte) (registry.WorkerConfig, error) {
	cfg := &config.SubTaskConfig{}
	err := cfg.Decode(string(configBytes), true)
	return cfg, err
}

type jobMasterFactory struct{}

func (j jobMasterFactory) NewWorker(ctx *context.Context, workerID lib.WorkerID, masterID lib.MasterID, config registry.WorkerConfig) (lib.Worker, error) {
	ret := newSubTaskMaster(nil, config)

	base := lib.NewBaseJobMaster(
		ctx,
		ret,
		masterID,
		workerID,
	)
	ret.BaseJobMaster = base
	return ret, nil
}

func (j jobMasterFactory) DeserializeConfig(configBytes []byte) (registry.WorkerConfig, error) {
	cfg := &config.SubTaskConfig{}
	err := cfg.Decode(string(configBytes), true)
	return cfg, err
}
