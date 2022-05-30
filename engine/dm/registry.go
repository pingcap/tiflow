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

package dm

import (
	"github.com/pingcap/tiflow/dm/dm/config"

	"github.com/pingcap/tiflow/engine/lib"
	libModel "github.com/pingcap/tiflow/engine/lib/model"
	"github.com/pingcap/tiflow/engine/lib/registry"
	"github.com/pingcap/tiflow/engine/pkg/context"
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
