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

package resourcejob

import (
	"github.com/pingcap/tiflow/engine/lib"
	libModel "github.com/pingcap/tiflow/engine/lib/model"
	"github.com/pingcap/tiflow/engine/lib/registry"
)

const (
	resourceTestWorkerType = libModel.WorkerType(10001)
	resourceTestMasterType = libModel.WorkerType(10002)
)

func Register(reg registry.Registry) {
	fakeMasterFactory := registry.NewSimpleWorkerFactory(NewWorker, &jobConfig{})
	reg.MustRegisterWorkerType(lib.FakeJobMaster, fakeMasterFactory)

	fakeWorkerFactory := registry.NewSimpleWorkerFactory(NewMaster, &workerConfig{})
	reg.MustRegisterWorkerType(lib.FakeTask, fakeWorkerFactory)
}
