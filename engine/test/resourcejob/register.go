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
	libModel "github.com/pingcap/tiflow/engine/lib/model"
	"github.com/pingcap/tiflow/engine/lib/registry"
)

const (
	// ResourceTestWorkerType is the type for a resource test worker.
	ResourceTestWorkerType = libModel.WorkerType(201)
	// ResourceTestMasterType is the type for a resource test master.
	ResourceTestMasterType = libModel.WorkerType(202)
)

func init() {
	r := registry.GlobalWorkerRegistry()
	Register(r)
}

// Register registers the resource jobMaster and worker.
func Register(reg registry.Registry) {
	fakeMasterFactory := registry.NewSimpleWorkerFactory(NewWorker, &JobConfig{})
	reg.MustRegisterWorkerType(ResourceTestMasterType, fakeMasterFactory)

	fakeWorkerFactory := registry.NewSimpleWorkerFactory(NewMaster, &workerConfig{})
	reg.MustRegisterWorkerType(ResourceTestWorkerType, fakeWorkerFactory)
}
