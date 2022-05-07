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
	"github.com/pingcap/tiflow/engine/lib"
	"github.com/pingcap/tiflow/engine/lib/fake"
	libModel "github.com/pingcap/tiflow/engine/lib/model"
	dcontext "github.com/pingcap/tiflow/engine/pkg/context"
)

// only for test.
func RegisterFake(registry Registry) {
	fakeMasterFactory := NewSimpleWorkerFactory(func(ctx *dcontext.Context, id libModel.WorkerID, masterID libModel.MasterID, config WorkerConfig) lib.WorkerImpl {
		return fake.NewFakeMaster(ctx, id, masterID, config)
	}, &fake.Config{})
	registry.MustRegisterWorkerType(lib.FakeJobMaster, fakeMasterFactory)

	fakeWorkerFactory := NewSimpleWorkerFactory(fake.NewDummyWorker, &fake.WorkerConfig{})
	registry.MustRegisterWorkerType(lib.FakeTask, fakeWorkerFactory)
}
