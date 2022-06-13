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
	"github.com/pingcap/tiflow/engine/framework"
	"github.com/pingcap/tiflow/engine/framework/fake"
)

// RegisterFake registers fake job master and fake worker to global registry
func RegisterFake(registry Registry) {
	fakeMasterFactory := NewSimpleWorkerFactory(fake.NewFakeMaster)
	registry.MustRegisterWorkerType(framework.FakeJobMaster, fakeMasterFactory)

	fakeWorkerFactory := NewSimpleWorkerFactory(fake.NewDummyWorker)
	registry.MustRegisterWorkerType(framework.FakeTask, fakeWorkerFactory)
}
