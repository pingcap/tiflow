package registry

import (
	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/lib/fake"
	dcontext "github.com/hanfei1991/microcosm/pkg/context"
)

const (
	WorkerTypeFakeMaster = 10000
	WorkerTypeFakeWorker = 10001
)

type FakeConfig struct{}

func LoadFake(registry Registry) {
	fakeMasterFactory := NewSimpleWorkerFactory(func(ctx *dcontext.Context, id lib.WorkerID, masterID lib.MasterID, config WorkerConfig) lib.Worker {
		return fake.NewFakeMaster(ctx, id, masterID, config)
	}, &FakeConfig{})
	registry.MustRegisterWorkerType(WorkerTypeFakeMaster, fakeMasterFactory)

	fakeWorkerFactory := NewSimpleWorkerFactory(fake.NewDummyWorker, &FakeConfig{})
	registry.MustRegisterWorkerType(WorkerTypeFakeWorker, fakeWorkerFactory)
}
