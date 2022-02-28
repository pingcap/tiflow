package registry

import (
	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/lib/fake"
	dcontext "github.com/hanfei1991/microcosm/pkg/context"
)

type FakeConfig struct{}

// only for test.
func RegisterFake(registry Registry) {
	fakeMasterFactory := NewSimpleWorkerFactory(func(ctx *dcontext.Context, id lib.WorkerID, masterID lib.MasterID, config WorkerConfig) lib.WorkerImpl {
		return fake.NewFakeMaster(ctx, id, masterID, config)
	}, &fake.Config{})
	registry.MustRegisterWorkerType(lib.FakeJobMaster, fakeMasterFactory)

	fakeWorkerFactory := NewSimpleWorkerFactory(fake.NewDummyWorker, &FakeConfig{})
	registry.MustRegisterWorkerType(lib.FakeTask, fakeWorkerFactory)
}
