package registry

import (
	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/lib/fake"
	libModel "github.com/hanfei1991/microcosm/lib/model"
	dcontext "github.com/hanfei1991/microcosm/pkg/context"
)

// RegisterFake registers fake job master and fake worker to global registry
func RegisterFake(registry Registry) {
	fakeMasterFactory := NewSimpleWorkerFactory(func(ctx *dcontext.Context, id libModel.WorkerID, masterID libModel.MasterID, config WorkerConfig) lib.WorkerImpl {
		return fake.NewFakeMaster(ctx, id, masterID, config)
	}, &fake.Config{})
	registry.MustRegisterWorkerType(lib.FakeJobMaster, fakeMasterFactory)

	fakeWorkerFactory := NewSimpleWorkerFactory(fake.NewDummyWorker, &fake.WorkerConfig{})
	registry.MustRegisterWorkerType(lib.FakeTask, fakeWorkerFactory)
}
