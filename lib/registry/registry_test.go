package registry

import (
	"testing"

	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/lib/fake"
	dcontext "github.com/hanfei1991/microcosm/pkg/context"
	"github.com/stretchr/testify/require"
)

var fakeWorkerFactory WorkerFactory = ConstructorFuncFactory(fake.NewDummyWorker)

const fakeWorkerType = lib.WorkerType(100)

func TestGlobalRegistry(t *testing.T) {
	GlobalWorkerRegistry().MustRegisterWorkerType(fakeWorkerType, fakeWorkerFactory)

	ctx := dcontext.Background()
	worker, err := GlobalWorkerRegistry().CreateWorker(ctx, fakeWorkerType, "worker-1", "master-1")
	require.NoError(t, err)
	require.IsType(t, &fake.Worker{}, worker)
	require.Equal(t, lib.WorkerID("worker-1"), worker.WorkerID())
}

func TestRegistryDuplicateType(t *testing.T) {
	registry := NewRegistry()
	ok := registry.RegisterWorkerType(fakeWorkerType, fakeWorkerFactory)
	require.True(t, ok)

	ok = registry.RegisterWorkerType(fakeWorkerType, fakeWorkerFactory)
	require.False(t, ok)

	require.Panics(t, func() {
		registry.MustRegisterWorkerType(fakeWorkerType, fakeWorkerFactory)
	})
}

func TestRegistryWorkerTypeNotFound(t *testing.T) {
	registry := NewRegistry()
	ctx := dcontext.Background()
	_, err := registry.CreateWorker(ctx, fakeWorkerType, "worker-1", "master-1")
	require.Error(t, err)
}
