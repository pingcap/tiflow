package registry

import (
	"testing"

	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/lib/fake"
	dcontext "github.com/hanfei1991/microcosm/pkg/context"
	"github.com/stretchr/testify/require"
)

func TestNewSimpleWorkerFactory(t *testing.T) {
	dummyConstructor := func(ctx *dcontext.Context, id lib.WorkerID, masterID lib.MasterID, config WorkerConfig) lib.Worker {
		return fake.NewDummyWorker(ctx, id, masterID, config)
	}
	fac := NewSimpleWorkerFactory(dummyConstructor, &dummyConfig{})
	config, err := fac.DeserializeConfig([]byte(`{"Val":1}`))
	require.NoError(t, err)
	require.Equal(t, &dummyConfig{Val: 1}, config)

	newWorker, err := fac.NewWorker(dcontext.Background(), "my-worker", "my-master", &dummyConfig{Val: 1})
	require.NoError(t, err)
	require.IsType(t, &fake.Worker{}, newWorker)
}
