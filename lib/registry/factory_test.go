package registry

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/dig"

	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/lib/fake"
	dcontext "github.com/hanfei1991/microcosm/pkg/context"
	"github.com/hanfei1991/microcosm/pkg/deps"
	"github.com/hanfei1991/microcosm/pkg/metadata"
	"github.com/hanfei1991/microcosm/pkg/p2p"
)

type paramList struct {
	dig.Out

	MessageHandlerManager p2p.MessageHandlerManager
	MessageSender         p2p.MessageSender
	MetaKVClient          metadata.MetaKV
}

func makeCtxWithMockDeps(t *testing.T) *dcontext.Context {
	dp := deps.NewDeps()
	err := dp.Provide(func() paramList {
		return paramList{
			MessageHandlerManager: p2p.NewMockMessageHandlerManager(),
			MessageSender:         p2p.NewMockMessageSender(),
			MetaKVClient:          metadata.NewMetaMock(),
		}
	})
	require.NoError(t, err)
	return dcontext.Background().WithDeps(dp)
}

func TestNewSimpleWorkerFactory(t *testing.T) {
	dummyConstructor := func(ctx *dcontext.Context, id lib.WorkerID, masterID lib.MasterID, config WorkerConfig) lib.WorkerImpl {
		return fake.NewDummyWorker(ctx, id, masterID, config)
	}
	fac := NewSimpleWorkerFactory(dummyConstructor, &dummyConfig{})
	config, err := fac.DeserializeConfig([]byte(`{"Val":1}`))
	require.NoError(t, err)
	require.Equal(t, &dummyConfig{Val: 1}, config)

	ctx := makeCtxWithMockDeps(t)
	newWorker, err := fac.NewWorkerImpl(ctx, "my-worker", "my-master", &dummyConfig{Val: 1})
	require.NoError(t, err)
	require.IsType(t, &fake.Worker{}, newWorker)
}
