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
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/engine/framework"
	"github.com/pingcap/tiflow/engine/framework/fake"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	dcontext "github.com/pingcap/tiflow/engine/pkg/context"
	pkgOrm "github.com/pingcap/tiflow/engine/pkg/orm"
	"github.com/stretchr/testify/require"
)

var fakeWorkerFactory WorkerFactory = NewSimpleWorkerFactory(fake.NewDummyWorker)

const (
	fakeWorkerType = frameModel.FakeJobMaster
)

func TestGlobalRegistry(t *testing.T) {
	GlobalWorkerRegistry().MustRegisterWorkerType(fakeWorkerType, fakeWorkerFactory)

	ctx, cancel := makeCtxWithMockDeps(t)
	defer cancel()
	metaCli, err := ctx.Deps().Construct(
		func(cli pkgOrm.Client) (pkgOrm.Client, error) {
			return cli, nil
		},
	)
	require.NoError(t, err)
	defer metaCli.(pkgOrm.Client).Close()
	epoch, err := metaCli.(pkgOrm.Client).GenEpoch(ctx)
	require.NoError(t, err)
	worker, err := GlobalWorkerRegistry().CreateWorker(
		ctx,
		fakeWorkerType,
		"worker-1",
		"master-1",
		[]byte(`{"target-tick":10}`),
		epoch,
	)
	require.NoError(t, err)
	require.IsType(t, &framework.DefaultBaseWorker{}, worker)
	impl := worker.(*framework.DefaultBaseWorker).Impl
	require.IsType(t, &fake.Worker{}, impl)
	require.NotNil(t, impl.(*fake.Worker).BaseWorker)
	require.Equal(t, "worker-1", worker.ID())
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
	_, err := registry.CreateWorker(ctx, fakeWorkerType, "worker-1", "master-1",
		[]byte(`{"Val"":0}`), int64(2))
	require.Error(t, err)
}

func TestLoadFake(t *testing.T) {
	registry := NewRegistry()
	require.NotPanics(t, func() {
		RegisterFake(registry)
	})
}

func TestGetTypeNameOfVarPtr(t *testing.T) {
	t.Parallel()

	type myType struct{}

	var (
		a int
		b myType
	)
	require.Equal(t, "int", getTypeNameOfVarPtr(&a))
	require.Equal(t, "myType", getTypeNameOfVarPtr(&b))
}

func TestImplHasMember(t *testing.T) {
	t.Parallel()

	type myImpl struct {
		MyMember int
	}
	type myIface interface{}

	var iface myIface = &myImpl{}
	require.True(t, implHasMember(iface, "MyMember"))
	require.False(t, implHasMember(iface, "notFound"))
}

func TestSetImplMember(t *testing.T) {
	t.Parallel()

	type MyBase interface{}

	type myImpl struct {
		MyBase
	}
	type myImplIface interface{}

	var iface myImplIface = &myImpl{}

	setImplMember(iface, "MyBase", 2)
	require.Equal(t, 2, iface.(*myImpl).MyBase.(int))
}

func TestIsRetryableError(t *testing.T) {
	t.Parallel()

	registry := NewRegistry()
	ok := registry.RegisterWorkerType(frameModel.FakeJobMaster, NewSimpleWorkerFactory(fake.NewFakeMaster))
	require.True(t, ok)

	testCases := []struct {
		err         error
		isRetryable bool
	}{
		{fake.NewJobUnRetryableError(errors.New("inner err")), false},
		{errors.New("normal error"), true},
	}

	for _, tc := range testCases {
		retryable, err := registry.IsRetryableError(tc.err, frameModel.FakeJobMaster)
		require.NoError(t, err)
		require.Equal(t, tc.isRetryable, retryable)
	}
}
