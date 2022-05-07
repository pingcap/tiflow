package resourcemeta

import (
	"context"
	"fmt"
	"testing"
	"time"

	resModel "github.com/hanfei1991/microcosm/pkg/externalresource/resourcemeta/model"
	pkgOrm "github.com/hanfei1991/microcosm/pkg/orm"
	"github.com/stretchr/testify/require"
)

func newAccessorWithMockKV() *MetadataAccessor {
	cli, _ := pkgOrm.NewMockClient()
	return NewMetadataAccessor(cli)
}

func TestAccessorBasics(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	acc := newAccessorWithMockKV()
	_, found, err := acc.GetResource(ctx, "resource-1")
	require.NoError(t, err)
	require.False(t, found)

	ok, err := acc.CreateResource(ctx, &resModel.ResourceMeta{
		ID:       "resource-1",
		Job:      "job-1",
		Worker:   "worker-1",
		Executor: "executor-1",
		Deleted:  false,
	})
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = acc.CreateResource(ctx, &resModel.ResourceMeta{
		ID:       "resource-1",
		Job:      "job-1",
		Worker:   "worker-1",
		Executor: "executor-1",
		Deleted:  false,
	})
	require.NoError(t, err)
	require.False(t, ok)

	resc, found, err := acc.GetResource(ctx, "resource-1")
	require.NoError(t, err)
	require.True(t, found)
	checkResourceMetaEqual(t, &resModel.ResourceMeta{
		ID:       "resource-1",
		Job:      "job-1",
		Worker:   "worker-1",
		Executor: "executor-1",
		Deleted:  false,
	}, resc)

	ok, err = acc.UpdateResource(ctx, &resModel.ResourceMeta{
		ID:       "resource-1",
		Job:      "job-1",
		Worker:   "worker-2",
		Executor: "executor-1",
		Deleted:  false,
	})
	require.NoError(t, err)
	require.True(t, ok)

	resc, found, err = acc.GetResource(ctx, "resource-1")
	require.NoError(t, err)
	require.True(t, found)
	checkResourceMetaEqual(t, &resModel.ResourceMeta{
		ID:       "resource-1",
		Job:      "job-1",
		Worker:   "worker-2",
		Executor: "executor-1",
		Deleted:  false,
	}, resc)

	ok, err = acc.UpdateResource(ctx, &resModel.ResourceMeta{
		ID:       "resource-2",
		Job:      "job-1",
		Worker:   "worker-2",
		Executor: "executor-1",
		Deleted:  false,
	})
	require.NoError(t, err)
	require.False(t, ok)

	ok, err = acc.DeleteResource(ctx, "resource-1")
	require.NoError(t, err)
	require.True(t, ok)
}

func TestMetadataAccessorGetResourcesForExecutor(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	acc := newAccessorWithMockKV()

	for i := 0; i < 1000; i++ {
		executor := "executor-1"
		if i >= 500 {
			executor = "executor-2"
		}

		ok, err := acc.CreateResource(ctx, &resModel.ResourceMeta{
			ID:       fmt.Sprintf("resource-%d", i),
			Job:      "job-1",
			Worker:   fmt.Sprintf("worker-%d", i),
			Executor: resModel.ExecutorID(executor),
			Deleted:  false,
		})
		require.NoError(t, err)
		require.True(t, ok)
	}

	results, err := acc.GetResourcesForExecutor(ctx, "executor-1")
	require.NoError(t, err)
	require.Len(t, results, 500)
}

func checkResourceMetaEqual(t *testing.T, expect, actual *resModel.ResourceMeta) {
	require.Equal(t, expect.ID, actual.ID)
	require.Equal(t, expect.Job, actual.Job)
	require.Equal(t, expect.Worker, actual.Worker)
	require.Equal(t, expect.Executor, actual.Executor)
	require.Equal(t, expect.Deleted, actual.Deleted)
}
