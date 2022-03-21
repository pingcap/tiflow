package externalresource

import (
	"context"
	"os"
	"sort"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProxyConcurrent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	testID := "TestProxyConcurrent"
	prefix := "resources"
	broker := NewBroker("executor-id", prefix, nil)
	p, err := broker.NewProxyForWorker(ctx, testID)
	require.NoError(t, err)

	require.Equal(t, broker.AllocatedIDs(), []string{testID})

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			p.ID()
			_, err := p.CreateFile(ctx, "test1"+strconv.Itoa(i))
			require.NoError(t, err)
			p.ID()
			_, err = p.CreateFile(ctx, "test2"+strconv.Itoa(i))
			require.NoError(t, err)
		}(i)
	}
	wg.Wait()

	err = os.RemoveAll(prefix)
	require.NoError(t, err)
}

func TestCollectLocalAndRemove(t *testing.T) {
	t.Parallel()

	prefix := "resources-2"
	broker := NewBroker("executor-id", prefix, nil)
	defer func() {
		err := os.RemoveAll(prefix)
		require.NoError(t, err)
	}()
	err := os.MkdirAll(prefix+"/id-1", 0o777)
	require.NoError(t, err)
	err = os.MkdirAll(prefix+"/id-2", 0o777)
	require.NoError(t, err)

	allocated := broker.AllocatedIDs()
	sort.Strings(allocated)
	require.Equal(t, []string{"id-1", "id-2"}, allocated)

	broker.Remove("id-1")
	allocated = broker.AllocatedIDs()
	require.Equal(t, []string{"id-2"}, allocated)
	// should not exist
	_, err = os.Stat(prefix + "/id-1")
	require.EqualError(t, err, "stat "+prefix+"/id-1: no such file or directory")
}
