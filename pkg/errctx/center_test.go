package errctx

import (
	"context"
	"sync"
	"testing"

	"github.com/pingcap/errors"
	"github.com/stretchr/testify/require"
)

func TestErrCenterMultipleErrCtx(t *testing.T) {
	center := NewErrCenter()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			ctx := center.WithCancelOnFirstError(context.Background())
			<-ctx.Done()
			require.Error(t, ctx.Err())
			require.Regexp(t, ".*fake error.*", ctx.Err())
		}()
	}

	center.OnError(errors.New("fake error"))
	wg.Wait()

	require.Error(t, center.CheckError())
	require.Regexp(t, ".*fake error.*", center.CheckError())
}

func TestErrCenterSingleErrCtx(t *testing.T) {
	center := NewErrCenter()
	ctx := center.WithCancelOnFirstError(context.Background())

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			<-ctx.Done()
			require.Error(t, ctx.Err())
			require.Regexp(t, ".*fake error.*", ctx.Err())
		}()
	}

	center.OnError(errors.New("fake error"))
	wg.Wait()

	require.Error(t, center.CheckError())
	require.Regexp(t, ".*fake error.*", center.CheckError())
}

func TestErrCenterRepeatedErrors(t *testing.T) {
	center := NewErrCenter()
	ctx := center.WithCancelOnFirstError(context.Background())

	center.OnError(nil) // no-op
	center.OnError(errors.New("first error"))
	center.OnError(errors.New("second error")) // no-op

	<-ctx.Done()
	require.Error(t, ctx.Err())
	require.Regexp(t, ".*first error.*", ctx.Err())

	require.Error(t, center.CheckError())
	require.Regexp(t, ".*first error.*", center.CheckError())
}

func TestErrCtxPropagate(t *testing.T) {
	center := NewErrCenter()
	parentCtx, cancelParent := context.WithCancel(context.Background())
	ctx := center.WithCancelOnFirstError(parentCtx)

	cancelParent()
	<-ctx.Done()
	require.Error(t, ctx.Err())
	require.Regexp(t, ".*context canceled.*", ctx.Err())
}

func TestErrCtxDoneChSame(t *testing.T) {
	center := NewErrCenter()
	ctx := center.WithCancelOnFirstError(context.Background())

	doneCh1 := ctx.Done()
	doneCh2 := ctx.Done()

	require.Equal(t, doneCh1, doneCh2)
}
