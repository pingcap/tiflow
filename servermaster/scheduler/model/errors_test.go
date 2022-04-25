package model

import (
	"testing"

	"github.com/gogo/status"
	"github.com/pingcap/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	derrors "github.com/hanfei1991/microcosm/pkg/errors"
)

func TestConflictErrorToGRPCError(t *testing.T) {
	errIn := NewResourceConflictError(
		"resource-1", "executor-1",
		"resource-2", "executor-2")
	errOut := SchedulerErrorToGRPCError(errors.Trace(errIn))
	st := status.Convert(errOut)
	require.Equal(t, codes.FailedPrecondition, st.Code())
	require.Equal(t, "Scheduler could not assign executor"+
		" due to conflicting requirements: resource resource-1 needs"+
		" executor executor-1, while resource resource-2 needs"+
		" executor executor-2", st.Message())
}

func TestNotFoundErrorToGRPCError(t *testing.T) {
	errIn := NewResourceNotFoundError("resource-1",
		derrors.ErrResourceDoesNotExist.GenWithStackByArgs("resource-1"))
	errOut := SchedulerErrorToGRPCError(errors.Trace(errIn))
	st := status.Convert(errOut)
	require.Equal(t, codes.NotFound, st.Code())
	require.Equal(t, "Scheduler could not find resource resource-1,"+
		" caused by [DFLOW:ErrResourceDoesNotExist]resource does not exists: resource-1", st.Message())
}
