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

package model

import (
	"testing"

	"github.com/gogo/status"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/pingcap/errors"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
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
		cerrors.ErrResourceDoesNotExist.GenWithStackByArgs("resource-1"))
	errOut := SchedulerErrorToGRPCError(errors.Trace(errIn))
	st := status.Convert(errOut)
	require.Equal(t, codes.NotFound, st.Code())
	require.Equal(t, "Scheduler could not find resource resource-1,"+
		" caused by [DFLOW:ErrResourceDoesNotExist]resource does not exists: resource-1", st.Message())
}
