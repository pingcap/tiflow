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

package rpcerror

import (
	"testing"

	"github.com/gogo/status"
	"github.com/pingcap/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	pb "github.com/pingcap/tiflow/engine/enginepb"
)

func TestTryUnwrapNormalizedError(t *testing.T) {
	t.Parallel()

	testErrorPrototype := Normalize[testError](WithName("ErrTestError"), WithMessage("test message"))
	err := testErrorPrototype.GenWithStack(&testError{Val: "first test error"})

	errOut, ok := tryUnwrapNormalizedError(err)
	require.True(t, ok)
	require.IsType(t, &normalizedError[testError]{}, errOut)
}

func TestToGRPCError(t *testing.T) {
	t.Parallel()

	testErrorPrototype := Normalize[testError](WithName("ErrTestError"), WithMessage("test message"))
	err := testErrorPrototype.GenWithStack(&testError{Val: "first test error"})

	grpcErr := ToGRPCError(err)
	st := status.Convert(grpcErr)
	require.Equal(t, codes.Unavailable, st.Code())
	require.Len(t, st.Details(), 1)

	pbErr := st.Details()[0].(*pb.ErrorV2)
	require.Equal(t, &pb.ErrorV2{
		Name:    "ErrTestError",
		Details: []byte(`{"val":"first test error"}`),
	}, pbErr)
}

func TestFromGRPCError(t *testing.T) {
	t.Parallel()

	testErrorPrototype := Normalize[testError](WithName("ErrTestError"), WithMessage("test message"))
	err := testErrorPrototype.GenWithStack(&testError{Val: "first test error"})

	grpcErr := ToGRPCError(err)

	errOut := FromGRPCError(grpcErr)
	require.True(t, testErrorPrototype.Is(errOut))
}

type unretryableErr struct {
	Error[NotRetryable, Unauthenticated]

	Val int `json:"val"`
}

func TestIsRetryable(t *testing.T) {
	t.Parallel()

	testErrorPrototype := Normalize[testError](WithName("ErrTestError"), WithMessage("test message"))
	err := testErrorPrototype.GenWithStack(&testError{Val: "first test error"})
	require.True(t, IsRetryable(err))

	unretryableErrorPrototype := Normalize[unretryableErr](WithName("ErrUnretryable"), WithMessage("not retryable"))
	err = unretryableErrorPrototype.GenWithStack(&unretryableErr{Val: 1})
	require.False(t, IsRetryable(err))

	otherError := errors.New("test")
	require.False(t, IsRetryable(otherError))
}
