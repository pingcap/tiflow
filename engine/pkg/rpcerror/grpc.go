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
	"errors"

	"github.com/gogo/status"
	"google.golang.org/grpc/codes"

	pb "github.com/pingcap/tiflow/engine/enginepb"
)

type GRPCError interface {
	GRPCStatus() *status.Status
}

type typeErasedNormalizedError interface {
	toPB() *pb.ErrorV2
	statusCode() codes.Code
	message() string
}

type grpcError struct {
	inner typeErasedNormalizedError
}

func (e grpcError) GRPCStatus() *status.Status {
	st := status.New(e.inner.statusCode(), e.inner.message())
	stWithDetail, err := st.WithDetails(e.inner.toPB())
	if err != nil {
		panic(err)
	}
	return stWithDetail
}

func tryUnwrapNormalizedError(errIn error) (typeErasedNormalizedError, bool) {
	var errOut typeErasedNormalizedError
	if errors.As(errIn, &errOut) {
		return errOut, true
	}
	return nil, false
}

func ToGRPCError(errIn error) (GRPCError, bool) {
	if normalized, ok := tryUnwrapNormalizedError(errIn); ok {
		return grpcError{normalized}, true
	}

	return nil, false
}
