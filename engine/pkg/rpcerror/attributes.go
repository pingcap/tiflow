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

import "google.golang.org/grpc/codes"

type retryablity interface {
	isRetryable() bool
}

// Retryable can be passed as a parameter to Error to
// mark the error as retryable.
type Retryable struct{}

func (r Retryable) isRetryable() bool {
	return true
}

// NotRetryable can be passed as a parameter to Error to
// mark the error as not retryable.
type NotRetryable struct{}

func (n NotRetryable) isRetryable() bool {
	return false
}

type grpcStatusCoder interface {
	grpcStatusCode() codes.Code
}

// Canceled can be passed as a parameter to Error to
// mark the grpc error code of this error as Canceled.
type Canceled struct{}

func (c Canceled) grpcStatusCode() codes.Code {
	return codes.Canceled
}

// InvalidArgument can be passed as a parameter to Error to
// mark the grpc error code of this error as InvalidArgument.
type InvalidArgument struct{}

func (c InvalidArgument) grpcStatusCode() codes.Code {
	return codes.InvalidArgument
}

// DeadlineExceeded can be passed as a parameter to Error to
// mark the grpc error code of this error as DeadlineExceeded.
type DeadlineExceeded struct{}

func (c DeadlineExceeded) grpcStatusCode() codes.Code {
	return codes.DeadlineExceeded
}

// AlreadyExists can be passed as a parameter to Error to
// mark the grpc error code of this error as AlreadyExists.
type AlreadyExists struct{}

func (c AlreadyExists) grpcStatusCode() codes.Code {
	return codes.AlreadyExists
}

// NotFound can be passed as a parameter to Error to
// mark the grpc error code of this error as NotFound.
type NotFound struct{}

func (c NotFound) grpcStatusCode() codes.Code {
	return codes.NotFound
}

// ResourceExhausted can be passed as a parameter to Error to
// mark the grpc error code of this error as ResourceExhausted.
type ResourceExhausted struct{}

func (c ResourceExhausted) grpcStatusCode() codes.Code {
	return codes.ResourceExhausted
}

// PermissionDenied can be passed as a parameter to Error to
// mark the grpc error code of this error as PermissionDenied.
type PermissionDenied struct{}

func (c PermissionDenied) grpcStatusCode() codes.Code {
	return codes.PermissionDenied
}

// FailedPrecondition can be passed as a parameter to Error to
// mark the grpc error code of this error as FailedPrecondition.
type FailedPrecondition struct{}

func (c FailedPrecondition) grpcStatusCode() codes.Code {
	return codes.FailedPrecondition
}

// Aborted can be passed as a parameter to Error to
// mark the grpc error code of this error as Aborted.
type Aborted struct{}

func (c Aborted) grpcStatusCode() codes.Code {
	return codes.Aborted
}

// OutOfRange can be passed as a parameter to Error to
// mark the grpc error code of this error as OutOfRange.
type OutOfRange struct{}

func (c OutOfRange) grpcStatusCode() codes.Code {
	return codes.OutOfRange
}

// Internal can be passed as a parameter to Error to
// mark the grpc error code of this error as Internal.
type Internal struct{}

func (c Internal) grpcStatusCode() codes.Code {
	return codes.Internal
}

// Unavailable can be passed as a parameter to Error to
// mark the grpc error code of this error as Unavailable.
type Unavailable struct{}

func (u Unavailable) grpcStatusCode() codes.Code {
	return codes.Unavailable
}

// DataLoss can be passed as a parameter to Error to
// mark the grpc error code of this error as DataLoss.
type DataLoss struct{}

func (u DataLoss) grpcStatusCode() codes.Code {
	return codes.DataLoss
}

// Unauthenticated can be passed as a parameter to Error to
// mark the grpc error code of this error as Unauthenticated.
type Unauthenticated struct{}

func (u Unauthenticated) grpcStatusCode() codes.Code {
	return codes.Unauthenticated
}

type errorInfo interface {
	isErrorInfo()
}

// Error should be embedded into any struct that is used with Normalize.
type Error[R retryablity, E grpcStatusCoder] struct {
	retryable R
	code      E
}

func (e Error[R, E]) isRetryable() bool {
	return e.retryable.isRetryable()
}

func (e Error[R, E]) grpcStatusCode() codes.Code {
	return e.code.grpcStatusCode()
}

func (e Error[R, E]) isErrorInfo() {
}
