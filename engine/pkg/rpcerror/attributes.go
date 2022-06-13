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

type Retryable struct{}

func (r Retryable) isRetryable() bool {
	return true
}

type NotRetryable struct{}

func (n NotRetryable) isRetryable() bool {
	return false
}

type grpcStatusCoder interface {
	grpcStatusCode() codes.Code
}

type Unavailable struct {
}

func (u Unavailable) grpcStatusCode() codes.Code {
	return codes.Unavailable
}

type errorInfo interface {
	isErrorInfo()
}

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
