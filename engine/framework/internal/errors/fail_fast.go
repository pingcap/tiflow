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
// See the License for the specific language

package errors

import (
	"github.com/pingcap/tiflow/pkg/errors"
)

// FailFastError is used internally in the framework to
// mark the error has needing to fail the worker or master immediately.
// FailFastError implements Cause() so that it is compatible with
// `errors` and `pingcap/errors`.
type FailFastError struct {
	cause error
}

// FailFast wraps an error and marks it as needing to fail
// the worker or master immediately.
func FailFast(errIn error) error {
	if errIn == nil {
		return nil
	}
	return &FailFastError{cause: errIn}
}

// Cause implement causer for the error.
func (e *FailFastError) Cause() error {
	return e.cause
}

// Unwrap returns cause of the error.
// It allows Error to work with errors.Is() and errors.As() from the Go
// standard package.
func (e *FailFastError) Unwrap() error {
	return e.cause
}

func (e *FailFastError) Error() string {
	return e.cause.Error()
}

// IsFailFastError tells whether the error is FailFastError.
func IsFailFastError(errIn error) bool {
	var out *FailFastError

	// We use `As` instead of `Is` because
	// `Is` requires exact equalities instead
	// of type matching.
	return errors.As(errIn, &out)
}
