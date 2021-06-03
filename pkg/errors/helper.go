// Copyright 2020 PingCAP, Inc.
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

package errors

import (
	"context"

	"github.com/pingcap/errors"
)

// WrapError generates a new error based on given `*errors.Error`, wraps the err
// as cause error.
// If given `err` is nil, returns a nil error, which a the different behavior
// against `Wrap` function in pingcap/errors.
func WrapError(rfcError *errors.Error, err error) error {
	if err == nil {
		return nil
	}
	return rfcError.Wrap(err).GenWithStackByCause()
}

// ChangefeedFastFailError checks the error, returns true if it is meaningless
// to retry on this error
func ChangefeedFastFailError(err error) bool {
	return ErrStartTsBeforeGC.Equal(errors.Cause(err))
}

// ChangefeedFastFailErrorCode checks the error, returns true if it is meaningless
// to retry on this error
func ChangefeedFastFailErrorCode(errCode errors.RFCErrorCode) bool {
	switch errCode {
	case ErrStartTsBeforeGC.RFCCode():
		return true
	default:
		return false
	}
}

// IsRetryableError check the error is safe or worth to retry
func IsRetryableError(err error) bool {
	switch errors.Cause(err) {
	case context.Canceled, context.DeadlineExceeded:
		return false
	}

	return true
}
