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
	"net/http"
	"strings"

	"github.com/pingcap/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// WrapError generates a new error based on given `*errors.Error`, wraps the err
// as cause error.
// If given `err` is nil, returns a nil error, which a the different behavior
// against `Wrap` function in pingcap/errors.
func WrapError(rfcError *errors.Error, err error, args ...interface{}) error {
	if err == nil {
		return nil
	}
	return rfcError.Wrap(err).GenWithStackByArgs(args...)
}

// ChangeFeedGCFastFailError is read only.
// If this type of error occurs in a changefeed, it means that the data it
// wants to replicate has been or will be GC. So it makes no sense to try to
// resume the changefeed, and the changefeed should immediately be failed.
var ChangeFeedGCFastFailError = []*errors.Error{
	ErrGCTTLExceeded, ErrSnapshotLostByGC, ErrStartTsBeforeGC,
}

// IsChangefeedGCFastFailError checks if an error is a ChangefeedFastFailError
func IsChangefeedGCFastFailError(err error) bool {
	if err == nil {
		return false
	}
	for _, e := range ChangeFeedGCFastFailError {
		if e.Equal(err) {
			return true
		}
		rfcCode, ok := RFCCode(err)
		if ok && e.RFCCode() == rfcCode {
			return true
		}
	}
	return false
}

// IsChangefeedGCFastFailErrorCode checks the error code, returns true if it is a
// ChangefeedFastFailError code
func IsChangefeedGCFastFailErrorCode(errCode errors.RFCErrorCode) bool {
	for _, e := range ChangeFeedGCFastFailError {
		if errCode == e.RFCCode() {
			return true
		}
	}
	return false
}

var changefeedUnRetryableErrors = []*errors.Error{
	ErrExpressionColumnNotFound,
	ErrExpressionParseFailed,
	ErrSchemaSnapshotNotFound,
	ErrSyncRenameTableFailed,
	ErrChangefeedUnretryable,

	ErrSinkURIInvalid,
	ErrKafkaInvalidConfig,
	ErrMySQLInvalidConfig,
	ErrStorageSinkInvalidConfig,
}

// ShouldFailChangefeed returns true if an error is a changefeed not retry error.
func ShouldFailChangefeed(err error) bool {
	for _, e := range changefeedUnRetryableErrors {
		if e.Equal(err) {
			return true
		}
		if code, ok := RFCCode(err); ok {
			if code == e.RFCCode() {
				return true
			}
		}
		if strings.Contains(err.Error(), string(e.RFCCode())) {
			return true
		}
	}
	return false
}

// RFCCode returns a RFCCode from an error
func RFCCode(err error) (errors.RFCErrorCode, bool) {
	type rfcCoder interface {
		RFCCode() errors.RFCErrorCode
	}
	if terr, ok := err.(rfcCoder); ok {
		return terr.RFCCode(), true
	}
	cause := errors.Unwrap(err)
	if cause == nil {
		return "", false
	}
	if terr, ok := cause.(rfcCoder); ok {
		return terr.RFCCode(), true
	}
	return RFCCode(cause)
}

// IsRetryableError check the error is safe or worth to retry
func IsRetryableError(err error) bool {
	if err == nil {
		return false
	}

	switch errors.Cause(err) {
	case context.Canceled, context.DeadlineExceeded:
		return false
	}
	return true
}

var cliUnprintableError = []*errors.Error{ErrCliAborted}

// IsCliUnprintableError returns true if the error should not be printed in cli.
func IsCliUnprintableError(err error) bool {
	if err == nil {
		return false
	}
	for _, e := range cliUnprintableError {
		if strings.Contains(err.Error(), string(e.RFCCode())) {
			return true
		}
	}
	return false
}

// WrapChangefeedUnretryableErr wraps an error into ErrChangefeedUnRetryable.
func WrapChangefeedUnretryableErr(err error, args ...interface{}) error {
	return WrapError(ErrChangefeedUnretryable, err, args...)
}

// IsContextCanceledError checks if an error is caused by context.Canceled.
func IsContextCanceledError(err error) bool {
	return errors.Cause(err) == context.Canceled
}

// IsContextDeadlineExceededError checks if an error is caused by context.DeadlineExceeded.
func IsContextDeadlineExceededError(err error) bool {
	return errors.Cause(err) == context.DeadlineExceeded
}

// httpStatusCodeMapping is a mapping from RFC error code to HTTP status code.
// It does not contain all RFC error codes, only the ones what we think
// are not just internal errors.
var httpStatusCodeMapping = map[errors.RFCErrorCode]int{
	ErrUnknown.RFCCode():               http.StatusInternalServerError,
	ErrInvalidArgument.RFCCode():       http.StatusBadRequest,
	ErrMasterNotReady.RFCCode():        http.StatusServiceUnavailable,
	ErrJobNotFound.RFCCode():           http.StatusNotFound,
	ErrJobAlreadyExists.RFCCode():      http.StatusConflict,
	ErrJobAlreadyCanceled.RFCCode():    http.StatusBadRequest,
	ErrJobNotTerminated.RFCCode():      http.StatusBadRequest,
	ErrJobNotRunning.RFCCode():         http.StatusBadRequest,
	ErrMetaStoreNotExists.RFCCode():    http.StatusNotFound,
	ErrResourceAlreadyExists.RFCCode(): http.StatusConflict,
	ErrIllegalResourcePath.RFCCode():   http.StatusBadRequest,
	ErrResourceDoesNotExist.RFCCode():  http.StatusNotFound,
	ErrResourceConflict.RFCCode():      http.StatusConflict,
}

// HTTPStatusCode returns the HTTP status code for the given error.
func HTTPStatusCode(err error) int {
	if err == nil {
		return http.StatusOK
	}
	rfcCode, ok := RFCCode(err)
	if !ok {
		if IsContextCanceledError(err) {
			return 499 // Client Closed Request
		}
		if IsContextDeadlineExceededError(err) {
			return http.StatusGatewayTimeout
		}
		return http.StatusInternalServerError
	}
	if code, ok := httpStatusCodeMapping[rfcCode]; ok {
		return code
	}
	return http.StatusInternalServerError
}

// gRPCStatusCodeMapping is a mapping from RFC error code to gRPC status code.
// It does not contain all RFC error codes, only the ones what we think
// are not just internal errors.
var gRPCStatusCodeMapping = map[errors.RFCErrorCode]codes.Code{
	ErrUnknown.RFCCode():               codes.Unknown,
	ErrInvalidArgument.RFCCode():       codes.InvalidArgument,
	ErrMasterNotReady.RFCCode():        codes.Unavailable,
	ErrUnknownExecutor.RFCCode():       codes.InvalidArgument,
	ErrTombstoneExecutor.RFCCode():     codes.FailedPrecondition,
	ErrJobNotFound.RFCCode():           codes.NotFound,
	ErrJobAlreadyExists.RFCCode():      codes.AlreadyExists,
	ErrJobAlreadyCanceled.RFCCode():    codes.FailedPrecondition,
	ErrJobNotTerminated.RFCCode():      codes.FailedPrecondition,
	ErrJobNotRunning.RFCCode():         codes.FailedPrecondition,
	ErrMetaStoreNotExists.RFCCode():    codes.NotFound,
	ErrResourceAlreadyExists.RFCCode(): codes.AlreadyExists,
	ErrIllegalResourcePath.RFCCode():   codes.InvalidArgument,
	ErrResourceDoesNotExist.RFCCode():  codes.NotFound,
	ErrResourceConflict.RFCCode():      codes.FailedPrecondition,
}

// GRPCStatusCode returns the gRPC status code for the given error.
func GRPCStatusCode(err error) codes.Code {
	if err == nil {
		return codes.OK
	}
	if s, ok := status.FromError(err); ok {
		return s.Code()
	}
	rfcCode, ok := RFCCode(err)
	if !ok {
		if IsContextCanceledError(err) {
			return codes.Canceled
		}
		if IsContextDeadlineExceededError(err) {
			return codes.DeadlineExceeded
		}
		return codes.Unknown
	}
	if code, ok := gRPCStatusCodeMapping[rfcCode]; ok {
		return code
	}
	return codes.Internal
}
