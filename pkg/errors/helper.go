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
	pb "github.com/pingcap/tiflow/engine/enginepb"
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

// ChangeFeedFastFailError is read only.
// If this type of error occurs in a changefeed, it means that the data it
// wants to replicate has been or will be GC. So it makes no sense to try to
// resume the changefeed, and the changefeed should immediately be failed.
var ChangeFeedFastFailError = []*errors.Error{
	ErrGCTTLExceeded, ErrSnapshotLostByGC, ErrStartTsBeforeGC,
}

// ChangefeedFastFailError checks if an error is a ChangefeedFastFailError
func ChangefeedFastFailError(err error) bool {
	if err == nil {
		return false
	}
	for _, e := range ChangeFeedFastFailError {
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

// ChangefeedFastFailErrorCode checks the error code, returns true if it is a
// ChangefeedFastFailError code
func ChangefeedFastFailErrorCode(errCode errors.RFCErrorCode) bool {
	for _, e := range ChangeFeedFastFailError {
		if errCode == e.RFCCode() {
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

// ToPBError translates go error to pb error.
func ToPBError(err error) *pb.Error {
	if err == nil {
		return nil
	}
	rfcCode, ok := RFCCode(err)
	if !ok {
		return &pb.Error{
			Code:    pb.ErrorCode_UnknownError,
			Message: err.Error(),
		}
	}
	pbErr := &pb.Error{}
	switch rfcCode {
	case ErrUnknownExecutorID.RFCCode():
		pbErr.Code = pb.ErrorCode_UnknownExecutor
	case ErrTombstoneExecutor.RFCCode():
		pbErr.Code = pb.ErrorCode_TombstoneExecutor
	case ErrSubJobFailed.RFCCode():
		pbErr.Code = pb.ErrorCode_SubJobSubmitFailed
	case ErrClusterResourceNotEnough.RFCCode():
		pbErr.Code = pb.ErrorCode_NotEnoughResource
	case ErrBuildJobFailed.RFCCode():
		pbErr.Code = pb.ErrorCode_SubJobBuildFailed
	case ErrGrpcBuildConn.RFCCode():
		pbErr.Code = pb.ErrorCode_BuildGrpcConnFailed
	default:
		pbErr.Code = pb.ErrorCode_UnknownError
	}
	pbErr.Message = err.Error()
	return pbErr
}
