// Copyright 2021 PingCAP, Inc.
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
	"github.com/hanfei1991/microcosm/pb"
	"github.com/pingcap/errors"
	cdc_errors "github.com/pingcap/ticdc/pkg/errors"
)

// ToPBError translates go error to pb error.
func ToPBError(err error) *pb.Error {
	if err == nil {
		return nil
	}
	rfcCode, ok := cdc_errors.RFCCode(err)
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
	default:
		pbErr.Code = pb.ErrorCode_UnknownError
	}
	pbErr.Message = err.Error()
	return pbErr
}

// Wrap generates a new error based on given `*errors.Error`, wraps the err as
// cause error.
// If given `err` is nil, returns a nil error, which a the different behavior
// against `Wrap` function in pingcap/errors.
func Wrap(rfcError *errors.Error, err error, args ...interface{}) error {
	if err == nil {
		return nil
	}
	return rfcError.Wrap(err).GenWithStackByArgs(args...)
}
