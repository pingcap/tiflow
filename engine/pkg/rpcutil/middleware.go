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

package rpcutil

import (
	"context"
	"fmt"
	"strings"

	perrors "github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ToGRPCError converts an error to a gRPC error.
func ToGRPCError(errIn error) error {
	if errIn == nil {
		return nil
	}
	if _, ok := status.FromError(errIn); ok {
		return errIn
	}
	code := errors.GRPCStatusCode(errIn)
	rfcCode, ok := errors.RFCCode(errIn)
	if !ok {
		rfcCode = errors.ErrUnknown.RFCCode()
	}
	st, err := status.New(code, errIn.Error()).
		WithDetails(&errdetails.ErrorInfo{
			Reason: string(rfcCode),
		})
	if err != nil {
		return status.New(code, errIn.Error()).Err()
	}
	return st.Err()
}

// FromGRPCError converts a gRPC error to a normalized error.
func FromGRPCError(errIn error) error {
	if errIn == nil {
		return nil
	}
	st, ok := status.FromError(errIn)
	if !ok {
		return errIn
	}
	var errInfo *errdetails.ErrorInfo
	for _, detail := range st.Details() {
		if ei, ok := detail.(*errdetails.ErrorInfo); ok {
			errInfo = ei
			break
		}
	}
	if errInfo == nil || errInfo.Reason == "" {
		return errors.ErrUnknown.GenWithStack(st.Message())
	}
	msg := strings.TrimPrefix(st.Message(), fmt.Sprintf("[%s]", errInfo.Reason))
	return perrors.Normalize(msg, perrors.RFCCodeText(errInfo.Reason)).GenWithStackByArgs()
}

// UnaryServerInterceptor is a gRPC server-side interceptor that converts errors to gRPC errors and logs requests.
func UnaryServerInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	resp, err := handler(ctx, req)
	if err != nil {
		errOut := ToGRPCError(err)
		s, _ := status.FromError(errOut)
		logger := log.With(zap.String("method", info.FullMethod), zap.Error(err), zap.Any("request", req))
		switch s.Code() {
		case codes.Unknown:
			logger.Warn("request handled with an unknown error")
		case codes.Internal:
			logger.Warn("request handled with an internal error")
		default:
			logger.Debug("request handled with an error")
		}
		return nil, errOut
	}

	log.With(
		zap.String("method", info.FullMethod),
		zap.Any("request", req),
		zap.Any("response", resp),
	).Debug("request handled successfully")
	return resp, nil
}
