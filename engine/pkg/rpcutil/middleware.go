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
	"reflect"
	"strings"
	"time"

	perrors "github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const metadataCauseKey = "cause"

// ToGRPCError converts an error to a gRPC error.
func ToGRPCError(errIn error) error {
	if errIn == nil {
		return nil
	}
	if _, ok := status.FromError(errIn); ok {
		return errIn
	}

	var (
		normalizedErr *perrors.Error
		metadata      map[string]string
		rfcCode       perrors.RFCErrorCode
		errMsg        string
	)
	if errors.As(errIn, &normalizedErr) {
		rfcCode = normalizedErr.RFCCode()
		if cause := normalizedErr.Cause(); cause != nil {
			metadata = map[string]string{
				metadataCauseKey: cause.Error(),
			}
		}
		errMsg = normalizedErr.GetMsg()
	} else {
		rfcCode = errors.ErrUnknown.RFCCode()
		errMsg = errIn.Error()
	}

	code := errors.GRPCStatusCode(errIn)
	st, err := status.New(code, errMsg).
		WithDetails(&errdetails.ErrorInfo{
			Reason:   string(rfcCode),
			Metadata: metadata,
		})
	if err != nil {
		return status.New(code, errMsg).Err()
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

	normalizedErr := perrors.Normalize(st.Message(), perrors.RFCCodeText(errInfo.Reason))
	if causeMsg := errInfo.Metadata[metadataCauseKey]; causeMsg != "" {
		return normalizedErr.Wrap(perrors.New(causeMsg)).GenWithStackByArgs()
	}
	return normalizedErr.GenWithStackByArgs()
}

// ForwardChecker is used for checking whether a request should be forwarded or not.
type ForwardChecker[T any] interface {
	// LeaderOnly returns whether the request is only allowed to handle by the leader.
	LeaderOnly(method string) bool
	// IsLeader returns whether the current node is the leader.
	IsLeader() bool
	// LeaderClient returns the leader client. If there is no leader, it should return
	// nil and errors.ErrMasterNoLeader.
	LeaderClient() (T, error)
}

// ForwardToLeader is a gRPC middleware that forwards the request to the leader if the current node is not the leader.
func ForwardToLeader[T any](fc ForwardChecker[T]) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, _ error) {
		method := extractMethod(info.FullMethod)
		if fc.IsLeader() || !fc.LeaderOnly(method) {
			return handler(ctx, req)
		}

		leaderCli, err := waitForLeader(ctx, fc)
		if err != nil {
			return nil, err
		}

		fv := reflect.ValueOf(leaderCli).MethodByName(method)
		if fv.IsZero() {
			return nil, status.Errorf(codes.Unimplemented, "method %s not implemented", method)
		}
		results := fv.Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(req)})
		if len(results) != 2 {
			log.Panic("invalid method signature", zap.String("method", method))
		}
		errI := results[1].Interface()
		if errI != nil {
			return nil, errI.(error)
		}
		return results[0].Interface(), nil
	}
}

const (
	waitForLeaderTimeout = 3 * time.Second
	waitForLeaderTick    = 300 * time.Millisecond
)

func waitForLeader[T any](ctx context.Context, fc ForwardChecker[T]) (leaderCli T, _ error) {
	leaderCli, err := fc.LeaderClient()
	if err == nil {
		return leaderCli, nil
	}
	if !errors.Is(err, errors.ErrMasterNoLeader) {
		return leaderCli, err
	}

	ticker := time.NewTicker(waitForLeaderTick)
	defer ticker.Stop()

	start := time.Now()
	for {
		if time.Since(start)+waitForLeaderTick > waitForLeaderTimeout {
			return leaderCli, errors.ErrMasterNoLeader.GenWithStackByArgs()
		}
		select {
		case <-ctx.Done():
			return leaderCli, errors.Trace(ctx.Err())
		case <-ticker.C:
			leaderCli, err = fc.LeaderClient()
			if err == nil {
				return leaderCli, nil
			}
			if !errors.Is(err, errors.ErrMasterNoLeader) {
				return leaderCli, err
			}
		}
	}
}

// FeatureChecker defines an interface that checks whether a feature is available
// or under degradation.
type FeatureChecker interface {
	Available(method string) bool
}

// CheckAvailable is a gRPC middleware that checks whether a method is ready to serve.
func CheckAvailable(fc FeatureChecker) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, _ error) {
		method := extractMethod(info.FullMethod)
		if !fc.Available(method) {
			return nil, errors.ErrMasterNotReady.GenWithStackByArgs()
		}
		return handler(ctx, req)
	}
}

// extract method name from full method name. fullMethod is the full RPC method string, i.e., /package.service/method.
func extractMethod(fullMethod string) string {
	return fullMethod[strings.LastIndexByte(fullMethod, '/')+1:]
}

// NormalizeError is a gRPC middleware that normalizes the error.
func NormalizeError() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, _ error) {
		resp, err := handler(ctx, req)
		if err != nil {
			errOut := ToGRPCError(err)
			s, _ := status.FromError(errOut)
			logger := log.L().With(zap.String("method", info.FullMethod), zap.Error(err), zap.Any("request", req))
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

		log.Debug("request handled successfully", zap.String("method", info.FullMethod), zap.Any("request", req), zap.Any("response", resp))
		return resp, nil
	}
}
