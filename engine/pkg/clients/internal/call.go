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

package internal

import (
	"context"
	"time"

	"google.golang.org/grpc"

	"github.com/pingcap/tiflow/engine/pkg/rpcerror"
	"github.com/pingcap/tiflow/pkg/retry"
)

type Call[ReqT any, RespT any, F func(context.Context, ReqT, ...grpc.CallOption) (RespT, error)] struct {
	f       F
	request ReqT
	opts    *callerOpts
}

type callerOpts struct {
	forceNoRetry bool
}

type CallOption func(*callerOpts)

func WithForceNoRetry() CallOption {
	return func(opts *callerOpts) {
		opts.forceNoRetry = true
	}
}

func NewCall[F func(context.Context, ReqT, ...grpc.CallOption) (RespT, error), ReqT any, RespT any](f F,
	req ReqT, ops ...CallOption) *Call[ReqT, RespT, F] {

	opts := &callerOpts{}

	for _, op := range ops {
		op(opts)
	}

	return &Call[ReqT, RespT, F]{
		f:       f,
		request: req,
		opts:    opts,
	}

}

// Do calls a grpc client function.
func (c *Call[ReqT, RespT, F]) Do(
	ctx context.Context,
) (RespT, error) {
	var resp RespT
	err := retry.Do(ctx, func() error {
		var err error
		resp, err = c.callOnce(ctx)
		return err
	}, retry.WithIsRetryableErr(c.isRetryable),
		retry.WithBackoffBaseDelay(10),
		retry.WithBackoffMaxDelay(1000),
		retry.WithTotalRetryDuratoin(10*time.Second))
	return resp, err
}

func (c *Call[ReqT, RespT, F]) callOnce(ctx context.Context) (RespT, error) {
	var zeroResp RespT

	resp, err := c.f(ctx, c.request)
	if err != nil {
		return zeroResp, rpcerror.FromGRPCError(err)
	}

	return resp, nil
}

func (c *Call[ReqT, RespT, F]) isRetryable(errIn error) bool {
	if c.opts.forceNoRetry {
		return false
	}

	return rpcerror.IsRetryable(errIn)
}
