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

	"google.golang.org/grpc"
)

func Call[ReqT any, RespT any, F func(context.Context, ReqT, ...grpc.CallOption) (RespT, error)](
	ctx context.Context,
	f F,
	req ReqT,
) (RespT, error) {
	return f(ctx, req)
}
