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
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/pkg/rpcerror"
)

type fakeError struct {
	rpcerror.Error[rpcerror.NotRetryable, rpcerror.Unavailable]
	Val string `json:"val"`
}

func TestCallError(t *testing.T) {
	errFake := rpcerror.Normalize[fakeError]()
	mockQueryJob := func(ctx context.Context, in *pb.QueryJobRequest, opts ...grpc.CallOption) (*pb.QueryJobResponse, error) {
		return nil, errFake.GenWithStack(&fakeError{
			Val: "test",
		})
	}

	_, err := Call(context.Background(), mockQueryJob, &pb.QueryJobRequest{})
	require.Error(t, err)
	require.True(t, errFake.Is(err))

	info, ok := errFake.Convert(err)
	require.True(t, ok)
	require.Equal(t, &fakeError{
		Val: "test",
	}, info)
}
