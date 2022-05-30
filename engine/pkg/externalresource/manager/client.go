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

package manager

import (
	"context"
	"time"

	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/pkg/errors"
	"github.com/pingcap/tiflow/engine/pkg/rpcutil"
	"google.golang.org/grpc"
)

const dialTimeout = 5 * time.Second

var dialImpl = func(ctx context.Context, addr string) (pb.ResourceManagerClient, rpcutil.CloseableConnIface, error) {
	ctx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()
	conn, err := grpc.DialContext(ctx, addr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return nil, nil, errors.Wrap(errors.ErrGrpcBuildConn, err)
	}
	return pb.NewResourceManagerClient(conn), conn, nil
}

// NewResourceClient creates a new resource manager rpc client
func NewResourceClient(ctx context.Context, join []string,
) (*rpcutil.FailoverRPCClients[pb.ResourceManagerClient], error) {
	clients, err := rpcutil.NewFailoverRPCClients(ctx, join, dialImpl)
	if err != nil {
		return nil, err
	}
	return clients, nil
}
