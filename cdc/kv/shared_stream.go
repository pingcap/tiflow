// Copyright 2023 PingCAP, Inc.
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

package kv

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/cdcpb"
	grpccodes "google.golang.org/grpc/codes"
	connectivity "google.golang.org/grpc/connectivity"
	grpcstatus "google.golang.org/grpc/status"
)

type polymorphicStream struct {
	multiplexing bool
	conn         *sharedConn
	client       cdcpb.ChangeData_EventFeedV2Client
	releaseConn  func()
}

func initStream(ctx context.Context, grpcPool GrpcPool, addr string) (_ *polymorphicStream, err error) {
	var conn *sharedConn
	var client cdcpb.ChangeData_EventFeedV2Client
	defer func() {
		if err != nil {
			if client != nil {
				_ = client.CloseSend()
			}
			if conn != nil {
				grpcPool.ReleaseConn(conn, addr)
			}
		}
	}()

	if conn, err = grpcPool.GetConn(addr); err != nil {
		return nil, errors.Trace(err)
	}
	state := conn.GetState()
	for state != connectivity.Ready {
		ctxConn, cancel := context.WithDeadline(ctx, time.Now().Add(time.Second))
		conn.WaitForStateChange(ctxConn, state)
		cancel()
	}

	rpc := cdcpb.NewChangeDataClient(conn.ClientConn)

	multiplexing := true
	ctx = getContextFromFeatures(ctx, []string{rpcMetaFeatureStreamMultiplexing})
	if client, err = rpc.EventFeedV2(ctx); err != nil {
		return nil, errors.Trace(err)
	}
	_ = client.Send(&cdcpb.ChangeDataRequest{
		RequestId: 0,
		RegionId:  0,
		Request:   &cdcpb.ChangeDataRequest_Deregister_{},
	})
	if _, err = client.Recv(); err != nil {
		if grpcstatus.Code(err) == grpccodes.Unimplemented {
			if client, err = rpc.EventFeed(ctx); err != nil {
				multiplexing = false
				return nil, errors.Trace(err)
			}
		}
	}

	releaseConn := func() { grpcPool.ReleaseConn(conn, addr) }
	return &polymorphicStream{multiplexing, conn, client, releaseConn}, nil
}
