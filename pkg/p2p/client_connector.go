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

package p2p

import (
	"context"
	"net"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/security"
	proto "github.com/pingcap/tiflow/proto/p2p"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type clientConnectOptions struct {
	// network and addr are similar to the parameters
	// to net.Dial.
	network string
	addr    string

	// credential is used to setup the connection to the gRPC server.
	credential *security.Credential
	// timeout specifies the DialTimeout of the connection.
	timeout        time.Duration
	maxRecvMsgSize int
}

type cancelFn = func()

// clientConnector abstracts away the underlying gRPC operation of dialing and
// the creation of a gRPC client.
type clientConnector interface {
	Connect(opts clientConnectOptions) (proto.CDCPeerToPeerClient, cancelFn, error)
}

type clientConnectorImpl struct{}

func newClientConnector() clientConnector {
	return &clientConnectorImpl{}
}

// Connect creates a gRPC client connection to the server.
// the returned cancelFn must be called to close the connection.
func (c *clientConnectorImpl) Connect(opts clientConnectOptions) (proto.CDCPeerToPeerClient, cancelFn, error) {
	securityOption, err := opts.credential.ToGRPCDialOption()
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	conn, err := grpc.Dial(
		opts.addr,
		securityOption,
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(opts.maxRecvMsgSize)),
		grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) {
			return net.DialTimeout(opts.network, s, opts.timeout)
		}),
		// We do not need a unary interceptor since we are not making any unary calls.
		grpc.WithStreamInterceptor(grpcClientMetrics.StreamClientInterceptor()))
	if err != nil {
		log.Warn("gRPC dial error", zap.Error(err))
		return nil, nil, errors.Trace(err)
	}

	cancel := func() {
		if err := conn.Close(); err != nil {
			log.Warn("gRPC connection close error", zap.Error(err))
		}
	}

	return proto.NewCDCPeerToPeerClient(conn), cancel, nil
}
