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

package client

import (
	"context"

	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"

	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/test"
	"github.com/pingcap/tiflow/engine/test/mock"
	"github.com/pingcap/tiflow/pkg/errors"
)

// baseExecutorClient handles requests and responses at the gRPC method level.
type baseExecutorClient interface {
	Send(context.Context, *ExecutorRequest) (*ExecutorResponse, error)
}

// closeableConnIface abstracts a gRPC connection.
type closeableConnIface interface {
	Close() error
}

// baseExecutorClientImpl implements baseExecutorClient.
// TODO unit tests.
type baseExecutorClientImpl struct {
	conn         closeableConnIface
	client       pb.ExecutorClient
	brokerClient pb.BrokerServiceClient
}

func newExecutorClientForTest(addr string) (*baseExecutorClientImpl, error) {
	conn, err := mock.Dial(addr)
	if err != nil {
		return nil, errors.ErrGrpcBuildConn.GenWithStackByArgs(addr)
	}
	return &baseExecutorClientImpl{
		conn:   conn,
		client: mock.NewExecutorClient(conn),
	}, nil
}

func newBaseExecutorClient(addr string) (*baseExecutorClientImpl, error) {
	if test.GetGlobalTestFlag() {
		return newExecutorClientForTest(addr)
	}
	// NOTE We use a non-blocking dial (which is the default),
	// so err could be nil even if an invalid address is given.
	// TODO We need a way to 1) remove failed clients
	// 2) prevent stale executor's client from being created
	conn, err := grpc.Dial(
		addr,
		grpc.WithInsecure(),
		grpc.WithConnectParams(grpc.ConnectParams{Backoff: backoff.DefaultConfig}),
		// We log gRPC requests here to aid debugging
		// TODO add a switch to turn off the gRPC request log.
		grpc.WithUnaryInterceptor(grpc_zap.UnaryClientInterceptor(log.L())))
	if err != nil {
		return nil, errors.ErrGrpcBuildConn.GenWithStackByArgs(addr)
	}

	return &baseExecutorClientImpl{
		conn:         conn,
		client:       pb.NewExecutorClient(conn),
		brokerClient: pb.NewBrokerServiceClient(conn),
	}, nil
}

func (c *baseExecutorClientImpl) Send(ctx context.Context, req *ExecutorRequest) (*ExecutorResponse, error) {
	resp := &ExecutorResponse{}
	var err error
	switch req.Cmd {
	case CmdPreDispatchTask:
		resp.Resp, err = c.client.PreDispatchTask(ctx, req.PreDispatchTask())
	case CmdConfirmDispatchTask:
		resp.Resp, err = c.client.ConfirmDispatchTask(ctx, req.ConfirmDispatchTask())
	case CmdRemoveLocalResourceRequest:
		resp.Resp, err = c.brokerClient.RemoveResource(ctx, req.RemoveLocalResourceRequest())
	}
	if err != nil {
		log.Error("send req meet error", zap.Error(err))
	}
	return resp, err
}

// CmdType represents the request type when dispatching task from server master to executor.
type CmdType uint16

// CmdType values.
const (
	CmdPreDispatchTask CmdType = 1 + iota
	CmdConfirmDispatchTask
	CmdRemoveLocalResourceRequest
)

// ExecutorRequest wraps CmdType and dispatch task request object
type ExecutorRequest struct {
	Cmd CmdType
	Req interface{}
}

// PreDispatchTask unwraps gRPC PreDispatchTaskRequest from ExecutorRequest
func (e *ExecutorRequest) PreDispatchTask() *pb.PreDispatchTaskRequest {
	return e.Req.(*pb.PreDispatchTaskRequest)
}

// ConfirmDispatchTask unwraps gRPC ConfirmDispatchTask from ExecutorRequest
func (e *ExecutorRequest) ConfirmDispatchTask() *pb.ConfirmDispatchTaskRequest {
	return e.Req.(*pb.ConfirmDispatchTaskRequest)
}

// RemoveLocalResourceRequest unwraps gRPC RemoveLocalResourceRequest from ExecutorRequest
func (e *ExecutorRequest) RemoveLocalResourceRequest() *pb.RemoveLocalResourceRequest {
	return e.Req.(*pb.RemoveLocalResourceRequest)
}

// ExecutorResponse wraps DispatchTaskResponse object
type ExecutorResponse struct {
	Resp interface{}
}
