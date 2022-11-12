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

package mock

import (
	"context"
	"io"
	"sync/atomic"

	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type testServer struct {
	*baseServer
	pb.TestServiceServer
}

type testServerConn struct {
	server *testServer
	stream *testStream
}

func (s *testServer) dial() (Conn, error) {
	return &testServerConn{s, nil}, nil
}

type testClient struct {
	conn Conn
}

func (t *testClient) FeedBinlog(ctx context.Context, in *pb.TestBinlogRequest, opts ...grpc.CallOption) (pb.TestService_FeedBinlogClient, error) {
	resp, err := t.conn.sendRequest(ctx, in)
	return resp.(pb.TestService_FeedBinlogClient), err
}

func (s *testServerConn) Close() error {
	return nil
}

type testStream struct {
	ctx    context.Context
	data   chan *pb.Record
	err    error
	closed int32
}

func (s *testStream) close() {
	if atomic.CompareAndSwapInt32(&s.closed, 0, 1) {
		s.err = io.EOF
		close(s.data)
	}
}

func (s *testStream) Send(r *pb.Record) error {
	if atomic.LoadInt32(&s.closed) == 1 {
		return errors.New("stream has been closed")
	}
	s.data <- r
	return nil
}

func (s *testStream) Recv() (*pb.Record, error) {
	select {
	case r := <-s.data:
		if r == nil {
			return nil, s.err
		}
		return r, nil
	case <-s.ctx.Done():
		return nil, errors.New("canceled")
	}
}

func (s *testStream) SetHeader(metadata.MD) error {
	return errors.New("unimplemented")
}

func (s *testStream) SendHeader(metadata.MD) error {
	return errors.New("unimplemented")
}

func (s *testStream) SetTrailer(metadata.MD) {}

func (s *testStream) Context() context.Context { return nil }

func (s *testStream) SendMsg(interface{}) error {
	return errors.New("unimplemented")
}

func (s *testStream) RecvMsg(interface{}) error {
	return errors.New("unimplemented")
}

func (s *testStream) Header() (metadata.MD, error) {
	return nil, errors.New("unimplemented")
}

func (s *testStream) Trailer() metadata.MD { return nil }

func (s *testStream) CloseSend() error {
	return errors.New("unimplemented")
}

func (s *testServerConn) sendRequest(ctx context.Context, req interface{}) (interface{}, error) {
	switch x := req.(type) {
	case *pb.TestBinlogRequest:
		stream := &testStream{
			data: make(chan *pb.Record, 1024),
			ctx:  ctx,
		}
		if s.stream != nil {
			return nil, errors.New("internal error")
		}
		s.stream = stream
		go func() {
			stream.err = s.server.FeedBinlog(x, stream)
			stream.close()
		}()
		return stream, nil
	}
	return nil, errors.New("unknown request")
}

// NewTestServer creates a mock gRPC server
func NewTestServer(addr string, server pb.TestServiceServer) (GrpcServer, error) {
	container.mu.Lock()
	defer container.mu.Unlock()
	_, ok := container.servers[addr]
	if ok {
		return nil, errors.New("addr " + addr + " has been listened")
	}
	newServer := &testServer{&baseServer{addr}, server}
	container.servers[addr] = newServer
	return newServer, nil
}

// NewTestClient creates a mock test service client
func NewTestClient(conn Conn) pb.TestServiceClient {
	return &testClient{conn}
}
