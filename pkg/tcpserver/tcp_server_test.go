// Copyright 2020 PingCAP, Inc.
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

package tcp_server

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/integralist/go-findroot/find"
	"github.com/phayes/freeport"
	"github.com/pingcap/ticdc/pkg/httputil"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/pingcap/ticdc/pkg/util/testleak"
	"github.com/pingcap/ticdc/proto/mock_service"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestTCPServerInsecureHTTP1(t *testing.T) {
	defer testleak.AfterTestT(t)

	port, err := freeport.GetFreePort()
	require.NoError(t, err)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	server, err := NewTCPServer(addr, &security.Credential{})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := server.Run(ctx)
		require.Error(t, err)
		require.Regexp(t, ".*ErrTCPServerClosed.*", err.Error())
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		testWithHTTPWorkload(ctx, t, server, addr, &security.Credential{})
		cancel()
	}()

	wg.Wait()
}

func TestTCPServerTLSHTTP1(t *testing.T) {
	defer testleak.AfterTestT(t)

	port, err := freeport.GetFreePort()
	require.NoError(t, err)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	server, err := NewTCPServer(addr, makeCredential4Testing(t))
	require.NoError(t, err)
	require.True(t, server.IsTLSEnabled())

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := server.Run(ctx)
		require.Error(t, err)
		require.Regexp(t, ".*ErrTCPServerClosed.*", err.Error())
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer cancel()
		testWithHTTPWorkload(ctx, t, server, addr, makeCredential4Testing(t))
	}()

	wg.Wait()
}

func TestTCPServerInsecureGrpc(t *testing.T) {
	defer testleak.AfterTestT(t)

	port, err := freeport.GetFreePort()
	require.NoError(t, err)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	server, err := NewTCPServer(addr, &security.Credential{})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := server.Run(ctx)
		require.Error(t, err)
		require.Regexp(t, ".*ErrTCPServerClosed.*", err.Error())
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		testWithGrpcWorkload(ctx, t, server, addr, &security.Credential{})
		cancel()
	}()

	wg.Wait()
}

func TestTCPServerTLSGrpc(t *testing.T) {
	defer testleak.AfterTestT(t)

	port, err := freeport.GetFreePort()
	require.NoError(t, err)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	server, err := NewTCPServer(addr, makeCredential4Testing(t))
	require.NoError(t, err)
	require.True(t, server.IsTLSEnabled())

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := server.Run(ctx)
		require.Error(t, err)
		require.Regexp(t, ".*ErrTCPServerClosed.*", err.Error())
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		testWithGrpcWorkload(ctx, t, server, addr, makeCredential4Testing(t))
		cancel()
	}()

	wg.Wait()
}

func makeCredential4Testing(t *testing.T) *security.Credential {
	stat, err := find.Repo()
	require.NoError(t, err)

	tlsPath := fmt.Sprintf("%s/tests/_certificates/", stat.Path)
	return &security.Credential{
		CAPath:        path.Join(tlsPath, "ca.pem"),
		CertPath:      path.Join(tlsPath, "server.pem"),
		KeyPath:       path.Join(tlsPath, "server-key.pem"),
		CertAllowedCN: nil,
	}
}

func testWithHTTPWorkload(ctx context.Context, t *testing.T, server TCPServer, addr string, credentials *security.Credential) {
	httpServer := &http.Server{}
	http.HandleFunc("/", func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(200)
		_, err := writer.Write([]byte("ok"))
		require.NoError(t, err)
	})
	defer func() {
		http.DefaultServeMux = http.NewServeMux()
	}()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := httpServer.Serve(server.HTTP1Listener())
		if err != nil && err != http.ErrServerClosed {
			require.FailNow(t,
				"unexpected error from http server",
				"%d",
				err.Error())
		}
	}()

	scheme := "http"
	if credentials.IsTLSEnabled() {
		scheme = "https"
	}

	cli, err := httputil.NewClient(credentials)
	require.NoError(t, err)

	uri := fmt.Sprintf("%s://%s/", scheme, addr)
	resp, err := cli.Get(uri)
	require.NoError(t, err)
	defer func() {
		_ = resp.Body.Close()
	}()
	require.Equal(t, 200, resp.StatusCode)

	body, err := ioutil.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, "ok", string(body))

	err = httpServer.Close()
	require.NoError(t, err)
}

type mockServiceServer struct {
	t *testing.T
}

func (s *mockServiceServer) SampleUnaryCall(ctx context.Context, data *mock_service.DummyData) (*mock_service.DummyData, error) {
	ret := &mock_service.DummyData{Value: data.Value + 1}
	return ret, nil
}

func (s *mockServiceServer) SampleStreamCall(server mock_service.SimpleService_SampleStreamCallServer) error {
	for i := 0; i < 10; i++ {
		data, err := server.Recv()
		require.NoError(s.t, err)
		require.Equal(s.t, int32(i), data.Value)

		err = server.Send(&mock_service.DummyData{
			Value: int32(i + 1),
		})
		require.NoError(s.t, err)
	}
	return nil
}

func testWithGrpcWorkload(ctx context.Context, t *testing.T, server TCPServer, addr string, credentials *security.Credential) {
	grpcServer := grpc.NewServer()
	mock_service.RegisterSimpleServiceServer(grpcServer, &mockServiceServer{t})

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := grpcServer.Serve(server.GrpcListener())
		require.NoError(t, err)
	}()

	var conn *grpc.ClientConn
	if credentials.IsTLSEnabled() {
		tlsOptions, err := credentials.ToGRPCDialOption()
		require.NoError(t, err)
		conn, err = grpc.Dial(addr, tlsOptions)
		require.NoError(t, err)
	} else {
		var err error
		conn, err = grpc.Dial(addr, grpc.WithInsecure())
		require.NoError(t, err)
	}

	client := mock_service.NewSimpleServiceClient(conn)

	for i := 0; i < 5; i++ {
		result, err := client.SampleUnaryCall(ctx, &mock_service.DummyData{
			Value: int32(i),
		})
		require.NoError(t, err)
		require.Equal(t, int32(i+1), result.Value)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()

		stream, err := client.SampleStreamCall(ctx)
		require.NoError(t, err)

		for i := 0; i < 10; i++ {
			err := stream.Send(&mock_service.DummyData{
				Value: int32(i),
			})
			require.NoError(t, err)

			received, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, int32(i+1), received.Value)
		}

		grpcServer.GracefulStop()
	}()

	wg.Wait()
}
