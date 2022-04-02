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

package tcpserver

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"path"
	"sync"
	"testing"
	"time"

	grpcTesting "github.com/grpc-ecosystem/go-grpc-middleware/testing"
	grpcTestingProto "github.com/grpc-ecosystem/go-grpc-middleware/testing/testproto"
	"github.com/integralist/go-findroot/find"
	"github.com/phayes/freeport"
	"github.com/pingcap/tiflow/pkg/httputil"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestTCPServerInsecureHTTP1(t *testing.T) {
	port, err := freeport.GetFreePort()
	require.NoError(t, err)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	server, err := NewTCPServer(addr, &security.Credential{})
	require.NoError(t, err)
	defer func() {
		err := server.Close()
		require.NoError(t, err)
	}()

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
	port, err := freeport.GetFreePort()
	require.NoError(t, err)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	server, err := NewTCPServer(addr, makeCredential4Testing(t))
	require.NoError(t, err)
	require.True(t, server.IsTLSEnabled())

	defer func() {
		err := server.Close()
		require.NoError(t, err)
	}()

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
	port, err := freeport.GetFreePort()
	require.NoError(t, err)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	server, err := NewTCPServer(addr, &security.Credential{})
	require.NoError(t, err)

	defer func() {
		err := server.Close()
		require.NoError(t, err)
	}()

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
	port, err := freeport.GetFreePort()
	require.NoError(t, err)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	server, err := NewTCPServer(addr, makeCredential4Testing(t))
	require.NoError(t, err)
	require.True(t, server.IsTLSEnabled())

	defer func() {
		err := server.Close()
		require.NoError(t, err)
	}()

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

	tlsPath := fmt.Sprintf("%s/tests/integration_tests/_certificates/", stat.Path)
	return &security.Credential{
		CAPath:        path.Join(tlsPath, "ca.pem"),
		CertPath:      path.Join(tlsPath, "server.pem"),
		KeyPath:       path.Join(tlsPath, "server-key.pem"),
		CertAllowedCN: nil,
	}
}

func testWithHTTPWorkload(_ context.Context, t *testing.T, server TCPServer, addr string, credentials *security.Credential) {
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
	resp, err := cli.Get(context.Background(), uri)
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

	wg.Wait()
}

func testWithGrpcWorkload(ctx context.Context, t *testing.T, server TCPServer, addr string, credentials *security.Credential) {
	grpcServer := grpc.NewServer()
	service := &grpcTesting.TestPingService{T: t}
	grpcTestingProto.RegisterTestServiceServer(grpcServer, service)

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
	defer func() {
		_ = conn.Close()
	}()

	client := grpcTestingProto.NewTestServiceClient(conn)

	for i := 0; i < 5; i++ {
		result, err := client.Ping(ctx, &grpcTestingProto.PingRequest{
			Value: fmt.Sprintf("%d", i),
		})
		require.NoError(t, err)
		require.Equal(t, fmt.Sprintf("%d", i), result.Value)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer grpcServer.GracefulStop()

		stream, err := client.PingStream(ctx)
		require.NoError(t, err)

		for i := 0; i < 10; i++ {
			err := stream.Send(&grpcTestingProto.PingRequest{
				Value: fmt.Sprintf("%d", i),
			})
			require.NoError(t, err)

			received, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, fmt.Sprintf("%d", i), received.Value)
		}
	}()

	wg.Wait()
}

func TestTcpServerClose(t *testing.T) {
	port, err := freeport.GetFreePort()
	require.NoError(t, err)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	server, err := NewTCPServer(addr, &security.Credential{})
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := server.Run(ctx)
		require.Error(t, err)
		require.Regexp(t, ".*ErrTCPServerClosed.*", err.Error())
	}()

	httpServer := &http.Server{}
	http.HandleFunc("/", func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(200)
		_, err := writer.Write([]byte("ok"))
		require.NoError(t, err)
	})
	defer func() {
		http.DefaultServeMux = http.NewServeMux()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := httpServer.Serve(server.HTTP1Listener())
		require.Error(t, err)
		require.Regexp(t, ".*mux: server closed.*", err.Error())
	}()

	cli, err := httputil.NewClient(&security.Credential{})
	require.NoError(t, err)

	uri := fmt.Sprintf("http://%s/", addr)
	resp, err := cli.Get(context.Background(), uri)
	require.NoError(t, err)
	defer func() {
		_ = resp.Body.Close()
	}()
	require.Equal(t, 200, resp.StatusCode)

	// Close should be idempotent.
	for i := 0; i < 3; i++ {
		err := server.Close()
		require.NoError(t, err)
	}

	wg.Wait()
}
