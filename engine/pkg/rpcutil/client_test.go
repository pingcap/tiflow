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

package rpcutil

import (
	"context"
	"testing"

	"github.com/pingcap/errors"
	derror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type (
	Request  struct{}
	Response struct{}
)

var req *Request = nil

type mockRPCClient struct {
	addr string
	cnt  int
}

func (c *mockRPCClient) MockRPC(ctx context.Context, req *Request, opts ...grpc.CallOption) (*Response, error) {
	c.cnt++
	return nil, nil
}

func (c *mockRPCClient) MockFailRPC(ctx context.Context, req *Request, opts ...grpc.CallOption) (*Response, error) {
	c.cnt++
	return nil, errors.New("mock fail")
}

var testClient = &mockRPCClient{}

func mockDail(context.Context, string) (*mockRPCClient, CloseableConnIface, error) {
	return testClient, nil, nil
}

func TestFailoverRpcClients(t *testing.T) {
	ctx := context.Background()
	clients, err := NewFailoverRPCClients(ctx, []string{"url1", "url2"}, mockDail)
	require.NoError(t, err)
	_, err = DoFailoverRPC(ctx, clients, req, (*mockRPCClient).MockRPC)
	require.NoError(t, err)
	require.Equal(t, 1, testClient.cnt)
	require.Len(t, clients.Endpoints(), 2)

	// reset
	testClient.cnt = 0
	_, err = DoFailoverRPC(ctx, clients, req, (*mockRPCClient).MockFailRPC)
	require.ErrorContains(t, err, "mock fail")
	require.Equal(t, 2, testClient.cnt)

	clients.UpdateClients(ctx, []string{"url1", "url2", "url3"}, "")
	testClient.cnt = 0
	_, err = DoFailoverRPC(ctx, clients, req, (*mockRPCClient).MockFailRPC)
	require.ErrorContains(t, err, "mock fail")
	require.Equal(t, 3, testClient.cnt)
	require.Len(t, clients.Endpoints(), 3)
}

type closer struct{}

func (c *closer) Close() error {
	return nil
}

func mockDailWithAddr(_ context.Context, addr string) (*mockRPCClient, CloseableConnIface, error) {
	return &mockRPCClient{addr: addr}, &closer{}, nil
}

func TestValidLeaderAfterUpdateClients(t *testing.T) {
	ctx := context.Background()
	clients, err := NewFailoverRPCClients(ctx, []string{"url1", "url2"}, mockDailWithAddr)
	require.NoError(t, err)
	require.NotNil(t, clients.GetLeaderClient())

	clients.UpdateClients(ctx, []string{"url1", "url2", "url3"}, "url1")
	require.Equal(t, "url1", clients.GetLeaderClient().addr)

	clients.UpdateClients(ctx, []string{"url1", "url2", "url3"}, "not-in-set")
	require.NotNil(t, clients.GetLeaderClient())

	clients.UpdateClients(ctx, []string{}, "")
	require.Nil(t, clients.GetLeaderClient())

	_, err = DoFailoverRPC(ctx, clients, req, (*mockRPCClient).MockRPC)
	require.ErrorIs(t, err, derror.ErrNoRPCClient)
}

func mockDailWithAddrWithError(_ context.Context, addr string) (*mockRPCClient, CloseableConnIface, error) {
	if addr == "cant-dial" {
		return nil, nil, errors.New("can't dial RPC connection")
	}
	return &mockRPCClient{addr: addr}, &closer{}, nil
}

func TestFailToDialLeaderAfterwards(t *testing.T) {
	ctx := context.Background()
	clients, err := NewFailoverRPCClients(ctx, []string{"url1"}, mockDailWithAddrWithError)
	require.NoError(t, err)
	require.Equal(t, "url1", clients.GetLeaderClient().addr)

	clients.UpdateClients(ctx, []string{"cant-dial"}, "cant-dial")
	require.Nil(t, clients.GetLeaderClient())
	require.Equal(t, "", clients.leader)
	_, err = DoFailoverRPC(ctx, clients, req, (*mockRPCClient).MockRPC)
	require.ErrorIs(t, err, derror.ErrNoRPCClient)
	t.Log(err.Error())
}

func getMockTimeoutDial(succEventually bool) DialFunc[*mockRPCClient] {
	retryCount := 0
	return func(_ context.Context, addr string) (*mockRPCClient, CloseableConnIface, error) {
		retryCount++
		if succEventually && retryCount == defaultDialRetry {
			return &mockRPCClient{addr: addr}, &closer{}, nil
		}
		return nil, nil, errors.New("context deadline exceeded")
	}
}

func TestTimeout(t *testing.T) {
	ctx := context.Background()
	clients, err := NewFailoverRPCClients(ctx, []string{"timeout"}, getMockTimeoutDial(true))
	require.NoError(t, err)
	require.Equal(t, "timeout", clients.GetLeaderClient().addr)

	_, err = NewFailoverRPCClients(ctx, []string{"timeout"}, getMockTimeoutDial(false))
	require.Regexp(t, ".*failed to dial to master.*", err)
}
