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
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
)

type mockResolverClientConn struct {
	resolver.ClientConn
	mock.Mock
}

func (cc *mockResolverClientConn) UpdateState(state resolver.State) error {
	args := cc.Called(state)
	return args.Error(0)
}

func (cc *mockResolverClientConn) ReportError(err error) {
	cc.Called(err)
}

func (cc *mockResolverClientConn) ParseServiceConfig(serviceConfigJSON string) *serviceconfig.ParseResult {
	args := cc.Called(serviceConfigJSON)
	return args.Get(0).(*serviceconfig.ParseResult)
}

func TestLeaderResolver(t *testing.T) {
	t.Parallel()

	mockConn := &mockResolverClientConn{}
	leaderResolver := NewLeaderResolver()
	defer leaderResolver.Close()

	mockConn.On("ParseServiceConfig", `{"loadBalancingPolicy": "pick_first"}`).
		Return(&serviceconfig.ParseResult{}).Once()

	res, err := leaderResolver.Build(resolver.Target{}, mockConn, resolver.BuildOptions{})
	require.NoError(t, err)

	res.ResolveNow(struct{}{})

	mockConn.AssertExpectations(t)
}
