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
	"reflect"
	"testing"
	"time"

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
	leaderResolver := newLeaderResolverWithFollowerSorter(map[string]bool{
		"leader":     true,
		"follower-1": false,
		"follower-2": false,
	}, orderedFollowerSorter)
	defer leaderResolver.Close()

	mockConn.On("UpdateState", mock.MatchedBy(func(state resolver.State) bool {
		return reflect.DeepEqual(state.Addresses, []resolver.Address{
			{Addr: "leader", ServerName: "leader"},
			{Addr: "follower-1", ServerName: "follower-1"},
			{Addr: "follower-2", ServerName: "follower-2"},
		})
	})).Return(nil).Once()
	mockConn.On("ParseServiceConfig", `{"loadBalancingPolicy": "pick_first"}`).
		Return(&serviceconfig.ParseResult{}).Once()

	res, err := leaderResolver.Build(resolver.Target{}, mockConn, resolver.BuildOptions{})
	require.NoError(t, err)

	res.ResolveNow(struct{}{})

	mockConn.On("UpdateState", mock.MatchedBy(func(state resolver.State) bool {
		return reflect.DeepEqual(state.Addresses, []resolver.Address{
			{Addr: "leader", ServerName: "leader"},
			{Addr: "follower-2", ServerName: "follower-2"},
			{Addr: "follower-3", ServerName: "follower-3"},
		})
	})).Return(nil).Once()
	leaderResolver.UpdateServerList(map[string]bool{
		"leader":     true,
		"follower-2": false,
		"follower-3": false,
	})
	time.Sleep(100 * time.Millisecond)

	mockConn.AssertExpectations(t)
}

func TestLeaderResolverFrequentUpdate(t *testing.T) {
	t.Parallel()

	mockConn := &mockResolverClientConn{}
	leaderResolver := newLeaderResolverWithFollowerSorter(map[string]bool{
		"leader":     true,
		"follower-1": false,
		"follower-2": false,
	}, orderedFollowerSorter)
	defer leaderResolver.Close()

	mockConn.On("UpdateState", mock.MatchedBy(func(state resolver.State) bool {
		return reflect.DeepEqual(state.Addresses, []resolver.Address{
			{Addr: "leader", ServerName: "leader"},
			{Addr: "follower-1", ServerName: "follower-1"},
			{Addr: "follower-2", ServerName: "follower-2"},
		})
	})).Return(nil)
	mockConn.On("ParseServiceConfig", `{"loadBalancingPolicy": "pick_first"}`).
		Return(&serviceconfig.ParseResult{}).Once()

	_, err := leaderResolver.Build(resolver.Target{}, mockConn, resolver.BuildOptions{})
	require.NoError(t, err)

	for i := 0; i < 1000; i++ {
		leaderResolver.UpdateServerList(map[string]bool{
			"leader":     true,
			"follower-1": false,
			"follower-2": false,
		})
	}
	time.Sleep(100 * time.Millisecond)

	mockConn.AssertExpectations(t)
}

func TestLeaderResolverUpdateAfterClose(t *testing.T) {
	t.Parallel()

	mockConn := &mockResolverClientConn{}
	leaderResolver := newLeaderResolverWithFollowerSorter(map[string]bool{
		"leader":     true,
		"follower-1": false,
		"follower-2": false,
	}, orderedFollowerSorter)

	mockConn.On("UpdateState", mock.MatchedBy(func(state resolver.State) bool {
		return reflect.DeepEqual(state.Addresses, []resolver.Address{
			{Addr: "leader", ServerName: "leader"},
			{Addr: "follower-1", ServerName: "follower-1"},
			{Addr: "follower-2", ServerName: "follower-2"},
		})
	})).Return(nil)
	mockConn.On("ParseServiceConfig", `{"loadBalancingPolicy": "pick_first"}`).
		Return(&serviceconfig.ParseResult{}).Once()

	_, err := leaderResolver.Build(resolver.Target{}, mockConn, resolver.BuildOptions{})
	require.NoError(t, err)

	// Close the resolver now.
	leaderResolver.Close()

	// Updating the server list after the resolver is closed
	// should not panic or deadlock.
	for i := 0; i < 1000; i++ {
		leaderResolver.UpdateServerList(map[string]bool{
			"leader":     true,
			"follower-1": false,
			"follower-2": false,
		})
	}
}
