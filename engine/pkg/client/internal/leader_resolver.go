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
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/engine/pkg/client/internal/endpoint"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
	"google.golang.org/grpc/serviceconfig"
)

const (
	schemaName = "tiflow"
)

// MasterServerList stores a map from server addresses to whether they are the leader.
type MasterServerList = map[string]bool

// FollowerSorterFn is a function used to customize the order of fail-over
// in case the servermaster's leader is unreachable.
type FollowerSorterFn = func(address []resolver.Address)

// LeaderResolver implements a gRPC resolver that handles the
// follower logic for servermaster clients.
type LeaderResolver struct {
	*manual.Resolver
	serviceConfig *serviceconfig.ParseResult

	serverListMu   sync.RWMutex
	serverList     MasterServerList
	updateNotifyCh chan struct{}

	doneCh chan struct{}
	wg     sync.WaitGroup

	FollowerSorter FollowerSorterFn
}

// NewLeaderResolver returns a new LeaderResolver.
func NewLeaderResolver(
	serverList MasterServerList,
) *LeaderResolver {
	return newLeaderResolverWithFollowerSorter(serverList, randomizedFollowerSorter)
}

func newLeaderResolverWithFollowerSorter(
	serverList MasterServerList,
	followerSorter FollowerSorterFn,
) *LeaderResolver {
	ret := &LeaderResolver{
		Resolver:       manual.NewBuilderWithScheme(schemaName),
		serverList:     serverList,
		updateNotifyCh: make(chan struct{}, 1),
		doneCh:         make(chan struct{}),
		FollowerSorter: followerSorter,
	}
	ret.wg.Add(1)
	go func() {
		defer ret.wg.Done()
		ret.bgUpdateServerList()
	}()

	return ret
}

func (b *LeaderResolver) bgUpdateServerList() {
	for {
		select {
		case <-b.doneCh:
			return
		case <-b.updateNotifyCh:
		}

		b.Resolver.UpdateState(b.getResolverState())
	}
}

func (b *LeaderResolver) getResolverState() resolver.State {
	b.serverListMu.RLock()
	defer b.serverListMu.RUnlock()

	if b.serverList == nil {
		panic("getResolverState must be called after UpdateServerList")
	}

	var (
		leaderAddr *resolver.Address
		addrList   []resolver.Address // without leader
	)

	for addr, isLeader := range b.serverList {
		if isLeader {
			if leaderAddr != nil {
				panic(fmt.Sprintf("Duplicate leaders: %s and %s", leaderAddr.Addr, addr))
			}
			leaderAddr = makeAddress(addr)
			continue
		}

		// Not leader
		addrList = append(addrList, *makeAddress(addr))
	}

	// Sorts the list of the followers to provide a fail-over order.
	b.FollowerSorter(addrList)

	if leaderAddr != nil {
		addrList = append([]resolver.Address{*leaderAddr}, addrList...)
	}

	return resolver.State{
		Addresses:     addrList,
		ServiceConfig: b.serviceConfig,
	}
}

// Close closes the LeaderResolver.
// It implements resolver.Resolver.
func (b *LeaderResolver) Close() {
	b.Resolver.Close()
	close(b.doneCh)
	b.wg.Wait()
}

// Build implements resolver.Builder.
// resolver.Builder is theoretically an abstract factory,
// but since we are using a one-one relationship between
// Builder and Resolver, we implement both interfaces on the
// same struct.
func (b *LeaderResolver) Build(
	target resolver.Target,
	cc resolver.ClientConn,
	opts resolver.BuildOptions,
) (resolver.Resolver, error) {
	b.serviceConfig = cc.ParseServiceConfig(`{"loadBalancingPolicy": "pick_first"}`)
	if b.serviceConfig.Err != nil {
		return nil, b.serviceConfig.Err
	}

	builtResolver, err := b.Resolver.Build(target, cc, opts)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Must update state once before returning,
	// so that the ClientConn knows where to connect.
	b.Resolver.UpdateState(b.getResolverState())
	return builtResolver, nil
}

// UpdateServerList should be called by engine's service discovery mechanism
// to update the serverList in a timely manner.
func (b *LeaderResolver) UpdateServerList(serverList MasterServerList) {
	updated := false

	b.serverListMu.Lock()
	if !reflect.DeepEqual(b.serverList, serverList) {
		b.serverList = serverList
		updated = true
	}
	b.serverListMu.Unlock()

	if !updated {
		return
	}

	select {
	case b.updateNotifyCh <- struct{}{}:
		log.L().Info("leader resolver state updated",
			zap.Any("server-list", b.serverList))
	default:
	}
}

// randomizedFollowerSorter randomizes the order of the addresses,
// to avoid a deterministic fail-over order.
// Time complexity: O(n).
func randomizedFollowerSorter(address []resolver.Address) {
	rand.Shuffle(len(address), func(i, j int) {
		address[i], address[j] = address[j], address[i]
	})
}

// orderedFollowerSorter sorts the addresses in lexicographical order.
// Used only for unit-testing for now.
// Time complexity: O(n log(n)).
func orderedFollowerSorter(address []resolver.Address) {
	sort.Slice(address, func(i, j int) bool {
		// Lexicographical comparison.
		// Note: strings.Compare(x, y) returns -1 iff x is considered less than y.
		return strings.Compare(address[i].Addr, address[j].Addr) < 0
	})
}

func makeAddress(ep string) *resolver.Address {
	addr, name := endpoint.Interpret(ep)
	return &resolver.Address{Addr: addr, ServerName: name}
}
