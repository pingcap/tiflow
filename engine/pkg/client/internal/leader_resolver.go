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
	"sync"

	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
	"google.golang.org/grpc/serviceconfig"
)

const (
	schemaName = "tiflow_masters"
)

// MasterServerList stores a map from server addresses to whether they are the leader.
type MasterServerList = map[string]bool

type LeaderResolver struct {
	*manual.Resolver
	serviceConfig *serviceconfig.ParseResult

	serverListMu   sync.RWMutex
	serverList     MasterServerList
	updateNotifyCh chan struct{}

	doneCh chan struct{}
	wg     sync.WaitGroup
}

func NewLeaderResolver() *LeaderResolver {
	ret := &LeaderResolver{
		Resolver:       manual.NewBuilderWithScheme(schemaName),
		updateNotifyCh: make(chan struct{}, 1),
		doneCh:         make(chan struct{}),
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

	var (
		leaderAddr *resolver.Address
		addrList   []resolver.Address // without leader
	)

	for addr, isLeader := range b.serverList {
		if isLeader {
			if leaderAddr != nil {
				panic(fmt.Sprintf("Duplicate leaders: %s and %s", leaderAddr.Addr, addr))
			}
			leaderAddr = &resolver.Address{
				Addr: addr,
			}
			continue
		}

		// Not leader
		addrList = append(addrList, resolver.Address{Addr: addr})
	}

	addrListWithLeader := append([]resolver.Address{*leaderAddr}, addrList...)

	return resolver.State{
		Addresses:     addrListWithLeader,
		ServiceConfig: b.serviceConfig,
	}
}

func (b *LeaderResolver) Close() {
	b.Resolver.Close()
	close(b.doneCh)
	b.wg.Wait()
}

func (b *LeaderResolver) Build(
	target resolver.Target,
	cc resolver.ClientConn,
	opts resolver.BuildOptions,
) (resolver.Resolver, error) {
	b.serviceConfig = cc.ParseServiceConfig(`{"loadBalancingPolicy": "pick_first"}`)
	if b.serviceConfig.Err != nil {
		return nil, b.serviceConfig.Err
	}

	return b.Resolver.Build(target, cc, opts)
}

func (b *LeaderResolver) UpdateServerList(serverList MasterServerList) {
	b.serverListMu.Lock()
	defer b.serverListMu.Unlock()

	b.serverList = serverList
}
