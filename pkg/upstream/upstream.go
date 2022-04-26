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

package upstream

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tiflow/cdc/kv"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/pdtime"
	"github.com/pingcap/tiflow/pkg/txnutil/gc"
	"github.com/pingcap/tiflow/pkg/version"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
)

type status int

const (
	ready status = iota
	normal
)

// Upstream holds resources of a upstream, it can be shared by many changefeeds
// and processors.
type Upstream struct {
	clusterID      uint64
	pdEndpoints    []string
	securityConfig *config.SecurityConfig

	PDClient    pd.Client
	KVStorage   tidbkv.Storage
	GrpcPool    kv.GrpcPool
	RegionCache *tikv.RegionCache
	PDClock     pdtime.Clock
	GCManager   gc.Manager

	wg     *sync.WaitGroup
	status status
}

func newUpstream(clusterID uint64, pdEndpoints []string, securityConfig *config.SecurityConfig) *Upstream {
	return &Upstream{
		clusterID: clusterID, pdEndpoints: pdEndpoints,
		securityConfig: securityConfig, status: ready,
	}
}

// NewUpstream4Test new a upstream for unit test.
func NewUpstream4Test(pdClient pd.Client) *Upstream {
	pdClock := pdtime.NewClock4Test()
	gcManager := gc.NewManager(pdClient, pdClock)
	res := &Upstream{PDClient: pdClient, PDClock: pdClock, GCManager: gcManager}
	res.status = normal
	return res
}

func (up *Upstream) init(ctx context.Context) error {
	log.Info("upstream is initializing")
	var err error

	grpcTLSOption, err := up.securityConfig.ToGRPCDialOption()
	if err != nil {
		return errors.Trace(err)
	}

	pdClient, err := pd.NewClientWithContext(
		ctx, up.pdEndpoints, up.securityConfig.PDSecurityOption(),
		pd.WithGRPCDialOptions(
			grpcTLSOption,
			grpc.WithBlock(),
			grpc.WithConnectParams(grpc.ConnectParams{
				Backoff: backoff.Config{
					BaseDelay:  time.Second,
					Multiplier: 1.1,
					Jitter:     0.1,
					MaxDelay:   3 * time.Second,
				},
				MinConnectTimeout: 3 * time.Second,
			}),
		))
	if err != nil {
		return errors.Trace(err)
	}
	up.PDClient = pdClient

	// To not block CDC server startup, we need to warn instead of error
	// when TiKV is incompatible.
	errorTiKVIncompatible := false
	err = version.CheckClusterVersion(ctx, up.PDClient, up.pdEndpoints, up.securityConfig, errorTiKVIncompatible)
	if err != nil {
		log.Error("init upstream error", zap.Error(err))
	}

	kvStore, err := kv.CreateTiStore(strings.Join(up.pdEndpoints, ","), up.securityConfig)
	if err != nil {
		return errors.Trace(err)
	}
	up.KVStorage = kvStore

	up.GrpcPool = kv.NewGrpcPoolImpl(ctx, up.securityConfig)
	up.RegionCache = tikv.NewRegionCache(up.PDClient)

	up.PDClock, err = pdtime.NewClock(ctx, up.PDClient)
	if err != nil {
		return errors.Trace(err)
	}

	up.wg.Add(1)
	go func() {
		defer up.wg.Done()
		up.PDClock.Run(ctx)
	}()
	up.wg.Add(1)
	go func() {
		defer up.wg.Done()
		up.GrpcPool.RecycleConn(ctx)
	}()

	up.GCManager = gc.NewManager(up.PDClient, up.PDClock)
	log.Info("upStream initialize successfully", zap.Uint64("clusterTD", up.clusterID))
	up.status = normal
	return nil
}

// close all resources
func (up *Upstream) close() {
	if up.PDClient != nil {
		up.PDClient.Close()
	}

	if up.KVStorage != nil {
		err := up.KVStorage.Close()
		if err != nil {
			log.Warn("kv store close failed", zap.Error(err))
		}
	}

	if up.GrpcPool != nil {
		up.GrpcPool.Close()
	}
	if up.RegionCache != nil {
		up.RegionCache.Close()
	}
	if up.PDClock != nil {
		up.PDClock.Stop()
	}

	up.wg.Wait()
	log.Info("upStream closed", zap.Uint64("cluster id", up.clusterID))
}

// IsNormal return true if this upstream is normal.
func (up *Upstream) IsNormal() bool {
	return up.status == normal
}
