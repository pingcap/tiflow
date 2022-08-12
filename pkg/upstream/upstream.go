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
	"sync/atomic"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tiflow/cdc/kv"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/pingcap/tiflow/pkg/txnutil/gc"
	"github.com/pingcap/tiflow/pkg/version"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
)

const (
	// indicate a upstream is crerated but not initialized.
	uninit int32 = iota
	// indicate a upstream is initialized and can works normally.
	normal
	// indicate a upstream is closed.
	closed

	maxIdleDuration = time.Minute * 30
)

// Upstream holds resources of a TiDB cluster, it can be shared by many changefeeds
// and processors. All public fileds and method of a upstream should be thread-safe.
// Please be careful that never change any exported field of a Upstream.
type Upstream struct {
	ID             uint64
	pdEndpoints    []string
	securityConfig *config.SecurityConfig

	PDClient    pd.Client
	KVStorage   tidbkv.Storage
	GrpcPool    kv.GrpcPool
	RegionCache *tikv.RegionCache
	PDClock     pdutil.Clock
	GCManager   gc.Manager

	hcMu struct {
		// mu should be lock when hc or idleTime need to be changed.
		mu sync.Mutex
		// holder count of this upstream
		hc int32
		// record the time when Upstream.hc becomes zero.
		idleTime time.Time
	}
	// use clock to facilitate unit test
	clock  clock.Clock
	wg     *sync.WaitGroup
	status int32
}

func newUpstream(upstreamID uint64, pdEndpoints []string, securityConfig *config.SecurityConfig) *Upstream {
	return &Upstream{
		ID: upstreamID, pdEndpoints: pdEndpoints, status: uninit,
		securityConfig: securityConfig, wg: new(sync.WaitGroup), clock: clock.New(),
	}
}

// NewUpstream4Test new a upstream for unit test.
func NewUpstream4Test(pdClient pd.Client) *Upstream {
	pdClock := pdutil.NewClock4Test()
	gcManager := gc.NewManager(pdClient, pdClock)
	res := &Upstream{
		ID:       DefaultUpstreamID,
		PDClient: pdClient, PDClock: pdClock, GCManager: gcManager,
		status: normal, wg: new(sync.WaitGroup), clock: clock.New(),
		hcMu: struct {
			mu       sync.Mutex
			hc       int32
			idleTime time.Time
		}{hc: 1},
	}

	return res
}

func (up *Upstream) init(ctx context.Context) error {
	log.Info("upstream is initializing", zap.Uint64("upstreamID", up.ID))
	var err error

	grpcTLSOption, err := up.securityConfig.ToGRPCDialOption()
	if err != nil {
		return errors.Trace(err)
	}

	up.PDClient, err = pd.NewClientWithContext(
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
	log.Info("upstream's PDClient created", zap.Uint64("upstreamID", up.ID))

	// To not block CDC server startup, we need to warn instead of error
	// when TiKV is incompatible.
	errorTiKVIncompatible := false
	err = version.CheckClusterVersion(ctx, up.PDClient, up.pdEndpoints, up.securityConfig, errorTiKVIncompatible)
	if err != nil {
		log.Error("init upstream error", zap.Error(err))
		return errors.Trace(err)
	}

	up.KVStorage, err = kv.CreateTiStore(strings.Join(up.pdEndpoints, ","), up.securityConfig)
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("upstream's KVStorage created", zap.Uint64("upstreamID", up.ID))

	up.GrpcPool = kv.NewGrpcPoolImpl(ctx, up.securityConfig)
	log.Info("upstream's GrpcPool created", zap.Uint64("upstreamID", up.ID))

	up.RegionCache = tikv.NewRegionCache(up.PDClient)
	log.Info("upstream's RegionCache created", zap.Uint64("upstreamID", up.ID))

	up.PDClock, err = pdutil.NewClock(ctx, up.PDClient)
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("upstream's PDClock created", zap.Uint64("upstreamID", up.ID))

	up.GCManager = gc.NewManager(up.PDClient, up.PDClock)
	log.Info("upstream's GCManager created", zap.Uint64("upstreamID", up.ID))

	// Update meta-region label to ensure that meta region isolated from data regions.
	err = pdutil.UpdateMetaLabel(ctx, up.PDClient)
	if err != nil {
		log.Warn("Fail to verify region label rule",
			zap.Error(err),
			zap.Uint64("upstreamID", up.ID),
			zap.Strings("upstramEndpoints", up.pdEndpoints))
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

	log.Info("upStream initialize successfully", zap.Uint64("upstreamID", up.ID))
	atomic.StoreInt32(&up.status, normal)
	return nil
}

// close all resources.
func (up *Upstream) close() {
	if atomic.LoadInt32(&up.status) == closed {
		return
	}

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
	atomic.StoreInt32(&up.status, closed)
	log.Info("upStream closed", zap.Uint64("upstreamID", up.ID))
}

// IsNormal returns true if the upstream is normal.
func (up *Upstream) IsNormal() bool {
	return atomic.LoadInt32(&up.status) == normal
}

// IsClosed returns true if the upstream is closed.
func (up *Upstream) IsClosed() bool {
	return atomic.LoadInt32(&up.status) == closed
}

func (up *Upstream) unhold() {
	up.hcMu.mu.Lock()
	defer up.hcMu.mu.Unlock()
	up.hcMu.hc--

	if up.hcMu.hc < 0 {
		log.Panic("upstream's hc should never less than 0", zap.Uint64("upstreamID", up.ID))
	}
	if up.hcMu.hc == 0 {
		up.hcMu.idleTime = up.clock.Now()
	}
}

func (up *Upstream) hold() {
	up.hcMu.mu.Lock()
	defer up.hcMu.mu.Unlock()
	if up.hcMu.hc < 0 {
		log.Panic("upstream's hc should never less than 0", zap.Uint64("upstreamID", up.ID))
	}
	up.hcMu.hc++
	// reset idleTime
	if !up.hcMu.idleTime.IsZero() {
		up.hcMu.idleTime = time.Time{}
	}
}

// Release release upstream from a holder
func (up *Upstream) Release() {
	up.unhold()
}

// return true if this upstream idleTime reachs maxIdleDuration.
func (up *Upstream) shouldClose() bool {
	// default upstream should never be closed.
	if up.ID == DefaultUpstreamID {
		return false
	}

	up.hcMu.mu.Lock()
	defer up.hcMu.mu.Unlock()
	if up.hcMu.hc == 0 && !up.hcMu.idleTime.IsZero() &&
		up.clock.Since(up.hcMu.idleTime) >= maxIdleDuration {
		return true
	}

	return false
}
