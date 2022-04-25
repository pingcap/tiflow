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
	initializing
	normal
	closed

	// 30 mins
	icTreshold = 180
)

type UpStream struct {
	clusterID      uint64
	pdEndpoints    []string
	securityConfig *config.SecurityConfig

	PDClient    pd.Client
	KVStorage   tidbkv.Storage
	GrpcPool    kv.GrpcPool
	RegionCache *tikv.RegionCache
	PDClock     *pdtime.PDClock
	GCManager   gc.Manager

	wg *sync.WaitGroup
	// 用来计算有多少个 changefeed 持有该资源，当持有数为 0 时，owner 负责关闭该资源。
	hc int32
	// 如果 ic 超过 icTreshold，manager 会关闭这个 upStream
	ic int32
	// 0 or 1, 1 indicate this upStream is initlized
	status status
	errCh  chan error
}

func newUpStream(clusterID uint64, pdEndpoints []string, securityConfig *config.SecurityConfig) *UpStream {
	return &UpStream{
		clusterID:      clusterID,
		pdEndpoints:    pdEndpoints,
		securityConfig: securityConfig,
		wg:             new(sync.WaitGroup),
		status:         ready,
		errCh:          make(chan error, 1),
	}
}

// 调用者需要定期检查 error chan 和 up 是否初始化完毕
// 若出现 error 调用者需要调用 close 方法来释放可能已经初始化完毕的资源
func (up *UpStream) asyncInit(ctx context.Context) {
	if up.status != ready {
		return
	}
	// 以后需要改为异步初始化
	up.status = initializing
	go up.init(ctx)
}

func (up *UpStream) init(ctx context.Context) {
	log.Info("upStream is initializing", zap.Uint64("clusterID", up.clusterID))
	var err error

	defer func() {
		if err != nil {
			log.Error("upStream initializing error", zap.Uint64("clusterID", up.clusterID), zap.Error(err))
			err = errors.Trace(err)
			up.close()
		}
		up.errCh <- err
	}()

	grpcTLSOption, err := up.securityConfig.ToGRPCDialOption()
	if err != nil {
		return
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
		return
	}
	up.PDClient = pdClient

	// To not block CDC server startup, we need to warn instead of error
	// when TiKV is incompatible.
	errorTiKVIncompatible := false
	err = version.CheckClusterVersion(ctx, up.PDClient, up.pdEndpoints, up.securityConfig, errorTiKVIncompatible)
	if err != nil {
		return
	}

	kvStore, err := kv.CreateTiStore(strings.Join(up.pdEndpoints, ","), up.securityConfig)
	if err != nil {
		return
	}
	up.KVStorage = kvStore

	up.GrpcPool = kv.NewGrpcPoolImpl(ctx, up.securityConfig)
	up.RegionCache = tikv.NewRegionCache(up.PDClient)

	up.PDClock, err = pdtime.NewClock(ctx, up.PDClient)
	if err != nil {
		return
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

	log.Info("upStream gcManager created", zap.Uint64("clusterID", up.clusterID))

	up.status = normal
}

// close closes all resources
func (up *UpStream) close() {
	if up.status == closed {
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
	log.Info("upStream closed", zap.Uint64("cluster id", up.clusterID))
	close(up.errCh)
	up.status = closed
}

func (up *UpStream) IsReady() bool {
	return up.status == ready
}

func (up *UpStream) IsNormal() bool {
	return up.status == normal
}

func (up *UpStream) IsInitializing() bool {
	return up.status == initializing
}

func (up *UpStream) IsColse() bool {
	return up.status == closed
}

func (up *UpStream) CheckError() error {
	var err error
	select {
	case err = <-up.errCh:
	default:
	}
	return err
}

func (up *UpStream) hold() {
	atomic.AddInt32(&up.hc, 1)
}

func (up *UpStream) unhold() {
	atomic.AddInt32(&up.hc, -1)
}

func (up *UpStream) isHold() bool {
	return atomic.LoadInt32(&up.hc) == 0
}

func (up *UpStream) addIc() {
	atomic.AddInt32(&up.ic, 1)
}

func (up *UpStream) clearIc() {
	atomic.StoreInt32(&up.ic, 0)
}

func (up *UpStream) shouldClose() bool {
	return atomic.LoadInt32(&up.ic) >= icTreshold
}
