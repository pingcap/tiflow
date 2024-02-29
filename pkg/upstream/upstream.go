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
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/benbjohnson/clock"
	dmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/log"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tiflow/cdc/kv"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/errorutil"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/pingcap/tiflow/pkg/security"
	pmysql "github.com/pingcap/tiflow/pkg/sink/mysql"
	"github.com/pingcap/tiflow/pkg/txnutil/gc"
	"github.com/pingcap/tiflow/pkg/version"
	"github.com/prometheus/client_golang/prometheus"
	tikvconfig "github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	uatomic "github.com/uber-go/atomic"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
)

const (
	// indicate an upstream is created but not initialized.
	uninit int32 = iota
	// indicate an upstream is initialized and can work normally.
	normal
	// indicate an upstream is closing
	closing
	// indicate an upstream is closed.
	closed

	maxIdleDuration = time.Minute * 30
)

// Upstream holds resources of a TiDB cluster, it can be shared by many changefeeds
// and processors. All public fields and method of an upstream should be thread-safe.
// Please be careful that never change any exported field of an Upstream.
type Upstream struct {
	ID uint64

	PdEndpoints    []string
	SecurityConfig *security.Credential
	PDClient       pd.Client
	etcdCli        *etcd.Client
	session        *concurrency.Session

	KVStorage   tidbkv.Storage
	GrpcPool    kv.GrpcPool
	RegionCache *tikv.RegionCache
	PDClock     pdutil.Clock
	GCManager   gc.Manager
	// Only use in Close().
	cancel func()
	mu     sync.Mutex
	// record the time when Upstream.hc becomes zero.
	idleTime time.Time
	// use clock to facilitate unit test
	clock  clock.Clock
	wg     *sync.WaitGroup
	status int32

	err               uatomic.Error
	isDefaultUpstream bool
}

func newUpstream(pdEndpoints []string,
	securityConfig *security.Credential,
) *Upstream {
	return &Upstream{
		PdEndpoints:    pdEndpoints,
		SecurityConfig: securityConfig,
		status:         uninit,
		wg:             new(sync.WaitGroup),
		clock:          clock.New(),
	}
}

// NewUpstream4Test new an upstream for unit test.
func NewUpstream4Test(pdClient pd.Client) *Upstream {
	pdClock := pdutil.NewClock4Test()
	gcManager := gc.NewManager(
		etcd.GcServiceIDForTest(),
		pdClient, pdClock)
	res := &Upstream{
		ID:             testUpstreamID,
		PDClient:       pdClient,
		PDClock:        pdClock,
		GCManager:      gcManager,
		status:         normal,
		wg:             new(sync.WaitGroup),
		clock:          clock.New(),
		SecurityConfig: &security.Credential{},
		cancel:         func() {},
	}

	return res
}

// init initializes the upstream
func initUpstream(ctx context.Context, up *Upstream, cfg CaptureTopologyCfg) error {
	ctx, up.cancel = context.WithCancel(ctx)
	grpcTLSOption, err := up.SecurityConfig.ToGRPCDialOption()
	if err != nil {
		up.err.Store(err)
		return errors.Trace(err)
	}
	// init the tikv client tls global config
	initGlobalConfig(up.SecurityConfig)
	// default upstream always use the pdClient pass from cdc server
	if !up.isDefaultUpstream {
		up.PDClient, err = pd.NewClientWithContext(
			ctx, up.PdEndpoints, up.SecurityConfig.PDSecurityOption(),
			// the default `timeout` is 3s, maybe too small if the pd is busy,
			// set to 10s to avoid frequent timeout.
			pd.WithCustomTimeoutOption(10*time.Second),
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
			up.err.Store(err)
			return errors.Trace(err)
		}

		etcdCli, err := etcd.CreateRawEtcdClient(up.SecurityConfig, grpcTLSOption, up.PdEndpoints...)
		if err != nil {
			return errors.Trace(err)
		}
		up.etcdCli = etcd.Wrap(etcdCli, make(map[string]prometheus.Counter))
	}
	clusterID := up.PDClient.GetClusterID(ctx)
	if up.ID != 0 && up.ID != clusterID {
		err := fmt.Errorf("upstream id missmatch expected %d, actual: %d",
			up.ID, clusterID)
		up.err.Store(err)
		return errors.Trace(err)
	}
	up.ID = clusterID

	// To not block CDC server startup, we need to warn instead of error
	// when TiKV is incompatible.
	errorTiKVIncompatible := false
	err = version.CheckClusterVersion(ctx, up.PDClient,
		up.PdEndpoints, up.SecurityConfig, errorTiKVIncompatible)
	if err != nil {
		up.err.Store(err)
		log.Error("init upstream error", zap.Error(err))
		return errors.Trace(err)
	}

	up.KVStorage, err = kv.CreateTiStore(strings.Join(up.PdEndpoints, ","), up.SecurityConfig)
	if err != nil {
		up.err.Store(err)
		return errors.Trace(err)
	}

	up.GrpcPool = kv.NewGrpcPoolImpl(ctx, up.SecurityConfig)

	up.RegionCache = tikv.NewRegionCache(up.PDClient)

	up.PDClock, err = pdutil.NewClock(ctx, up.PDClient)
	if err != nil {
		up.err.Store(err)
		return errors.Trace(err)
	}

	up.GCManager = gc.NewManager(cfg.GCServiceID, up.PDClient, up.PDClock)

	// Update meta-region label to ensure that meta region isolated from data regions.
	pc, err := pdutil.NewPDAPIClient(up.PDClient, up.SecurityConfig)
	if err != nil {
		log.Error("create pd api client failed", zap.Error(err))
		return errors.Trace(err)
	}
	defer pc.Close()

	err = pc.UpdateMetaLabel(ctx)
	if err != nil {
		log.Warn("Fail to verify region label rule",
			zap.Error(err),
			zap.Uint64("upstreamID", up.ID),
			zap.Strings("upstreamEndpoints", up.PdEndpoints))
	}
	err = up.registerTopologyInfo(ctx, cfg)
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

	log.Info("upstream initialize successfully", zap.Uint64("upstreamID", up.ID))
	atomic.StoreInt32(&up.status, normal)
	return nil
}

// initGlobalConfig initializes the global config for tikv client tls.
// region cache health check will use the global config.
// TODO: remove this function after tikv client tls is refactored.
func initGlobalConfig(secCfg *security.Credential) {
	if secCfg.CAPath != "" || secCfg.CertPath != "" || secCfg.KeyPath != "" {
		conf := tikvconfig.GetGlobalConfig()
		conf.Security.ClusterSSLCA = secCfg.CAPath
		conf.Security.ClusterSSLCert = secCfg.CertPath
		conf.Security.ClusterSSLKey = secCfg.KeyPath
		conf.Security.ClusterVerifyCN = secCfg.CertAllowedCN
		tikvconfig.StoreGlobalConfig(conf)
	}
}

// Close all resources.
func (up *Upstream) Close() {
	up.mu.Lock()
	defer up.mu.Unlock()
	up.cancel()
	if atomic.LoadInt32(&up.status) == closed ||
		atomic.LoadInt32(&up.status) == closing {
		return
	}
	atomic.StoreInt32(&up.status, closing)

	// should never close default upstream's pdClient and etcdClient here
	// because it's shared in the cdc server
	if !up.isDefaultUpstream {
		if up.PDClient != nil {
			up.PDClient.Close()
		}
		if up.etcdCli != nil {
			err := up.etcdCli.Unwrap().Close()
			if err != nil {
				log.Warn("etcd client close failed", zap.Error(err))
			}
		}
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
	if up.session != nil {
		err := up.session.Close()
		if err != nil {
			log.Warn("etcd session close failed", zap.Error(err))
		}
	}

	up.wg.Wait()
	atomic.StoreInt32(&up.status, closed)
	log.Info("upstream closed", zap.Uint64("upstreamID", up.ID))
}

// Error returns the error during init this stream
func (up *Upstream) Error() error {
	return up.err.Load()
}

// IsNormal returns true if the upstream is normal.
func (up *Upstream) IsNormal() bool {
	return atomic.LoadInt32(&up.status) == normal && up.err.Load() == nil
}

// IsClosed returns true if the upstream is closed.
func (up *Upstream) IsClosed() bool {
	return atomic.LoadInt32(&up.status) == closed
}

// resetIdleTime set the upstream idle time to true
func (up *Upstream) resetIdleTime() {
	up.mu.Lock()
	defer up.mu.Unlock()

	if !up.idleTime.IsZero() {
		log.Info("upstream idle time is set to 0",
			zap.Uint64("id", up.ID))
		up.idleTime = time.Time{}
	}
}

// trySetIdleTime set the upstream idle time if it's not zero
func (up *Upstream) trySetIdleTime() {
	up.mu.Lock()
	defer up.mu.Unlock()
	// reset idleTime
	if up.idleTime.IsZero() {
		log.Info("upstream idle time is set to current time",
			zap.Uint64("id", up.ID))
		up.idleTime = up.clock.Now()
	}
}

func (up *Upstream) registerTopologyInfo(ctx context.Context, cfg CaptureTopologyCfg) error {
	lease, err := up.etcdCli.Grant(ctx, cfg.SessionTTL)
	if err != nil {
		return errors.Trace(err)
	}
	up.session, err = concurrency.NewSession(up.etcdCli.Unwrap(), concurrency.WithLease(lease.ID))
	if err != nil {
		return errors.Trace(err)
	}
	// register capture info to upstream pd
	key := fmt.Sprintf(topologyTiCDC, cfg.GCServiceID, cfg.AdvertiseAddr)
	value, err := cfg.CaptureInfo.Marshal()
	if err != nil {
		return errors.Trace(err)
	}
	_, err = up.etcdCli.Put(ctx, key, string(value), clientv3.WithLease(up.session.Lease()))
	return errors.WrapError(errors.ErrPDEtcdAPIError, err)
}

// shouldClose returns true if
// this upstream idleTime reaches maxIdleDuration.
func (up *Upstream) shouldClose() bool {
	// default upstream should never be closed.
	if up.isDefaultUpstream {
		return false
	}

	if !up.idleTime.IsZero() &&
		up.clock.Since(up.idleTime) >= maxIdleDuration {
		return true
	}

	return false
}

// VerifyTiDBUser verify whether the username and password are valid in TiDB. It does the validation via
// the successfully build of a connection with upstream TiDB with the username and password.
func (up *Upstream) VerifyTiDBUser(ctx context.Context, username, password string) error {
	tidbs, err := fetchTiDBTopology(ctx, up.etcdCli.Unwrap())
	if err != nil {
		return errors.Trace(err)
	}
	if len(tidbs) == 0 {
		return errors.New("tidb instance not found in topology, please check if the tidb is running")
	}

	for _, tidb := range tidbs {
		// connect tidb
		host := fmt.Sprintf("%s:%d", tidb.IP, tidb.Port)
		dsnStr := fmt.Sprintf("%s:%s@tcp(%s)/", username, password, host)
		err = up.doVerify(ctx, dsnStr)
		if err == nil {
			return nil
		}
		if errorutil.IsAccessDeniedError(err) {
			// For access denied error, we can return immediately.
			// For other errors, we need to continue to verify the next tidb instance.
			return errors.Trace(err)
		}
	}
	return errors.Trace(err)
}

func (up *Upstream) doVerify(ctx context.Context, dsnStr string) error {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()

	dsn, err := dmysql.ParseDSN(dsnStr)
	if err != nil {
		return errors.Trace(err)
	}
	// Note: we use "preferred" here to make sure the connection is encrypted if possible. It is the same as the default
	// behavior of mysql client, refer to: https://dev.mysql.com/doc/refman/8.0/en/using-encrypted-connections.html.
	dsn.TLSConfig = "preferred"

	db, err := pmysql.GetTestDB(ctx, dsn, pmysql.CreateMySQLDBConn)
	if err != nil {
		return errors.Trace(err)
	}
	defer db.Close()

	rows, err := db.Query("SHOW STATUS LIKE '%Ssl_cipher'")
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			log.Warn("query Ssl_cipher close rows failed", zap.Error(err))
		}
		if rows.Err() != nil {
			log.Warn("query Ssl_cipher rows has error", zap.Error(rows.Err()))
		}
	}()

	var name, value string
	err = rows.Scan(&name, &value)
	if err != nil {
		log.Warn("failed to get ssl cipher", zap.Error(err),
			zap.String("username", dsn.User), zap.Uint64("upstreamID", up.ID))
	}
	log.Info("verify tidb user successfully", zap.String("username", dsn.User),
		zap.String("sslCipherName", name), zap.String("sslCipherValue", value),
		zap.Uint64("upstreamID", up.ID))
	return nil
}
