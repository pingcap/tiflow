// Copyright 2020 PingCAP, Inc.
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

package etcd

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/errorutil"
	"github.com/pingcap/tiflow/pkg/retry"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/pd/pkg/errs"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	v3rpc "go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"go.etcd.io/etcd/client/pkg/v3/logutil"
	clientV3 "go.etcd.io/etcd/client/v3"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
)

// etcd operation names
const (
	EtcdPut    = "Put"
	EtcdGet    = "Get"
	EtcdTxn    = "Txn"
	EtcdDel    = "Del"
	EtcdGrant  = "Grant"
	EtcdRevoke = "Revoke"
)

const (
	backoffBaseDelayInMs = 500
	// in previous/backoff retry pkg, the DefaultMaxInterval = 60 * time.Second
	backoffMaxDelayInMs = 60 * 1000
	// If no msg comes from an etcd watchCh for etcdWatchChTimeoutDuration long,
	// we should cancel the watchCh and request a new watchCh from etcd client
	etcdWatchChTimeoutDuration = 10 * time.Second
	// If no msg comes from an etcd watchCh for etcdRequestProgressDuration long,
	// we should call RequestProgress of etcd client
	etcdRequestProgressDuration = 1 * time.Second
	// etcdWatchChBufferSize is arbitrarily specified, it will be modified in the future
	etcdWatchChBufferSize = 16
	// etcdClientTimeoutDuration represents the timeout duration for
	// etcd client to execute a remote call
	etcdClientTimeoutDuration = 30 * time.Second
)

var (
	txnEmptyCmps    = []clientV3.Cmp{}
	txnEmptyOpsThen = []clientV3.Op{}
	// TxnEmptyOpsElse is a no-op operation.
	TxnEmptyOpsElse = []clientV3.Op{}
)

// set to var instead of const for mocking the value to speedup test
var maxTries uint64 = 12

// Client is a simple wrapper that adds retry to etcd RPC
type Client struct {
	cli     *clientV3.Client
	metrics map[string]prometheus.Counter
	// clock is for making it easier to mock time-related data structures in unit tests
	clock clock.Clock
}

// Wrap warps a clientV3.Client that provides etcd APIs required by TiCDC.
func Wrap(cli *clientV3.Client, metrics map[string]prometheus.Counter) *Client {
	return &Client{cli: cli, metrics: metrics, clock: clock.New()}
}

// Unwrap returns a clientV3.Client
func (c *Client) Unwrap() *clientV3.Client {
	return c.cli
}

func retryRPC(rpcName string, metric prometheus.Counter, etcdRPC func() error) error {
	// By default, PD etcd sets [3s, 6s) for election timeout.
	// Some rpc could fail due to etcd errors, like "proposal dropped".
	// Retry at least two election timeout to handle the case that two PDs restarted
	// (the first election maybe failed).
	// 16s = \sum_{n=0}^{6} 0.5*1.5^n
	return retry.Do(context.Background(), func() error {
		err := etcdRPC()
		if err != nil && errors.Cause(err) != context.Canceled {
			log.Warn("etcd RPC failed", zap.String("RPC", rpcName), zap.Error(err))
		}
		if metric != nil {
			metric.Inc()
		}
		return err
	}, retry.WithBackoffBaseDelay(backoffBaseDelayInMs),
		retry.WithBackoffMaxDelay(backoffMaxDelayInMs),
		retry.WithMaxTries(maxTries),
		retry.WithIsRetryableErr(isRetryableError(rpcName)))
}

// Put delegates request to clientV3.KV.Put
func (c *Client) Put(
	ctx context.Context, key, val string, opts ...clientV3.OpOption,
) (resp *clientV3.PutResponse, err error) {
	putCtx, cancel := context.WithTimeout(ctx, etcdClientTimeoutDuration)
	defer cancel()
	err = retryRPC(EtcdPut, c.metrics[EtcdPut], func() error {
		var inErr error
		resp, inErr = c.cli.Put(putCtx, key, val, opts...)
		return inErr
	})
	return
}

// Get delegates request to clientV3.KV.Get
func (c *Client) Get(
	ctx context.Context, key string, opts ...clientV3.OpOption,
) (resp *clientV3.GetResponse, err error) {
	getCtx, cancel := context.WithTimeout(ctx, etcdClientTimeoutDuration)
	defer cancel()
	err = retryRPC(EtcdGet, c.metrics[EtcdGet], func() error {
		var inErr error
		resp, inErr = c.cli.Get(getCtx, key, opts...)
		return inErr
	})
	return
}

// Delete delegates request to clientV3.KV.Delete
func (c *Client) Delete(
	ctx context.Context, key string, opts ...clientV3.OpOption,
) (resp *clientV3.DeleteResponse, err error) {
	if metric, ok := c.metrics[EtcdDel]; ok {
		metric.Inc()
	}
	delCtx, cancel := context.WithTimeout(ctx, etcdClientTimeoutDuration)
	defer cancel()
	// We don't retry on delete operation. It's dangerous.
	return c.cli.Delete(delCtx, key, opts...)
}

// Txn delegates request to clientV3.KV.Txn. The error returned can only be a non-retryable error,
// such as context.Canceled, context.DeadlineExceeded, errors.ErrReachMaxTry.
func (c *Client) Txn(
	ctx context.Context, cmps []clientV3.Cmp, opsThen, opsElse []clientV3.Op,
) (resp *clientV3.TxnResponse, err error) {
	txnCtx, cancel := context.WithTimeout(ctx, etcdClientTimeoutDuration)
	defer cancel()
	err = retryRPC(EtcdTxn, c.metrics[EtcdTxn], func() error {
		var inErr error
		resp, inErr = c.cli.Txn(txnCtx).If(cmps...).Then(opsThen...).Else(opsElse...).Commit()
		return inErr
	})
	return
}

// Grant delegates request to clientV3.Lease.Grant
func (c *Client) Grant(
	ctx context.Context, ttl int64,
) (resp *clientV3.LeaseGrantResponse, err error) {
	grantCtx, cancel := context.WithTimeout(ctx, etcdClientTimeoutDuration)
	defer cancel()
	err = retryRPC(EtcdGrant, c.metrics[EtcdGrant], func() error {
		var inErr error
		resp, inErr = c.cli.Grant(grantCtx, ttl)
		return inErr
	})
	return
}

// Revoke delegates request to clientV3.Lease.Revoke
func (c *Client) Revoke(
	ctx context.Context, id clientV3.LeaseID,
) (resp *clientV3.LeaseRevokeResponse, err error) {
	revokeCtx, cancel := context.WithTimeout(ctx, etcdClientTimeoutDuration)
	defer cancel()
	err = retryRPC(EtcdRevoke, c.metrics[EtcdRevoke], func() error {
		var inErr error
		resp, inErr = c.cli.Revoke(revokeCtx, id)
		return inErr
	})
	return
}

// TimeToLive delegates request to clientV3.Lease.TimeToLive
func (c *Client) TimeToLive(
	ctx context.Context, lease clientV3.LeaseID, opts ...clientV3.LeaseOption,
) (resp *clientV3.LeaseTimeToLiveResponse, err error) {
	timeToLiveCtx, cancel := context.WithTimeout(ctx, etcdClientTimeoutDuration)
	defer cancel()
	err = retryRPC(EtcdRevoke, c.metrics[EtcdRevoke], func() error {
		var inErr error
		resp, inErr = c.cli.TimeToLive(timeToLiveCtx, lease, opts...)
		return inErr
	})
	return
}

// Watch delegates request to clientV3.Watcher.Watch
func (c *Client) Watch(
	ctx context.Context, key string, role string, opts ...clientV3.OpOption,
) clientV3.WatchChan {
	watchCh := make(chan clientV3.WatchResponse, etcdWatchChBufferSize)
	go c.WatchWithChan(ctx, watchCh, key, role, opts...)
	return watchCh
}

// WatchWithChan maintains a watchCh and sends all msg from the watchCh to outCh
func (c *Client) WatchWithChan(
	ctx context.Context, outCh chan<- clientV3.WatchResponse,
	key string, role string, opts ...clientV3.OpOption,
) {
	// get initial revision from opts to avoid revision fall back
	lastRevision := getRevisionFromWatchOpts(opts...)
	watchCtx, cancel := context.WithCancel(ctx)
	watchCh := c.cli.Watch(watchCtx, key, opts...)

	ticker := c.clock.Ticker(etcdRequestProgressDuration)
	lastReceivedResponseTime := c.clock.Now()

	defer func() {
		// Using closures to handle changes to the cancel function
		ticker.Stop()
		cancel()
		close(outCh)

		log.Info("WatchWithChan exited", zap.String("role", role))
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case response := <-watchCh:
			lastReceivedResponseTime = c.clock.Now()
			if response.Err() == nil && !response.IsProgressNotify() {
				lastRevision = response.Header.Revision
			}

		Loop:
			// we must loop here until the response is sent to outCh
			// or otherwise the response will be lost
			for {
				select {
				case <-ctx.Done():
					return
				case outCh <- response: // it may block here
					break Loop
				case <-ticker.C:
					if c.clock.Since(lastReceivedResponseTime) >= etcdWatchChTimeoutDuration {
						log.Warn("etcd client outCh blocking too long, the etcdWorker may be stuck",
							zap.Duration("duration", c.clock.Since(lastReceivedResponseTime)),
							zap.String("role", role))
					}
				}
			}

			ticker.Reset(etcdRequestProgressDuration)
		case <-ticker.C:
			if err := c.RequestProgress(ctx); err != nil {
				log.Warn("failed to request progress for etcd watcher", zap.Error(err))
			}
			if c.clock.Since(lastReceivedResponseTime) >= etcdWatchChTimeoutDuration {
				// cancel the last cancel func to reset it
				log.Warn("etcd client watchCh blocking too long, reset the watchCh",
					zap.Duration("duration", c.clock.Since(lastReceivedResponseTime)),
					zap.Stack("stack"),
					zap.String("role", role))
				cancel()
				watchCtx, cancel = context.WithCancel(ctx)
				// to avoid possible context leak warning from govet
				_ = cancel
				watchCh = c.cli.Watch(watchCtx, key,
					clientV3.WithPrefix(), clientV3.WithRev(lastRevision))
				// we need to reset lastReceivedResponseTime after reset Watch
				lastReceivedResponseTime = c.clock.Now()
			}
		}
	}
}

// RequestProgress requests a progress notify response be sent in all watch channels.
func (c *Client) RequestProgress(ctx context.Context) error {
	return c.cli.RequestProgress(ctx)
}

func isRetryableError(rpcName string) retry.IsRetryable {
	return func(err error) bool {
		if !cerror.IsRetryableError(err) {
			return false
		}

		switch rpcName {
		case EtcdRevoke:
			if etcdErr, ok := err.(v3rpc.EtcdError); ok && etcdErr.Code() == codes.NotFound {
				// It means the etcd lease is already expired or revoked
				return false
			}
		case EtcdTxn:
			return errorutil.IsRetryableEtcdError(err)
		default:
			// For other types of operation, we retry directly without handling errors
		}

		return true
	}
}

// The following code is mainly copied from:
// https://github.com/tikv/pd/blob/master/pkg/utils/etcdutil/etcdutil.go
const (
	// defaultEtcdClientTimeout is the default timeout for etcd client.
	defaultEtcdClientTimeout = 5 * time.Second
	// defaultDialKeepAliveTime is the time after which client pings the server to see if transport is alive.
	defaultDialKeepAliveTime = 10 * time.Second
	// defaultDialKeepAliveTimeout is the time that the client waits for a response for the
	// keep-alive probe. If the response is not received in this time, the connection is closed.
	defaultDialKeepAliveTimeout = 3 * time.Second
	// etcdServerOfflineTimeout is the timeout for an unhealthy etcd endpoint to be offline from healthy checker.
	etcdServerOfflineTimeout = 30 * time.Minute
	// etcdServerDisconnectedTimeout is the timeout for an unhealthy etcd endpoint to be disconnected from healthy checker.
	etcdServerDisconnectedTimeout = 1 * time.Minute
	// healthyPath is the path to check etcd health.
	healthyPath = "health"
)

func newClient(tlsConfig *tls.Config, grpcDialOption grpc.DialOption, endpoints ...string) (*clientv3.Client, error) {
	if len(endpoints) == 0 {
		return nil, errors.New("empty endpoints")
	}
	logConfig := logutil.DefaultZapLoggerConfig
	logConfig.Level = zap.NewAtomicLevelAt(zapcore.ErrorLevel)

	lgc := zap.NewProductionConfig()
	lgc.Encoding = log.ZapEncodingName
	client, err := clientv3.New(clientv3.Config{
		Endpoints:            endpoints,
		TLS:                  tlsConfig,
		LogConfig:            &logConfig,
		DialTimeout:          defaultEtcdClientTimeout,
		DialKeepAliveTime:    defaultDialKeepAliveTime,
		DialKeepAliveTimeout: defaultDialKeepAliveTimeout,
		DialOptions: []grpc.DialOption{
			grpcDialOption,
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
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:    10 * time.Second,
				Timeout: 20 * time.Second,
			}),
		},
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	return client, nil
}

// CreateRawEtcdClient creates etcd v3 client with detecting endpoints.
// It will check the health of endpoints periodically, and update endpoints if needed.
func CreateRawEtcdClient(securityConf *security.Credential, grpcDialOption grpc.DialOption, endpoints ...string) (*clientv3.Client, error) {
	log.Info("create etcdCli", zap.Strings("endpoints", endpoints))

	tlsConfig, err := securityConf.ToTLSConfig()
	if err != nil {
		return nil, err
	}

	client, err := newClient(tlsConfig, grpcDialOption, endpoints...)
	if err != nil {
		return nil, err
	}

	tickerInterval := defaultDialKeepAliveTime

	checker := &healthyChecker{
		tlsConfig:      tlsConfig,
		grpcDialOption: grpcDialOption,
	}
	eps := syncUrls(client)
	checker.update(eps)

	// Create a goroutine to check the health of etcd endpoints periodically.
	go func(client *clientv3.Client) {
		ticker := time.NewTicker(tickerInterval)
		defer ticker.Stop()
		lastAvailable := time.Now()
		for {
			select {
			case <-client.Ctx().Done():
				log.Info("etcd client is closed, exit health check goroutine")
				checker.Range(func(key, value interface{}) bool {
					client := value.(*healthyClient)
					client.Close()
					return true
				})
				return
			case <-ticker.C:
				usedEps := client.Endpoints()
				healthyEps := checker.patrol(client.Ctx())
				if len(healthyEps) == 0 {
					// when all endpoints are unhealthy, try to reset endpoints to update connect
					// rather than delete them to avoid there is no any endpoint in client.
					// Note: reset endpoints will trigger subconn closed, and then trigger reconnect.
					// otherwise, the subconn will be retrying in grpc layer and use exponential backoff,
					// and it cannot recover as soon as possible.
					if time.Since(lastAvailable) > etcdServerDisconnectedTimeout {
						log.Info("no available endpoint, try to reset endpoints", zap.Strings("lastEndpoints", usedEps))
						client.SetEndpoints([]string{}...)
						client.SetEndpoints(usedEps...)
					}
				} else {
					if !util.AreStringSlicesEquivalent(healthyEps, usedEps) {
						client.SetEndpoints(healthyEps...)
						change := fmt.Sprintf("%d->%d", len(usedEps), len(healthyEps))
						etcdStateGauge.WithLabelValues("endpoints").Set(float64(len(healthyEps)))
						log.Info("update endpoints", zap.String("numChange", change),
							zap.Strings("lastEndpoints", usedEps), zap.Strings("endpoints", client.Endpoints()))
					}
					lastAvailable = time.Now()
				}
			}
		}
	}(client)

	// Notes: use another goroutine to update endpoints to avoid blocking health check in the first goroutine.
	go func(client *clientv3.Client) {
		ticker := time.NewTicker(tickerInterval)
		defer ticker.Stop()
		for {
			select {
			case <-client.Ctx().Done():
				log.Info("etcd client is closed, exit update endpoint goroutine")
				return
			case <-ticker.C:
				eps := syncUrls(client)
				checker.update(eps)
			}
		}
	}(client)

	return client, nil
}

type healthyClient struct {
	*clientv3.Client
	lastHealth time.Time
}

type healthyChecker struct {
	sync.Map       // map[string]*healthyClient
	tlsConfig      *tls.Config
	grpcDialOption grpc.DialOption
}

func (checker *healthyChecker) patrol(ctx context.Context) []string {
	// See https://github.com/etcd-io/etcd/blob/85b640cee793e25f3837c47200089d14a8392dc7/etcdctl/ctlv3/command/ep_command.go#L105-L145
	var wg sync.WaitGroup
	count := 0
	checker.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	hch := make(chan string, count)
	healthyList := make([]string, 0, count)
	checker.Range(func(key, value interface{}) bool {
		wg.Add(1)
		go func(key, value interface{}) {
			defer wg.Done()
			ep := key.(string)
			client := value.(*healthyClient)
			if IsHealthy(ctx, client.Client) {
				hch <- ep
				checker.Store(ep, &healthyClient{
					Client:     client.Client,
					lastHealth: time.Now(),
				})
				return
			}
		}(key, value)
		return true
	})
	wg.Wait()
	close(hch)
	for h := range hch {
		healthyList = append(healthyList, h)
	}
	return healthyList
}

func (checker *healthyChecker) update(eps []string) {
	for _, ep := range eps {
		// check if client exists, if not, create one, if exists, check if it's offline or disconnected.
		if client, ok := checker.Load(ep); ok {
			lastHealthy := client.(*healthyClient).lastHealth
			if time.Since(lastHealthy) > etcdServerOfflineTimeout {
				log.Info("some etcd server maybe offline", zap.String("endpoint", ep))
				checker.Delete(ep)
			}
			if time.Since(lastHealthy) > etcdServerDisconnectedTimeout {
				// try to reset client endpoint to trigger reconnect
				client.(*healthyClient).Client.SetEndpoints([]string{}...)
				client.(*healthyClient).Client.SetEndpoints(ep)
			}
			continue
		}
		checker.addClient(ep, time.Now())
	}
}

func (checker *healthyChecker) addClient(ep string, lastHealth time.Time) {
	client, err := newClient(checker.tlsConfig, checker.grpcDialOption, ep)
	if err != nil {
		log.Error("failed to create etcd healthy client", zap.Error(err))
		return
	}
	checker.Store(ep, &healthyClient{
		Client:     client,
		lastHealth: lastHealth,
	})
}

func syncUrls(client *clientv3.Client) []string {
	// See https://github.com/etcd-io/etcd/blob/85b640cee793e25f3837c47200089d14a8392dc7/clientv3/client.go#L170-L183
	ctx, cancel := context.WithTimeout(clientv3.WithRequireLeader(client.Ctx()),
		etcdClientTimeoutDuration)
	defer cancel()
	mresp, err := client.MemberList(ctx)
	if err != nil {
		log.Error("failed to list members", errs.ZapError(err))
		return []string{}
	}
	var eps []string
	for _, m := range mresp.Members {
		if len(m.Name) != 0 && !m.IsLearner {
			eps = append(eps, m.ClientURLs...)
		}
	}
	return eps
}

// IsHealthy checks if the etcd is healthy.
func IsHealthy(ctx context.Context, client *clientv3.Client) bool {
	timeout := etcdClientTimeoutDuration
	ctx, cancel := context.WithTimeout(clientv3.WithRequireLeader(ctx), timeout)
	defer cancel()
	_, err := client.Get(ctx, healthyPath)
	// permission denied is OK since proposal goes through consensus to get it
	// See: https://github.com/etcd-io/etcd/blob/85b640cee793e25f3837c47200089d14a8392dc7/etcdctl/ctlv3/command/ep_command.go#L124
	return err == nil || err == rpctypes.ErrPermissionDenied
}
