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
	"reflect"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/errorutil"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/pingcap/tiflow/pkg/retry"
	"github.com/prometheus/client_golang/prometheus"
	pd "github.com/tikv/pd/client"
	v3rpc "go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"go.etcd.io/etcd/client/pkg/v3/logutil"
	clientV3 "go.etcd.io/etcd/client/v3"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
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
var maxTries uint64 = 8

// Client is a simple wrapper that adds retry to etcd RPC
type Client struct {
	endpoints []string
	cli       *clientV3.Client
	// pdCli works well in such situation:
	// 1. pd leader is network isolated with other pd followers
	// 2. pd leader is io hang
	// 3. pd leader is send kill -19 signal and not recover
	// 4. pd leader is killed and not recover
	// but the etcd client will not work well in these situations
	// so we use pdCli to reset etcd client when we found the etcd client is not working
	pdCli   pdutil.PDAPIClient
	metrics map[string]prometheus.Counter
	// clock is for making it easier to mock time-related data structures in unit tests
	clock  clock.Clock
	cancel context.CancelFunc
	rwLock sync.RWMutex
}

// NewClient warps a clientV3.Client that provides etcd APIs required by TiCDC.
func NewClient(pdEndpoints []string, metrics map[string]prometheus.Counter) (*Client, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conf := config.GetGlobalServerConfig()

	grpcClient, err := pd.NewClientWithContext(ctx, pdEndpoints, conf.Security.PDSecurityOption())
	if err != nil {
		return nil, errors.Trace(err)
	}
	pdClient, err := pdutil.NewPDAPIClient(grpcClient, conf.Security)
	if err != nil {
		return nil, errors.Trace(err)
	}

	apiCtx, apiCancel := context.WithTimeout(ctx, 10*time.Second)
	defer apiCancel()

	realPDEndpoints, err := pdClient.CollectMemberEndpoints(apiCtx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(realPDEndpoints) > len(pdEndpoints) {
		log.Info("collect more endpoints than configured, update configured endpoints",
			zap.Strings("configured", pdEndpoints),
			zap.Strings("collected", realPDEndpoints))
		pdEndpoints = realPDEndpoints
	}

	log.Info("create etcd client with endpoints", zap.Strings("endpoints", pdEndpoints))
	cli, err := newBaseEtcdClient(realPDEndpoints)
	if err != nil {
		return nil, errors.Trace(err)
	}

	res := &Client{
		cli:       cli,
		pdCli:     pdClient,
		metrics:   metrics,
		clock:     clock.New(),
		cancel:    cancel,
		endpoints: pdEndpoints}

	//go res.checkEndpointsChange(ctx, pdEndpoints)

	return res, nil
}

func (c *Client) checkEndpointsChange(ctx context.Context, pdEndpoints []string) error {
	ticker := time.NewTicker(time.Second * 3)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Info("checkEndpointsChange exited")
			return ctx.Err()
		case <-ticker.C:
			endpoints, err := c.pdCli.CollectMemberEndpoints(ctx)
			if err != nil {
				log.Warn("cannot collect all members", zap.Error(err))
				continue
			}

			healthEndpoints := make([]string, 0, len(endpoints))
			for _, endpoint := range endpoints {
				ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
				if err := c.pdCli.Healthy(ctx, endpoint); err != nil {
					log.Warn("pd health check error",
						zap.String("endpoint", endpoint), zap.Error(err))
				} else {
					healthEndpoints = append(healthEndpoints, endpoint)
				}
				cancel()
			}

			// we only reset the etcd client when the endpoints number are reduced
			// and the endpoints are not equal to the previous endpoints
			if !reflect.DeepEqual(c.endpoints, healthEndpoints) &&
				len(healthEndpoints) <= len(c.endpoints) {
				log.Info("pd endpoints changed",
					zap.Strings("old", c.endpoints),
					zap.Strings("new", healthEndpoints))
				// double check if we can still connect to the etcd cluster by
				// the old etcd client, if so, there is no need to reset the etcd client.
				_, err := c.cli.MemberList(ctx)
				if err != nil {
					log.Warn("etcd member list failed, update the endpoints and reset the etcd client",
						zap.Strings("old", c.endpoints),
						zap.Strings("new", healthEndpoints),
						zap.Error(err))
					c.endpoints = healthEndpoints
					// TODO: fizz add retry logic to make it more robust
					baseCli, err := newBaseEtcdClient(c.endpoints)
					if err != nil {
						log.Warn("reset etcd client failed", zap.Error(err))
						continue
					}
					c.rwLock.Lock()
					c.cli.Close()
					c.cli = baseCli
					c.rwLock.Unlock()
				}
			}
		}
	}
}

func (c *Client) Close() error {
	log.Info("closing etcd client")
	c.cancel()
	c.rwLock.Lock()
	defer c.rwLock.Unlock()
	c.pdCli.Close()
	return c.cli.Close()
}

// Put delegates request to clientV3.KV.Put
func (c *Client) Put(
	ctx context.Context, key, val string, opts ...clientV3.OpOption,
) (resp *clientV3.PutResponse, err error) {
	putCtx, cancel := context.WithTimeout(ctx, etcdClientTimeoutDuration)
	defer cancel()
	err = retryRPC(EtcdPut, c.metrics[EtcdPut], func() error {
		var inErr error
		c.rwLock.RLock()
		resp, inErr = c.cli.Put(putCtx, key, val, opts...)
		c.rwLock.RUnlock()
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
		c.rwLock.RLock()
		resp, inErr = c.cli.Get(getCtx, key, opts...)
		c.rwLock.RUnlock()
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
	c.rwLock.RLock()
	defer c.rwLock.RUnlock()
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
		c.rwLock.RLock()
		resp, inErr = c.cli.Txn(txnCtx).If(cmps...).Then(opsThen...).Else(opsElse...).Commit()
		c.rwLock.RUnlock()
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
		c.rwLock.RLock()
		resp, inErr = c.cli.Grant(grantCtx, ttl)
		c.rwLock.RUnlock()
		return inErr
	})
	if err != nil {
		log.Error("etcd grant failed", zap.Error(err))
	}
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
		c.rwLock.RLock()
		resp, inErr = c.cli.Revoke(revokeCtx, id)
		c.rwLock.RUnlock()
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
		c.rwLock.RLock()
		resp, inErr = c.cli.TimeToLive(timeToLiveCtx, lease, opts...)
		c.rwLock.RUnlock()
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

	c.rwLock.RLock()
	watchCh := c.cli.Watch(watchCtx, key, opts...)
	c.rwLock.RUnlock()

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
				c.rwLock.RLock()
				watchCh = c.cli.Watch(watchCtx, key,
					clientV3.WithPrefix(), clientV3.WithRev(lastRevision))
				c.rwLock.RUnlock()
				// we need to reset lastReceivedResponseTime after reset Watch
				lastReceivedResponseTime = c.clock.Now()
			}
		}
	}
}

// RequestProgress requests a progress notify response be sent in all watch channels.
func (c *Client) RequestProgress(ctx context.Context) error {
	c.rwLock.RLock()
	defer c.rwLock.RUnlock()
	return c.cli.RequestProgress(ctx)
}

// GetSession creates a new session with leaseID.
func (c *Client) GetSession(leaseID clientV3.LeaseID) (*concurrency.Session, error) {
	sess, err := concurrency.NewSession(
		c.cli, concurrency.WithLease(leaseID))
	if err != nil {
		return nil, errors.Trace(err)
	}
	return sess, nil
}

// newBaseEtcdClient creates a new etcd client with default configurations.
func newBaseEtcdClient(endpoints []string) (*clientV3.Client, error) {
	conf := config.GetGlobalServerConfig()

	grpcTLSOption, err := conf.Security.ToGRPCDialOption()
	if err != nil {
		return nil, errors.Trace(err)
	}

	tlsConfig, err := conf.Security.ToTLSConfig()
	if err != nil {
		return nil, errors.Trace(err)
	}

	logConfig := logutil.DefaultZapLoggerConfig
	logConfig.Level = zap.NewAtomicLevelAt(zapcore.ErrorLevel)

	// we do not pass a `context` to the etcd client,
	// to prevent it's cancelled when the server is closing.
	// For example, when the non-owner node goes offline,
	// it would resign the campaign key which was put by call `campaign`,
	// if this is not done due to the passed context cancelled,
	// the key will be kept for the lease TTL, which is 10 seconds,
	// then cause the new owner cannot be elected immediately after the old owner offline.
	// see https://github.com/etcd-io/etcd/blob/525d53bd41/client/v3/concurrency/election.go#L98
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:        endpoints,
		TLS:              tlsConfig,
		LogConfig:        &logConfig,
		DialTimeout:      5 * time.Second,
		AutoSyncInterval: 30 * time.Second,
		DialOptions: []grpc.DialOption{
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
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:    10 * time.Second,
				Timeout: 20 * time.Second,
			}),
		},
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	return etcdCli, nil
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
