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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/retry"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

// Client is a simple wrapper that adds retry to etcd RPC
type Client struct {
	cli *clientv3.Client
}

// Wrap warps a clientv3.Client that provides etcd APIs required by TiCDC.
func Wrap(cli *clientv3.Client) *Client {
	return &Client{cli: cli}
}

// Unwrap returns a clientv3.Client
func (c *Client) Unwrap() *clientv3.Client {
	return c.cli
}

func retryRPC(rpcName string, etcdRPC func() error) error {
	// By default, PD etcd sets [3s, 6s) for election timeout.
	// Some rpc could fail due to etcd errors, like "proposal dropped".
	// Retry at least two election timeout to handle the case that two PDs restarted
	// (the first election maybe failed).
	// 16s = \sum_{n=0}^{6} 0.5*1.5^n
	return retry.Run(500*time.Millisecond, 7+1, // +1 for the inital request.
		func() error {
			err := etcdRPC()
			if err != nil && errors.Cause(err) != context.Canceled {
				log.Warn("etcd RPC failed", zap.String("RPC", rpcName), zap.Error(err))
			}
			return err
		})
}

// Put delegates request to clientv3.KV.Put
func (c *Client) Put(ctx context.Context, key, val string, opts ...clientv3.OpOption) (resp *clientv3.PutResponse, err error) {
	err = retryRPC("Put", func() error {
		var inErr error
		resp, inErr = c.cli.Put(ctx, key, val, opts...)
		return inErr
	})
	return
}

// Get delegates request to clientv3.KV.Get
func (c *Client) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (resp *clientv3.GetResponse, err error) {
	err = retryRPC("Get", func() error {
		var inErr error
		resp, inErr = c.cli.Get(ctx, key, opts...)
		return inErr
	})
	return
}

// Delete delegates request to clientv3.KV.Delete
func (c *Client) Delete(ctx context.Context, key string, opts ...clientv3.OpOption) (resp *clientv3.DeleteResponse, err error) {
	// We don't retry on delete operatoin. It's dangerous.
	return c.cli.Delete(ctx, key, opts...)
}

// Txn delegates request to clientv3.KV.Txn
func (c *Client) Txn(ctx context.Context) clientv3.Txn {
	return c.cli.Txn(ctx)
}

// Grant delegates request to clientv3.Lease.Grant
func (c *Client) Grant(ctx context.Context, ttl int64) (resp *clientv3.LeaseGrantResponse, err error) {
	err = retryRPC("Grant", func() error {
		var inErr error
		resp, inErr = c.cli.Grant(ctx, ttl)
		return inErr
	})
	return
}

// Revoke delegates request to clientv3.Lease.Revoke
func (c *Client) Revoke(ctx context.Context, id clientv3.LeaseID) (resp *clientv3.LeaseRevokeResponse, err error) {
	err = retryRPC("Revoke", func() error {
		var inErr error
		resp, inErr = c.cli.Revoke(ctx, id)
		return inErr
	})
	return
}

// Watch delegates request to clientv3.Watcher.Watch
func (c *Client) Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
	return c.cli.Watch(ctx, key, opts...)
}
