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

package metadata

import (
	"context"
	"errors"

	clientv3 "go.etcd.io/etcd/client/v3"
)

var _ MetaKV = &MetaEtcd{}

type MetaEtcd struct {
	cli *clientv3.Client
}

func NewMetaEtcd(cli *clientv3.Client) *MetaEtcd {
	return &MetaEtcd{
		cli: cli,
	}
}

func getEtcdOptions(opts ...interface{}) ([]clientv3.OpOption, error) {
	etcdOpts := make([]clientv3.OpOption, 0, len(opts))
	for _, opt := range opts {
		if eopt, ok := opt.(clientv3.OpOption); ok {
			etcdOpts = append(etcdOpts, eopt)
		} else {
			return nil, errors.New("invalid option")
		}
	}
	return etcdOpts, nil
}

func (c *MetaEtcd) Put(
	ctx context.Context, key, value string, opts ...interface{},
) (interface{}, error) {
	etcdOpts, err := getEtcdOptions(opts...)
	if err != nil {
		return nil, err
	}
	return c.cli.Put(ctx, key, value, etcdOpts...)
}

func (c *MetaEtcd) Get(
	ctx context.Context, key string, opts ...interface{},
) (interface{}, error) {
	etcdOpts, err := getEtcdOptions(opts...)
	if err != nil {
		return nil, err
	}
	return c.cli.Get(ctx, key, etcdOpts...)
}

func (c *MetaEtcd) Delete(
	ctx context.Context, key string, opts ...interface{},
) (interface{}, error) {
	etcdOpts, err := getEtcdOptions(opts...)
	if err != nil {
		return nil, err
	}
	return c.cli.Delete(ctx, key, etcdOpts...)
}

func (c *MetaEtcd) Txn(ctx context.Context) interface{} {
	return c.cli.Txn(ctx)
}

func (c *MetaEtcd) Watch(ctx context.Context, key string, opts ...interface{}) interface{} {
	etcdOpts, err := getEtcdOptions(opts...)
	if err != nil {
		panic(err)
	}
	return c.cli.Watch(ctx, key, etcdOpts...)
}
