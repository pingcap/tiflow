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

package test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/phayes/freeport"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"

	"github.com/pingcap/tiflow/engine/pkg/etcdutils"
	"github.com/pingcap/tiflow/pkg/retry"
)

func allocTempURL(t *testing.T) string {
	port, err := freeport.GetFreePort()
	require.Nil(t, err)
	return fmt.Sprintf("127.0.0.1:%d", port)
}

// PrepareEtcd creates a single embed etcd server used fo test, and returns
// - advertiseAddr of the embed etcd
// - embed etcd server
// - etcd client
// - etcd cleanup function
func PrepareEtcd(t *testing.T, name string) (
	advertiseAddr string, etcd *embed.Etcd, client *clientv3.Client, cleanFn func(),
) {
	err := retry.Do(context.TODO(), func() error {
		var err error
		advertiseAddr, etcd, client, cleanFn, err = prepareEtcdOnce(t, name)
		return err
	},
		retry.WithBackoffBaseDelay(1000 /* 1000 ms */),
		retry.WithBackoffMaxDelay(3000 /* 3 seconds */),
		retry.WithMaxTries(3 /* fail after 10 seconds*/),
		retry.WithIsRetryableErr(func(err error) bool {
			if strings.Contains(err.Error(), "address already in use") {
				return true
			}
			return false
		}),
	)
	require.Nil(t, err)
	return
}

func prepareEtcdOnce(t *testing.T, name string) (
	advertiseAddr string,
	etcd *embed.Etcd,
	client *clientv3.Client,
	cleanFn func(),
	err error,
) {
	dir := t.TempDir()

	masterAddr := allocTempURL(t)
	advertiseAddr = masterAddr
	cfgCluster := &etcdutils.ConfigParams{}
	cfgCluster.Name = name
	cfgCluster.DataDir = dir
	cfgCluster.PeerUrls = "http://" + allocTempURL(t)
	cfgCluster.Adjust("", embed.ClusterStateFlagNew)

	cfgClusterEtcd := etcdutils.GenEmbedEtcdConfigWithLogger("info")
	cfgClusterEtcd, err = etcdutils.GenEmbedEtcdConfig(cfgClusterEtcd, masterAddr, advertiseAddr, cfgCluster)
	require.Nil(t, err)

	etcd, err = etcdutils.StartEtcd(cfgClusterEtcd, nil, nil, time.Minute)
	if err != nil {
		return
	}

	client, err = clientv3.New(clientv3.Config{
		Endpoints:   []string{advertiseAddr},
		DialTimeout: 3 * time.Second,
	})
	if err != nil {
		return
	}
	require.Nil(t, err)

	cleanFn = func() {
		etcd.Close()
	}

	return
}

// PrepareEtcdCluster creates an etcd cluster with multiple nodes
func PrepareEtcdCluster(t *testing.T, names []string) (
	advertiseAddrs []string,
	embedEtcds []*embed.Etcd,
	etcdCli *clientv3.Client,
	cleanFn func(),
) {
	err := retry.Do(context.TODO(), func() error {
		var err error
		advertiseAddrs, embedEtcds, etcdCli, cleanFn, err = prepareEtcdClusterOnce(t, names)
		return err
	},
		retry.WithBackoffBaseDelay(1000 /* 1000 ms */),
		retry.WithBackoffMaxDelay(3000 /* 3 seconds */),
		retry.WithMaxTries(3 /* fail after 10 seconds*/),
		retry.WithIsRetryableErr(func(err error) bool {
			if strings.Contains(err.Error(), "address already in use") {
				return true
			}
			return false
		}),
	)
	require.Nil(t, err)
	return
}

func prepareEtcdClusterOnce(t *testing.T, names []string) (
	advertiseAddrs []string,
	embedEtcds []*embed.Etcd,
	etcdCli *clientv3.Client,
	cleanFn func(),
	err error,
) {
	dirs := make([]string, 0, len(names))
	cfgs := make([]*etcdutils.ConfigParams, 0, len(names))
	initialCluster := make([]string, 0, len(names))
	for _, name := range names {
		dir := t.TempDir()
		dirs = append(dirs, dir)

		advertiseAddr := allocTempURL(t)
		advertiseAddrs = append(advertiseAddrs, advertiseAddr)

		cfgCluster := &etcdutils.ConfigParams{}
		cfgCluster.Name = name
		cfgCluster.DataDir = dir
		cfgCluster.PeerUrls = "http://" + allocTempURL(t)
		cfgs = append(cfgs, cfgCluster)

		initialCluster = append(initialCluster, fmt.Sprintf("%s=%s", name, cfgCluster.PeerUrls))
	}
	etcdCh := make(chan *embed.Etcd)
	startEtcdError := make(chan error)
	for idx, cfg := range cfgs {
		cfg.InitialCluster = strings.Join(initialCluster, ",")
		cfg.Adjust("", embed.ClusterStateFlagNew)
		cfgClusterEtcd := etcdutils.GenEmbedEtcdConfigWithLogger("info")
		addr := advertiseAddrs[idx]
		cfgClusterEtcd, err := etcdutils.GenEmbedEtcdConfig(cfgClusterEtcd, addr, addr, cfg)
		require.Nil(t, err)
		go func() {
			etcd, err := etcdutils.StartEtcd(cfgClusterEtcd, nil, nil, time.Minute)
			if err != nil {
				startEtcdError <- err
			} else {
				etcdCh <- etcd
			}
		}()
	}
	startEtcdErrors := make([]error, 0)
	checkFetchFinish := func() bool {
		return len(startEtcdErrors)+len(embedEtcds) == len(names)
	}
fetchLoop:
	for {
		select {
		case etcd := <-etcdCh:
			embedEtcds = append(embedEtcds, etcd)
			if len(embedEtcds) == len(names) {
				break fetchLoop
			}
		case startErr := <-startEtcdError:
			startEtcdErrors = append(startEtcdErrors, startErr)
		case <-time.After(time.Minute):
			t.Error("etcd doesn't start in time")
		}
		if checkFetchFinish() {
			break fetchLoop
		}
	}
	if len(startEtcdErrors) > 0 {
		err = startEtcdErrors[0]
		return
	}

	etcdCli, err = clientv3.New(clientv3.Config{
		Endpoints:   advertiseAddrs,
		DialTimeout: 3 * time.Second,
	})
	require.Nil(t, err)

	cleanFn = func() {
		for _, etcd := range embedEtcds {
			etcd.Close()
		}
	}

	return
}
