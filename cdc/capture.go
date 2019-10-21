// Copyright 2019 PingCAP, Inc.
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

package cdc

import (
	"context"
	"fmt"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-cdc/cdc/kv"
	"github.com/pingcap/tidb-cdc/cdc/roles"
	"github.com/pingcap/tidb-cdc/cdc/util"
	"github.com/pingcap/tidb-cdc/pkg/flags"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store"
	"github.com/pingcap/tidb/store/tikv"
	"google.golang.org/grpc"
)

const (
	// CaptureOwnerKey is the capture owner path that is saved to etcd
	CaptureOwnerKey = kv.EtcdKeyBase + "/capture/owner"
)

type ResolvedSpan struct {
	Span      util.Span
	Timestamp uint64
}

// Capture represents a Capture server, it monitors the changefeed information in etcd and schedules SubChangeFeed on it.
type Capture struct {
	id string

	pdEndpoints  []string
	etcdClient   *clientv3.Client
	ownerManager roles.Manager
}

// NewCapture returns a new Capture instance
func NewCapture(pdEndpoints []string) (c *Capture, err error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   pdEndpoints,
		DialTimeout: 5 * time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithBackoffMaxDelay(time.Second * 3),
		},
	})
	if err != nil {
		return nil, errors.Annotate(err, "new etcd client")
	}

	id := uuid.New().String()

	manager := roles.NewOwnerManager(cli, id, CaptureOwnerKey)

	c = &Capture{
		id:           id,
		pdEndpoints:  pdEndpoints,
		etcdClient:   cli,
		ownerManager: manager,
	}

	return
}

// Start starts the Capture mainloop
func (c *Capture) Start(ctx context.Context) (err error) {
	// TODO: better channgefeed model with etcd storage
	err = c.register(ctx)
	if err != nil {
		return err
	}

	err = c.ownerManager.CampaignOwner(ctx)
	if err != nil {
		return errors.Annotate(err, "CampaignOwner")
	}

	// create a test changefeed
	detail := ChangeFeedDetail{
		SinkURI:      "root@tcp(127.0.0.1:3306)/test",
		Opts:         make(map[string]string),
		CheckpointTS: 0,
		CreateTime:   time.Now(),
	}
	changefeedID := uuid.New().String()
	err = detail.SaveChangeFeedDetail(ctx, c.etcdClient, changefeedID)
	if err != nil {
		return errors.Trace(err)
	}
	// create subchangefeed for test
	// TODO: should be updated by owner
	skey := getEtcdKeySubChangeFeed(changefeedID, c.id)
	c.etcdClient.Put(ctx, skey, "")
	defer func() {
		c.etcdClient.Delete(ctx, getEtcdKeyChangeFeed(changefeedID))
		c.etcdClient.Delete(ctx, skey)
	}()

	watcher := NewChangeFeedWatcher(c.id, c.pdEndpoints, c.etcdClient)
	return watcher.Watch(ctx)
}

func (c *Capture) Close(ctx context.Context) error {
	_, err := c.etcdClient.Delete(ctx, c.infoKey())
	return errors.Trace(err)
}

func (c *Capture) infoKey() string {
	return fmt.Sprintf("%s/capture/info/%s", kv.EtcdKeyBase, c.id)
}

// register registers the capture information in etcd
func (c *Capture) register(ctx context.Context) error {
	_, err := c.etcdClient.Put(ctx, c.infoKey(), "{}")
	return errors.Trace(err)
}

func createTiStore(urls string) (tidbkv.Storage, error) {
	urlv, err := flags.NewURLsValue(urls)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if err := store.Register("tikv", tikv.Driver{}); err != nil {
		return nil, errors.Trace(err)
	}
	tiPath := fmt.Sprintf("tikv://%s?disableGC=true", urlv.HostString())
	tiStore, err := store.New(tiPath)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return tiStore, nil
}
