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
	"math"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	pd "github.com/pingcap/pd/client"
	"github.com/pingcap/tidb-cdc/cdc/kv"
	"github.com/pingcap/tidb-cdc/cdc/model"
	"github.com/pingcap/tidb-cdc/cdc/roles"
	"github.com/pingcap/tidb-cdc/pkg/flags"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store"
	"github.com/pingcap/tidb/store/tikv"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

const (
	// CaptureOwnerKey is the capture owner path that is saved to etcd
	CaptureOwnerKey = kv.EtcdKeyBase + "/capture/owner"
)

// Capture represents a Capture server, it monitors the changefeed information in etcd and schedules SubChangeFeed on it.
type Capture struct {
	pdEndpoints  []string
	etcdClient   *clientv3.Client
	ownerManager roles.Manager
	ownerWorker  roles.Owner

	info *model.CaptureInfo
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
	info := &model.CaptureInfo{
		ID: id,
	}

	log.Info("creating capture", zap.String("capture-id", id))

	manager := roles.NewOwnerManager(cli, id, CaptureOwnerKey)
	pdCli, err := fNewPDCli(pdEndpoints, pd.SecurityOption{})
	if err != nil {
		return nil, errors.Annotatef(err, "create pd client failed, addr: %v", pdEndpoints)
	}
	ddlHandler := NewDDLHandler(pdCli)
	worker := roles.NewOwner(math.MaxUint64, cli, manager, ddlHandler)

	c = &Capture{
		pdEndpoints:  pdEndpoints,
		etcdClient:   cli,
		ownerManager: manager,
		ownerWorker:  worker,
		info:         info,
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

	errg, cctx := errgroup.WithContext(ctx)

	errg.Go(func() error {
		return c.ownerWorker.Run(cctx, time.Second*5)
	})

	watcher := NewChangeFeedWatcher(c.info.ID, c.pdEndpoints, c.etcdClient)
	errg.Go(func() error {
		return watcher.Watch(cctx)
	})

	return errg.Wait()
}

func (c *Capture) Close(ctx context.Context) error {
	return errors.Trace(DeleteCaptureInfo(ctx, c.info.ID, c.etcdClient))
}

func (c *Capture) infoKey() string {
	return infoKey(c.info.ID)
}

// register registers the capture information in etcd
func (c *Capture) register(ctx context.Context) error {
	return errors.Trace(PutCaptureInfo(ctx, c.info, c.etcdClient))
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
