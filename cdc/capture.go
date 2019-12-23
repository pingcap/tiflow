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
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/roles"
	"github.com/pingcap/ticdc/pkg/flags"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store"
	"github.com/pingcap/tidb/store/tikv"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

// Capture represents a Capture server, it monitors the changefeed information in etcd and schedules SubChangeFeed on it.
type Capture struct {
	pdEndpoints  []string
	etcdClient   *clientv3.Client
	ownerManager roles.Manager
	ownerWorker  *ownerImpl

	processors map[string]*processor

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

	manager := roles.NewOwnerManager(cli, id, kv.CaptureOwnerKey)

	worker, err := NewOwner(pdEndpoints, cli, manager)
	if err != nil {
		return nil, errors.Annotate(err, "new owner failed")
	}

	c = &Capture{
		processors:   make(map[string]*processor),
		pdEndpoints:  pdEndpoints,
		etcdClient:   cli,
		ownerManager: manager,
		ownerWorker:  worker,
		info:         info,
	}

	return
}

var _ processorCallback = &Capture{}

// OnRunProcessor implements processorCallback.
func (c *Capture) OnRunProcessor(p *processor) {
	c.processors[p.changefeedID] = p
}

// OnStopProcessor implements processorCallback.
func (c *Capture) OnStopProcessor(p *processor) {
	delete(c.processors, p.changefeedID)
}

// Start starts the Capture mainloop
func (c *Capture) Start(ctx context.Context) (err error) {
	// TODO: better channgefeed model with etcd storage
	err = c.register(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	err = c.ownerManager.CampaignOwner(ctx)
	if err != nil {
		return errors.Annotate(err, "CampaignOwner")
	}

	errg, cctx := errgroup.WithContext(ctx)

	errg.Go(func() error {
		return c.ownerWorker.Run(cctx, time.Second*1)
	})

	watcher := NewChangeFeedWatcher(c.info.ID, c.pdEndpoints, c.etcdClient)
	errg.Go(func() error {
		return watcher.Watch(cctx, c)
	})

	return errg.Wait()
}

// Close closes the capture by unregistering it from etcd
func (c *Capture) Close(ctx context.Context) error {
	return errors.Trace(DeleteCaptureInfo(ctx, c.info.ID, c.etcdClient))
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

	// Ignore error if it is already registered.
	_ = store.Register("tikv", tikv.Driver{})

	tiPath := fmt.Sprintf("tikv://%s?disableGC=true", urlv.HostString())
	tiStore, err := store.New(tiPath)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return tiStore, nil
}
