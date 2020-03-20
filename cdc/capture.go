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
	"sync"
	"time"

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
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
)

const (
	ownerRunInterval  = time.Millisecond * 500
	captureSessionTTL = 3
)

// Capture represents a Capture server, it monitors the changefeed information in etcd and schedules Task on it.
type Capture struct {
	pdEndpoints  []string
	etcdClient   kv.CDCEtcdClient
	ownerManager roles.Manager
	ownerWorker  *ownerImpl

	processors map[string]*processor
	procLock   sync.Mutex

	info *model.CaptureInfo

	// session keeps alive between the capture and etcd
	session *concurrency.Session
}

// NewCapture returns a new Capture instance
func NewCapture(pdEndpoints []string) (c *Capture, err error) {
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   pdEndpoints,
		DialTimeout: 5 * time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithConnectParams(grpc.ConnectParams{
				Backoff: backoff.Config{
					BaseDelay:  time.Second,
					Multiplier: 1.1,
					Jitter:     0.1,
					MaxDelay:   3 * time.Second,
				},
				MinConnectTimeout: 3 * time.Second,
			}),
		},
	})
	if err != nil {
		return nil, errors.Annotate(err, "new etcd client")
	}
	sess, err := concurrency.NewSession(etcdCli,
		concurrency.WithTTL(captureSessionTTL))
	if err != nil {
		return nil, errors.Annotate(err, "create capture session")
	}
	cli := kv.NewCDCEtcdClient(etcdCli)
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
		session:      sess,
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
		return errors.Trace(err)
	}

	err = c.ownerManager.CampaignOwner(ctx)
	if err != nil {
		return errors.Annotate(err, "CampaignOwner")
	}

	errg, cctx := errgroup.WithContext(ctx)

	errg.Go(func() error {
		return c.ownerWorker.Run(cctx, ownerRunInterval)
	})

	taskWatcher := NewTaskWatcher(c, &TaskWatcherConfig{
		Prefix:      kv.TaskStatusKeyPrefix + "/" + c.info.ID,
		ChannelSize: 128,
	})
	log.Info("waiting for tasks", zap.String("captureid", c.info.ID))
	for ev := range taskWatcher.Watch(ctx) {
		if ev.Err != nil {
			return errors.Trace(ev.Err)
		}
		task := ev.Task
		if ev.Op == TaskOpCreate {
			cf, err := c.etcdClient.GetChangeFeedInfo(ctx, task.ChangeFeedID)
			if err != nil {
				log.Error("get change feed info failed",
					zap.String("changefeedid", task.ChangeFeedID),
					zap.String("captureid", c.info.ID),
					zap.Error(err))
				return err
			}
			log.Info("run processor", zap.String("captureid", c.info.ID),
				zap.String("changefeedid", task.ChangeFeedID))
			if _, ok := c.processors[task.ChangeFeedID]; !ok {
				p, err := runProcessor(ctx, c.pdEndpoints, *cf, task.ChangeFeedID,
					c.info.ID, task.CheckpointTS)
				if err != nil {
					log.Error("run processor failed",
						zap.String("changefeedid", task.ChangeFeedID),
						zap.String("captureid", c.info.ID),
						zap.Error(err))
					return err
				}
				c.processors[task.ChangeFeedID] = p
			}
		} else if ev.Op == TaskOpDelete {
			if p, ok := c.processors[task.ChangeFeedID]; ok {
				if err := p.stop(ctx); err != nil {
					return errors.Trace(err)
				}
				delete(c.processors, task.ChangeFeedID)
			}
		}
	}

	return errg.Wait()
}

// Cleanup cleans all dynamic resources
func (c *Capture) Cleanup() {
	c.procLock.Lock()
	defer c.procLock.Unlock()

	for _, processor := range c.processors {
		processor.wait()
	}
}

// Close closes the capture by unregistering it from etcd
func (c *Capture) Close(ctx context.Context) error {
	return errors.Trace(c.etcdClient.DeleteCaptureInfo(ctx, c.info.ID))
}

// register registers the capture information in etcd
func (c *Capture) register(ctx context.Context) error {
	return errors.Trace(c.etcdClient.PutCaptureInfo(ctx, c.info, c.session.Lease()))
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
