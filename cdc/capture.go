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

package cdc

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/security"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.etcd.io/etcd/mvcc"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
)

const (
	captureSessionTTL = 3
)

// processorOpts records options for processor
type processorOpts struct {
	flushCheckpointInterval time.Duration
}

// Capture represents a Capture server, it monitors the changefeed information in etcd and schedules Task on it.
type Capture struct {
	etcdClient kv.CDCEtcdClient
	credential *security.Credential

	processors map[string]*processor
	procLock   sync.Mutex

	info *model.CaptureInfo

	// session keeps alive between the capture and etcd
	session  *concurrency.Session
	election *concurrency.Election

	opts *processorOpts
}

// NewCapture returns a new Capture instance
func NewCapture(
	ctx context.Context,
	pdEndpoints []string,
	credential *security.Credential,
	advertiseAddr string,
	opts *processorOpts,
) (c *Capture, err error) {
	tlsConfig, err := credential.ToTLSConfig()
	if err != nil {
		return nil, errors.Trace(err)
	}
	grpcTLSOption, err := credential.ToGRPCDialOption()
	if err != nil {
		return nil, errors.Trace(err)
	}
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   pdEndpoints,
		TLS:         tlsConfig,
		Context:     ctx,
		DialTimeout: 5 * time.Second,
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
		},
	})
	if err != nil {
		return nil, errors.Annotate(cerror.WrapError(cerror.ErrNewCaptureFailed, err), "new etcd client")
	}
	sess, err := concurrency.NewSession(etcdCli,
		concurrency.WithTTL(captureSessionTTL))
	if err != nil {
		return nil, errors.Annotate(cerror.WrapError(cerror.ErrNewCaptureFailed, err), "create capture session")
	}
	elec := concurrency.NewElection(sess, kv.CaptureOwnerKey)
	cli := kv.NewCDCEtcdClient(ctx, etcdCli)
	id := uuid.New().String()
	info := &model.CaptureInfo{
		ID:            id,
		AdvertiseAddr: advertiseAddr,
	}
	log.Info("creating capture",
		zap.String("capture-id", id), zap.String("advertise-addr", advertiseAddr))

	c = &Capture{
		processors: make(map[string]*processor),
		etcdClient: cli,
		credential: credential,
		session:    sess,
		election:   elec,
		info:       info,
		opts:       opts,
	}

	return
}

// Run runs the Capture mainloop
func (c *Capture) Run(ctx context.Context) (err error) {
	ctx, cancel := context.WithCancel(ctx)
	// TODO: we'd better to add some wait mechanism to ensure no routine is blocked
	defer cancel()
	err = c.register(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	taskWatcher := NewTaskWatcher(c, &TaskWatcherConfig{
		Prefix:      kv.TaskStatusKeyPrefix + "/" + c.info.ID,
		ChannelSize: 128,
	})
	log.Info("waiting for tasks", zap.String("captureid", c.info.ID))
	var ev *TaskEvent
	wch := taskWatcher.Watch(ctx)
	for {
		// Return error when the session is done unexpectedly, it means the
		// server does not send heartbeats in time, or network interrupted
		// In this case, the state of the capture is undermined, the task may
		// have or have not been rebalanced, the owner may be or not be held,
		// so we must cancel context to let all sub routines exit.
		select {
		case <-c.session.Done():
			if ctx.Err() != context.Canceled {
				log.Info("capture session done, capture suicide itself", zap.String("capture", c.info.ID))
				return cerror.ErrCaptureSuicide.GenWithStackByArgs()
			}
		case ev = <-wch:
			if ev == nil {
				return nil
			}
			if ev.Err != nil {
				return errors.Trace(ev.Err)
			}
			if err := c.handleTaskEvent(ctx, ev); err != nil {
				return errors.Trace(err)
			}
		}
	}
}

// Campaign to be an owner
func (c *Capture) Campaign(ctx context.Context) error {
	failpoint.Inject("capture-campaign-compacted-error", func() {
		failpoint.Return(errors.Trace(mvcc.ErrCompacted))
	})
	return cerror.WrapError(cerror.ErrCaptureCampaignOwner, c.election.Campaign(ctx, c.info.ID))
}

// Resign lets a owner start a new election.
func (c *Capture) Resign(ctx context.Context) error {
	failpoint.Inject("capture-resign-failed", func() {
		failpoint.Return(errors.New("capture resign failed"))
	})
	return cerror.WrapError(cerror.ErrCaptureResignOwner, c.election.Resign(ctx))
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

func (c *Capture) handleTaskEvent(ctx context.Context, ev *TaskEvent) error {
	task := ev.Task
	if ev.Op == TaskOpCreate {
		if _, ok := c.processors[task.ChangeFeedID]; !ok {
			p, err := c.assignTask(ctx, task)
			if err != nil {
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
	return nil
}

func (c *Capture) assignTask(ctx context.Context, task *Task) (*processor, error) {
	cf, err := c.etcdClient.GetChangeFeedInfo(ctx, task.ChangeFeedID)
	if err != nil {
		log.Error("get change feed info failed",
			zap.String("changefeedid", task.ChangeFeedID),
			zap.String("captureid", c.info.ID),
			zap.Error(err))
		return nil, err
	}
	err = cf.VerifyAndFix()
	if err != nil {
		return nil, err
	}
	log.Info("run processor", zap.String("captureid", c.info.ID),
		zap.String("changefeedid", task.ChangeFeedID))

	p, err := runProcessor(
		ctx, c.credential, c.session, *cf, task.ChangeFeedID, *c.info, task.CheckpointTS, c.opts.flushCheckpointInterval)
	if err != nil {
		log.Error("run processor failed",
			zap.String("changefeedid", task.ChangeFeedID),
			zap.String("captureid", c.info.ID),
			zap.String("captureaddr", c.info.AdvertiseAddr),
			zap.Error(err))
		return nil, err
	}
	return p, nil
}

// register registers the capture information in etcd
func (c *Capture) register(ctx context.Context) error {
	err := c.etcdClient.PutCaptureInfo(ctx, c.info, c.session.Lease())
	return cerror.WrapError(cerror.ErrCaptureRegister, err)
}
