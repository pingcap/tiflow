// Copyright 2021 PingCAP, Inc.
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

package capture

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
	"github.com/pingcap/ticdc/cdc/owner"
	"github.com/pingcap/ticdc/cdc/processor"
	"github.com/pingcap/ticdc/pkg/config"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/orchestrator"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/ticdc/pkg/version"
	pd "github.com/tikv/pd/client"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.etcd.io/etcd/mvcc"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

// Capture represents a Capture server, it monitors the changefeed information in etcd and schedules Task on it.
type Capture struct {
	info *model.CaptureInfo

	etcdCli kv.CDCEtcdClient
	pdCli   pd.Client

	owner            *owner.Owner
	ownerMu          sync.Mutex
	processorManager *processor.Manager

	// session keeps alive between the capture and etcd
	session  *concurrency.Session
	election *concurrency.Election
}

// NewCapture returns a new Capture instance
func NewCapture(ctx context.Context, etcdCli *clientv3.Client, pdCli pd.Client) (c *Capture, err error) {
	conf := config.GetGlobalServerConfig()
	sess, err := concurrency.NewSession(etcdCli,
		concurrency.WithTTL(conf.CaptureSessionTTL))
	if err != nil {
		return nil, errors.Annotate(cerror.WrapError(cerror.ErrNewCaptureFailed, err), "create capture session")
	}
	elec := concurrency.NewElection(sess, kv.CaptureOwnerKey)
	cli := kv.NewCDCEtcdClient(ctx, sess.Client())
	info := &model.CaptureInfo{
		ID:            uuid.New().String(),
		AdvertiseAddr: conf.AdvertiseAddr,
		Version:       version.ReleaseVersion,
	}
	processorManager := processor.NewManager(pdCli, conf.Security, info, sess.Lease())
	log.Info("creating capture", zap.String("capture-id", info.ID), util.ZapFieldCapture(ctx))
	c = &Capture{
		etcdCli:          cli,
		session:          sess,
		election:         elec,
		info:             info,
		pdCli:            pdCli,
		processorManager: processorManager,
	}
	return
}

func (c *Capture) Run(ctx context.Context) error {
	err := c.register(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		timeoutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := c.etcdCli.DeleteCaptureInfo(timeoutCtx, c.info.ID); err != nil {
			log.Warn("failed to delete capture info when capture exited", zap.Error(err))
		}
		cancel()
	}()
	ctx, cancel := context.WithCancel(ctx)
	wg, ctx := errgroup.WithContext(ctx)
	wg.Go(func() error {
		defer cancel()
		return c.campaignOwner(ctx)
	})

	wg.Go(func() error {
		defer cancel()
		return c.runEtcdWorker(ctx, c.processorManager, model.NewGlobalState())
	})
	return wg.Wait()
}

func (c *Capture) Info() model.CaptureInfo {
	return *c.info
}

func (c *Capture) campaignOwner(ctx context.Context) error {
	// In most failure cases, we don't return error directly, just run another
	// campaign loop. We treat campaign loop as a special background routine.
	conf := config.GetGlobalServerConfig()
	rl := rate.NewLimiter(0.05, 2)
	for {
		err := rl.Wait(ctx)
		if err != nil {
			if errors.Cause(err) == context.Canceled {
				return nil
			}
			return errors.Trace(err)
		}
		// Campaign to be an owner, it blocks until it becomes the owner
		if err := c.campaign(ctx); err != nil {
			switch errors.Cause(err) {
			case context.Canceled:
				return nil
			case mvcc.ErrCompacted:
				continue
			}
			log.Warn("campaign owner failed", zap.Error(err))
			continue
		}

		log.Info("campaign owner successfully", zap.String("capture-id", c.info.ID))
		owner := owner.NewOwner(c.pdCli, conf.Security, c.session.Lease())
		c.setOwner(owner)
		err = c.runEtcdWorker(ctx, owner, model.NewGlobalState())
		c.setOwner(nil)
		log.Info("run owner exited", zap.Error(err))
		if err == nil || cerror.ErrCaptureSuicide.Equal(errors.Cause(err)) {
			// if owner exits normally, or exits caused by lease expired
			if err = c.resign(ctx); err != nil {
				// if regisn owner failed, return error to let capture exits
				return errors.Annotatef(err, "resign owner failed, capture: %s", c.info.ID)
			}
			return nil
		}
	}
}

func (c *Capture) runEtcdWorker(ctx context.Context, reactor orchestrator.Reactor, reactorState orchestrator.ReactorState) error {
	etcdWorker, err := orchestrator.NewEtcdWorker(c.etcdCli.Client, kv.EtcdKeyBase, reactor, reactorState)
	if err != nil {
		return errors.Trace(err)
	}
	if err := etcdWorker.Run(ctx, c.session, 200*time.Millisecond); err != nil {
		// We check ttl of lease instead of check `session.Done`, because
		// `session.Done` is only notified when etcd client establish a
		// new keepalive request, there could be a time window as long as
		// 1/3 of session ttl that `session.Done` can't be triggered even
		// the lease is already revoked.
		switch {
		case cerror.ErrEtcdSessionDone.Equal(errors.Cause(err)),
			cerror.ErrLeaseExpired.Equal(errors.Cause(err)):
			return cerror.ErrCaptureSuicide.GenWithStackByArgs()
		}
		lease, inErr := c.etcdCli.Client.TimeToLive(ctx, c.session.Lease())
		if inErr != nil {
			return cerror.WrapError(cerror.ErrPDEtcdAPIError, inErr)
		}
		if lease.TTL == int64(-1) {
			log.Warn("session is disconnected", zap.Error(err))
			return cerror.ErrCaptureSuicide.GenWithStackByArgs()
		}
		return errors.Trace(err)
	}
	return nil
}

func (c *Capture) setOwner(owner *owner.Owner) {
	c.ownerMu.Lock()
	defer c.ownerMu.Unlock()
	c.owner = owner
}

func (c *Capture) OperateOwnerUnderLock(fn func(*owner.Owner) error) error {
	c.ownerMu.Lock()
	defer c.ownerMu.Unlock()
	if c.owner == nil {
		return cerror.ErrNotOwner.GenWithStackByArgs()
	}
	return fn(c.owner)
}

// Campaign to be an owner
func (c *Capture) campaign(ctx context.Context) error {
	failpoint.Inject("capture-campaign-compacted-error", func() {
		failpoint.Return(errors.Trace(mvcc.ErrCompacted))
	})
	return cerror.WrapError(cerror.ErrCaptureCampaignOwner, c.election.Campaign(ctx, c.info.ID))
}

// Resign lets a owner start a new election.
func (c *Capture) resign(ctx context.Context) error {
	failpoint.Inject("capture-resign-failed", func() {
		failpoint.Return(errors.New("capture resign failed"))
	})
	return cerror.WrapError(cerror.ErrCaptureResignOwner, c.election.Resign(ctx))
}

// register registers the capture information in etcd
func (c *Capture) register(ctx context.Context) error {
	err := c.etcdCli.PutCaptureInfo(ctx, c.info, c.session.Lease())
	return cerror.WrapError(cerror.ErrCaptureRegister, err)
}

// Close closes the capture by unregistering it from etcd
func (c *Capture) AsyncClose() {
	c.processorManager.AsyncClose()
	c.OperateOwnerUnderLock(func(o *owner.Owner) error {
		o.AsyncStop()
		return nil
	}) //nolint:errcheck
}

func (c *Capture) DebugInfo() string {
	return "TODO debug info"
}

func (c *Capture) IsOwner() bool {
	return c.OperateOwnerUnderLock(func(o *owner.Owner) error {
		return nil
	}) == nil
}
