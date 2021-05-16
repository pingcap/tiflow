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
	cdcContext "github.com/pingcap/ticdc/pkg/context"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/orchestrator"
	"github.com/pingcap/ticdc/pkg/version"
	tidbkv "github.com/pingcap/tidb/kv"
	pd "github.com/tikv/pd/client"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.etcd.io/etcd/mvcc"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

// Capture represents a Capture server, it monitors the changefeed information in etcd and schedules Task on it.
type Capture struct {
	captureMu sync.Mutex
	info      *model.CaptureInfo

	owner            *owner.Owner
	ownerMu          sync.Mutex
	processorManager *processor.Manager

	// session keeps alive between the capture and etcd
	session  *concurrency.Session
	election *concurrency.Election

	pdClient   pd.Client
	kvStorage  tidbkv.Storage
	etcdClient *kv.CDCEtcdClient

	newProcessorManager func() *processor.Manager
	newOwner            func() *owner.Owner
}

// NewCapture returns a new Capture instance
func NewCapture(pdClient pd.Client, kvStorage tidbkv.Storage, etcdClient *kv.CDCEtcdClient) *Capture {
	return &Capture{
		pdClient:   pdClient,
		kvStorage:  kvStorage,
		etcdClient: etcdClient,

		newProcessorManager: processor.NewManager,
		newOwner:            owner.NewOwner,
	}
}

func NewCapture4Test(pdClient pd.Client, kvStorage tidbkv.Storage, etcdClient *kv.CDCEtcdClient,
	newProcessorManager func() *processor.Manager,
	newOwner func() *owner.Owner,
) *Capture {
	c := NewCapture(pdClient, kvStorage, etcdClient)
	c.newProcessorManager = newProcessorManager
	c.newOwner = newOwner
	return c
}

func (c *Capture) reset() error {
	c.captureMu.Lock()
	defer c.captureMu.Unlock()
	conf := config.GetGlobalServerConfig()
	c.info = &model.CaptureInfo{
		ID:            uuid.New().String(),
		AdvertiseAddr: conf.AdvertiseAddr,
		Version:       version.ReleaseVersion,
	}
	c.processorManager = c.newProcessorManager()
	if c.session != nil {
		c.session.Close() //nolint:errcheck
	}
	sess, err := concurrency.NewSession(c.etcdClient.Client.Unwrap(),
		concurrency.WithTTL(conf.CaptureSessionTTL))
	if err != nil {
		return errors.Annotate(cerror.WrapError(cerror.ErrNewCaptureFailed, err), "create capture session")
	}
	c.session = sess
	c.election = concurrency.NewElection(sess, kv.CaptureOwnerKey)
	log.Info("init capture", zap.String("capture-id", c.info.ID), zap.String("capture-addr", c.info.AdvertiseAddr))
	return nil
}

func (c *Capture) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		err := c.reset()
		if err != nil {
			return errors.Trace(err)
		}
		err = c.run(ctx)
		// if no error returned or capture suicide, reset the capture and run again
		if err == nil || cerror.ErrCaptureSuicide.Equal(errors.Cause(err)) {
			log.Info("capture recovered", zap.String("capture-id", c.info.ID))
			continue
		}
		return errors.Trace(err)
	}
}

func (c *Capture) run(stdCtx context.Context) error {
	ctx := cdcContext.NewContext(stdCtx, &cdcContext.GlobalVars{
		PDClient:    c.pdClient,
		KVStorage:   c.kvStorage,
		CaptureInfo: c.info,
		EtcdClient:  c.etcdClient,
	})
	err := c.register(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		timeoutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := ctx.GlobalVars().EtcdClient.DeleteCaptureInfo(timeoutCtx, c.info.ID); err != nil {
			log.Warn("failed to delete capture info when capture exited", zap.Error(err))
		}
		cancel()
	}()
	wg := new(sync.WaitGroup)
	wg.Add(2)
	var ownerErr, processorErr error
	go func() {
		defer wg.Done()
		defer c.AsyncClose()
		ownerErr = c.campaignOwner(ctx)
	}()
	go func() {
		defer wg.Done()
		defer c.AsyncClose()
		conf := config.GetGlobalServerConfig()
		processorFlushInterval := time.Duration(conf.ProcessorFlushInterval)
		// no matter why the processor is exited, return to let capture restart or exit
		processorErr = c.runEtcdWorker(ctx, c.processorManager, model.NewGlobalState(), processorFlushInterval)
	}()
	go func() {
		<-ctx.Done()
		log.Info("LEOPPRO cancelled")
	}()
	wg.Wait()
	if ownerErr != nil {
		return errors.Annotate(ownerErr, "owner exited with error")
	}
	if processorErr != nil {
		return errors.Annotate(processorErr, "processor exited with error")
	}
	return nil
}

func (c *Capture) Info() model.CaptureInfo {
	c.captureMu.Lock()
	defer c.captureMu.Unlock()
	return *c.info
}

func (c *Capture) campaignOwner(ctx cdcContext.Context) error {
	// In most failure cases, we don't return error directly, just run another
	// campaign loop. We treat campaign loop as a special background routine.
	conf := config.GetGlobalServerConfig()
	ownerFlushInterval := time.Duration(conf.OwnerFlushInterval)
	failpoint.Inject("ownerFlushIntervalInject", func(val failpoint.Value) {
		ownerFlushInterval = time.Millisecond * time.Duration(val.(int))
	})
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
			// if campaign owner failed, restart capture
			return cerror.ErrCaptureSuicide.GenWithStackByArgs()
		}

		log.Info("campaign owner successfully", zap.String("capture-id", c.info.ID))
		owner := c.newOwner()
		c.setOwner(owner)
		err = c.runEtcdWorker(ctx, owner, model.NewGlobalState(), ownerFlushInterval)
		c.setOwner(nil)
		log.Info("run owner exited", zap.Error(err))
		// if owner exits, resign the owner key
		if resignErr := c.resign(ctx); resignErr != nil {
			// if regisn owner failed, return error to let capture exits
			return errors.Annotatef(resignErr, "resign owner failed, capture: %s", c.info.ID)
		}
		if err != nil {
			// for other errors, return error and let capture exits or restart
			return errors.Trace(err)
		}
		// if owner exits normally, continue the campaign loop and try to election owner again
	}
}

func (c *Capture) runEtcdWorker(ctx cdcContext.Context, reactor orchestrator.Reactor, reactorState orchestrator.ReactorState, timerInterval time.Duration) error {
	etcdWorker, err := orchestrator.NewEtcdWorker(ctx.GlobalVars().EtcdClient.Client, kv.EtcdKeyBase, reactor, reactorState)
	if err != nil {
		return errors.Trace(err)
	}
	if err := etcdWorker.Run(ctx, c.session, timerInterval); err != nil {
		log.Debug("LEOPPRO etcd worker err", zap.Error(err))
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
		lease, inErr := ctx.GlobalVars().EtcdClient.Client.TimeToLive(ctx, c.session.Lease())
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
func (c *Capture) campaign(ctx cdcContext.Context) error {
	failpoint.Inject("capture-campaign-compacted-error", func() {
		failpoint.Return(errors.Trace(mvcc.ErrCompacted))
	})
	return cerror.WrapError(cerror.ErrCaptureCampaignOwner, c.election.Campaign(ctx, c.info.ID))
}

// Resign lets a owner start a new election.
func (c *Capture) resign(ctx cdcContext.Context) error {
	failpoint.Inject("capture-resign-failed", func() {
		failpoint.Return(errors.New("capture resign failed"))
	})
	return cerror.WrapError(cerror.ErrCaptureResignOwner, c.election.Resign(ctx))
}

// register registers the capture information in etcd
func (c *Capture) register(ctx cdcContext.Context) error {
	err := ctx.GlobalVars().EtcdClient.PutCaptureInfo(ctx, c.info, c.session.Lease())
	if err != nil {
		return cerror.WrapError(cerror.ErrCaptureRegister, err)
	}
	return nil
}

// Close closes the capture by unregistering it from etcd
func (c *Capture) AsyncClose() {
	c.OperateOwnerUnderLock(func(o *owner.Owner) error {
		o.AsyncStop()
		return nil
	}) //nolint:errcheck
	c.captureMu.Lock()
	defer c.captureMu.Unlock()
	if c.processorManager != nil {
		c.processorManager.AsyncClose()
	}
}

func (c *Capture) DebugInfo() string {
	return "TODO debug info"
}

func (c *Capture) IsOwner() bool {
	return c.OperateOwnerUnderLock(func(o *owner.Owner) error {
		return nil
	}) == nil
}
