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
	"fmt"
	"io"
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
	"github.com/pingcap/ticdc/pkg/p2p"
	"github.com/pingcap/ticdc/pkg/security"
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

	ownerMu          sync.Mutex
	owner            *owner.Owner
	processorManager *processor.Manager

	// session keeps alive between the capture and etcd
	session  *concurrency.Session
	election *concurrency.Election

	pdClient   pd.Client
	kvStorage  tidbkv.Storage
	etcdClient *kv.CDCEtcdClient
	grpcPool   kv.GrpcPool

	peerMessageServer    *p2p.MessageServer
	peerMessageRouter    p2p.MessageRouter
	grpcResettableServer *p2p.ResettableServer

	cancel context.CancelFunc

	newProcessorManager func() *processor.Manager
	newOwner            func() *owner.Owner
}

// NewCapture returns a new Capture instance
func NewCapture(pdClient pd.Client, kvStorage tidbkv.Storage, etcdClient *kv.CDCEtcdClient, grpcServer *p2p.ResettableServer) *Capture {
	return &Capture{
		pdClient:             pdClient,
		kvStorage:            kvStorage,
		etcdClient:           etcdClient,
		cancel:               func() {},
		grpcResettableServer: grpcServer,

		newProcessorManager: processor.NewManager,
		newOwner:            owner.NewOwner,
	}
}

func (c *Capture) reset(ctx context.Context) error {
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
		// It can't be handled even after it fails, so we ignore it.
		_ = c.session.Close()
	}
	sess, err := concurrency.NewSession(c.etcdClient.Client.Unwrap(),
		concurrency.WithTTL(conf.CaptureSessionTTL))
	if err != nil {
		return errors.Annotate(cerror.WrapError(cerror.ErrNewCaptureFailed, err), "create capture session")
	}
	c.session = sess
	c.election = concurrency.NewElection(sess, kv.CaptureOwnerKey)
	if c.grpcPool != nil {
		c.grpcPool.Close()
	}
	c.grpcPool = kv.NewGrpcPoolImpl(ctx, conf.Security)
	log.Info("init capture", zap.String("capture-id", c.info.ID), zap.String("capture-addr", c.info.AdvertiseAddr))
	return nil
}

// Run runs the capture
func (c *Capture) Run(ctx context.Context) error {
	defer log.Info("the capture routine has exited")
	// Limit the frequency of reset capture to avoid frequent recreating of resources
	rl := rate.NewLimiter(0.05, 2)
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		ctx, cancel := context.WithCancel(ctx)
		c.cancel = cancel
		err := rl.Wait(ctx)
		if err != nil {
			if errors.Cause(err) == context.Canceled {
				return nil
			}
			return errors.Trace(err)
		}
		err = c.reset(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		err = c.run(ctx)
		log.Warn("capture exited", zap.Error(err))
		// if capture suicided, reset the capture and run again.
		// if the canceled error throw, there are two possible scenarios:
		//   1. the internal context canceled, it means some error happened in the internal, and the routine is exited, we should restart the capture
		//   2. the parent context canceled, it means that the caller of the capture hope the capture to exit, and this loop will return in the above `select` block
		// TODO: make sure the internal cancel should return the real error instead of context.Canceled
		if cerror.ErrCaptureSuicide.Equal(err) || context.Canceled == errors.Cause(err) {
			log.Info("capture recovered", zap.String("capture-id", c.info.ID))
			continue
		}
		return errors.Trace(err)
	}
}

func (c *Capture) run(stdCtx context.Context) error {
	conf := config.GetGlobalServerConfig()
	c.InitPeerMessaging(c.info.ID, conf.Security)

	ctx := cdcContext.NewContext(stdCtx, &cdcContext.GlobalVars{
		PDClient:      c.pdClient,
		KVStorage:     c.kvStorage,
		CaptureInfo:   c.info,
		EtcdClient:    c.etcdClient,
		GrpcPool:      c.grpcPool,
		MessageRouter: c.peerMessageRouter,
		MessageServer: c.peerMessageServer,
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
	c.grpcResettableServer.Reset(c.peerMessageServer)
	wg := new(sync.WaitGroup)
	wg.Add(4)
	// TODO refactor error handling here, probably using errgroup.
	var ownerErr, processorErr, messageServerErr, grpcServerErr error
	go func() {
		defer wg.Done()
		defer c.AsyncClose()
		// when the campaignOwner returns an error, it means that the owner throws an unrecoverable serious errors
		// (recoverable errors are intercepted in the owner tick)
		// so we should also stop the owner and let capture restart or exit
		ownerErr = c.campaignOwner(ctx)
		log.Info("the owner routine has exited", zap.Error(ownerErr))
	}()
	go func() {
		defer wg.Done()
		defer c.AsyncClose()
		conf := config.GetGlobalServerConfig()
		processorFlushInterval := time.Duration(conf.ProcessorFlushInterval)
		// when the etcd worker of processor returns an error, it means that the processor throws an unrecoverable serious errors
		// (recoverable errors are intercepted in the processor tick)
		// so we should also stop the processor and let capture restart or exit
		processorErr = c.runEtcdWorker(ctx, c.processorManager, c.MakeGlobalEtcdStateWithCaptureChangeEventHandler(), processorFlushInterval)
		log.Info("the processor routine has exited", zap.Error(processorErr))
	}()
	go func() {
		defer wg.Done()
		defer c.AsyncClose()
		messageServerErr = c.peerMessageServer.Run(ctx)
		log.Info("message server has exited", zap.Error(messageServerErr))
	}()
	go func() {
		defer wg.Done()
		c.grpcPool.RecycleConn(ctx)
	}()
	wg.Wait()
	if ownerErr != nil {
		return errors.Annotate(ownerErr, "owner exited with error")
	}
	if processorErr != nil {
		return errors.Annotate(processorErr, "processor exited with error")
	}
	if messageServerErr != nil {
		return errors.Annotate(messageServerErr, "message server exited with error")
	}
	if grpcServerErr != nil {
		return errors.Annotate(grpcServerErr, "grpc server exited with error")
	}
	return nil
}

// Info gets the capture info
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
	// Limit the frequency of elections to avoid putting too much pressure on the etcd server
	rl := rate.NewLimiter(0.05, 2)
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
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
				// the revision we requested is compacted, just retry
				continue
			}
			log.Warn("campaign owner failed", zap.Error(err))
			// if campaign owner failed, restart capture
			return cerror.ErrCaptureSuicide.GenWithStackByArgs()
		}

		log.Info("campaign owner successfully", zap.String("capture-id", c.info.ID))
		owner := c.newOwner()
		c.setOwner(owner)

		// TODO refactor this piece of code into a function
		leaderResp, err := c.election.Leader(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		if len(leaderResp.Kvs) != 1 {
			return errors.New("unexpected response when querying about leader")
		}
		ownerRev := leaderResp.Kvs[0].CreateRevision
		ctx.GlobalVars().OwnerRev = ownerRev
		err = c.runEtcdWorker(ctx, owner, c.MakeGlobalEtcdStateWithCaptureChangeEventHandler(), ownerFlushInterval)
		ctx.GlobalVars().OwnerRev = 0
		c.setOwner(nil)
		log.Info("run owner exited", zap.Error(err))
		// if owner exits, resign the owner key
		if resignErr := c.resign(ctx); resignErr != nil {
			log.Warn("failed to resign owner. Probably the cluster has been reset", zap.Error(err))
		}
		if err != nil {
			// for errors, return error and let capture exits or restart
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
		// We check ttl of lease instead of check `session.Done`, because
		// `session.Done` is only notified when etcd client establish a
		// new keepalive request, there could be a time window as long as
		// 1/3 of session ttl that `session.Done` can't be triggered even
		// the lease is already revoked.
		switch {
		case cerror.ErrEtcdSessionDone.Equal(err),
			cerror.ErrLeaseExpired.Equal(err):
			log.Warn("session is disconnected", zap.Error(err))
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

// OperateOwnerUnderLock operates the owner with lock
func (c *Capture) OperateOwnerUnderLock(fn func(*owner.Owner) error) error {
	c.ownerMu.Lock()
	defer c.ownerMu.Unlock()
	if c.owner == nil {
		return cerror.ErrNotOwner.GenWithStackByArgs()
	}
	return fn(c.owner)
}

// campaign to be an owner.
func (c *Capture) campaign(ctx cdcContext.Context) error {
	failpoint.Inject("capture-campaign-compacted-error", func() {
		failpoint.Return(errors.Trace(mvcc.ErrCompacted))
	})
	return cerror.WrapError(cerror.ErrCaptureCampaignOwner, c.election.Campaign(ctx, c.info.ID))
}

// resign lets an owner start a new election.
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

// AsyncClose closes the capture by unregistering it from etcd
func (c *Capture) AsyncClose() {
	defer c.cancel()

	log.Info("async close", zap.Stack("stack"))

	// Safety: Here we mainly want to stop the owner
	// and ignore it if the owner does not exist or is not set.
	_ = c.OperateOwnerUnderLock(func(o *owner.Owner) error {
		o.AsyncStop()
		return nil
	})
	c.captureMu.Lock()
	defer c.captureMu.Unlock()
	if c.processorManager != nil {
		c.processorManager.AsyncClose()
	}
	if c.grpcPool != nil {
		c.grpcPool.Close()
	}
	c.grpcResettableServer.Reset(nil)
}

// WriteDebugInfo writes the debug info into writer.
func (c *Capture) WriteDebugInfo(w io.Writer) {
	// Safety: Because we are mainly outputting information about the owner here,
	// if the owner does not exist or is not set, the information will not be output.
	_ = c.OperateOwnerUnderLock(func(o *owner.Owner) error {
		fmt.Fprintf(w, "\n\n*** owner info ***:\n\n")
		o.WriteDebugInfo(w)
		return nil
	})
	c.captureMu.Lock()
	defer c.captureMu.Unlock()
	if c.processorManager != nil {
		fmt.Fprintf(w, "\n\n*** processors info ***:\n\n")
		c.processorManager.WriteDebugInfo(w)
	}
}

// IsOwner returns whether the capture is an owner
func (c *Capture) IsOwner() bool {
	c.ownerMu.Lock()
	defer c.ownerMu.Unlock()
	return c.owner != nil
}

// GetOwner return the owner of current TiCDC cluster
func (c *Capture) GetOwner(ctx context.Context) (*model.CaptureInfo, error) {
	_, captureInfos, err := c.etcdClient.GetCaptures(ctx)
	if err != nil {
		return nil, err
	}

	ownerID, err := c.etcdClient.GetOwnerID(ctx, kv.CaptureOwnerKey)
	if err != nil {
		return nil, err
	}

	for _, captureInfo := range captureInfos {
		if captureInfo.ID == ownerID {
			return captureInfo, nil
		}
	}
	return nil, cerror.ErrOwnerNotFound.FastGenByArgs()
}

func (c *Capture) InitPeerMessaging(captureID model.CaptureID, credentials *security.Credential) {
	c.peerMessageServer = p2p.NewMessageServer(p2p.SenderID(captureID))
	c.peerMessageRouter = p2p.NewMessageRouter(p2p.SenderID(captureID), credentials)
}

func (c *Capture) MakeGlobalEtcdStateWithCaptureChangeEventHandler() *model.GlobalReactorState {
	ret := model.NewGlobalState()
	ret.SetUpdateCaptureFn(func(id model.CaptureID, addr string) {
		if addr == "" {
			c.peerMessageRouter.RemoveSenderID(p2p.SenderID(id))
			return
		}
		c.peerMessageRouter.AddSenderID(p2p.SenderID(id), addr)
	})
	return ret
}
