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
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tiflow/cdc/kv"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/owner"
	"github.com/pingcap/tiflow/cdc/processor"
	"github.com/pingcap/tiflow/cdc/processor/pipeline/system"
	ssystem "github.com/pingcap/tiflow/cdc/sorter/leveldb/system"
	"github.com/pingcap/tiflow/pkg/config"
	cdcContext "github.com/pingcap/tiflow/pkg/context"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"github.com/pingcap/tiflow/pkg/p2p"
	"github.com/pingcap/tiflow/pkg/pdtime"
	"github.com/pingcap/tiflow/pkg/version"
	"github.com/tikv/client-go/v2/tikv"
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

	PDClient     pd.Client
	Storage      tidbkv.Storage
	EtcdClient   *etcd.CDCEtcdClient
	grpcPool     kv.GrpcPool
	regionCache  *tikv.RegionCache
	pdClock      *pdtime.PDClock
	sorterSystem *ssystem.System

	enableNewScheduler bool
	tableActorSystem   *system.System

	// MessageServer is the receiver of the messages from the other nodes.
	// It should be recreated each time the capture is restarted.
	MessageServer *p2p.MessageServer

	// MessageRouter manages the clients to send messages to all peers.
	MessageRouter p2p.MessageRouter

	// grpcService is a wrapper that can hold a MessageServer.
	// The instance should last for the whole life of the server,
	// regardless of server restarting.
	// This design is to solve the problem that grpc-go cannot gracefully
	// unregister a service.
	grpcService *p2p.ServerWrapper

	cancel context.CancelFunc

	newProcessorManager func() *processor.Manager
	newOwner            func(pd.Client) *owner.Owner
}

// NewCapture returns a new Capture instance
func NewCapture(pdClient pd.Client, kvStorage tidbkv.Storage, etcdClient *etcd.CDCEtcdClient, grpcService *p2p.ServerWrapper) *Capture {
	conf := config.GetGlobalServerConfig()
	return &Capture{
		PDClient:    pdClient,
		Storage:     kvStorage,
		EtcdClient:  etcdClient,
		grpcService: grpcService,
		cancel:      func() {},

		enableNewScheduler:  conf.Debug.EnableNewScheduler,
		newProcessorManager: processor.NewManager,
		newOwner:            owner.NewOwner,
	}
}

func NewCapture4Test(isOwner bool) *Capture {
	res := &Capture{
		info: &model.CaptureInfo{ID: "capture-for-test", AdvertiseAddr: "127.0.0.1", Version: "test"},
	}
	if isOwner {
		res.owner = &owner.Owner{}
	}
	return res
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
	sess, err := concurrency.NewSession(c.EtcdClient.Client.Unwrap(),
		concurrency.WithTTL(conf.CaptureSessionTTL))
	if err != nil {
		return errors.Annotate(
			cerror.WrapError(cerror.ErrNewCaptureFailed, err),
			"create capture session")
	}
	c.session = sess
	c.election = concurrency.NewElection(sess, etcd.CaptureOwnerKey)

	if c.pdClock != nil {
		c.pdClock.Stop()
	}
	c.pdClock = pdtime.NewClock(c.PDClient)

	if c.tableActorSystem != nil {
		err := c.tableActorSystem.Stop()
		if err != nil {
			log.Warn("stop table actor system failed", zap.Error(err))
		}
	}
	if conf.Debug.EnableTableActor {
		c.tableActorSystem = system.NewSystem()
		err = c.tableActorSystem.Start(ctx)
		if err != nil {
			return errors.Annotate(
				cerror.WrapError(cerror.ErrNewCaptureFailed, err),
				"create table actor system")
		}
	}
	if conf.Debug.EnableDBSorter {
		if c.sorterSystem != nil {
			err := c.sorterSystem.Stop()
			if err != nil {
				log.Warn("stop sorter system failed", zap.Error(err))
			}
		}
		// Sorter dir has been set and checked when server starts.
		// See https://github.com/pingcap/tiflow/blob/9dad09/cdc/server.go#L275
		sortDir := config.GetGlobalServerConfig().Sorter.SortDir
		c.sorterSystem = ssystem.NewSystem(sortDir, conf.Debug.DB)
		err = c.sorterSystem.Start(ctx)
		if err != nil {
			return errors.Annotate(
				cerror.WrapError(cerror.ErrNewCaptureFailed, err),
				"create sorter system")
		}
	}
	if c.grpcPool != nil {
		c.grpcPool.Close()
	}

	if c.enableNewScheduler {
		c.grpcService.Reset(nil)

		if c.MessageRouter != nil {
			c.MessageRouter.Close()
			c.MessageRouter.Wait()
			c.MessageRouter = nil
		}
	}

	c.grpcPool = kv.NewGrpcPoolImpl(ctx, conf.Security)
	if c.regionCache != nil {
		c.regionCache.Close()
	}
	c.regionCache = tikv.NewRegionCache(c.PDClient)

	if c.enableNewScheduler {
		messageServerConfig := conf.Debug.Messages.ToMessageServerConfig()
		c.MessageServer = p2p.NewMessageServer(c.info.ID, messageServerConfig)
		c.grpcService.Reset(c.MessageServer)

		messageClientConfig := conf.Debug.Messages.ToMessageClientConfig()

		// Puts the advertise-addr of the local node to the client config.
		// This is for metrics purpose only, so that the receiver knows which
		// node the connections are from.
		advertiseAddr := conf.AdvertiseAddr
		messageClientConfig.AdvertisedAddr = advertiseAddr

		c.MessageRouter = p2p.NewMessageRouter(c.info.ID, conf.Security, messageClientConfig)
	}

	log.Info("init capture",
		zap.String("captureID", c.info.ID),
		zap.String("captureAddr", c.info.AdvertiseAddr))
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
		// if capture suicided, reset the capture and run again.
		// if the canceled error throw, there are two possible scenarios:
		//   1. the internal context canceled, it means some error happened in the internal, and the routine is exited, we should restart the capture
		//   2. the parent context canceled, it means that the caller of the capture hope the capture to exit, and this loop will return in the above `select` block
		// TODO: make sure the internal cancel should return the real error instead of context.Canceled
		if cerror.ErrCaptureSuicide.Equal(err) || context.Canceled == errors.Cause(err) {
			log.Info("capture recovered", zap.String("captureID", c.info.ID))
			continue
		}
		return errors.Trace(err)
	}
}

func (c *Capture) run(stdCtx context.Context) error {
	ctx := cdcContext.NewContext(stdCtx, &cdcContext.GlobalVars{
		PDClient:         c.PDClient,
		KVStorage:        c.Storage,
		CaptureInfo:      c.info,
		EtcdClient:       c.EtcdClient,
		GrpcPool:         c.grpcPool,
		RegionCache:      c.regionCache,
		PDClock:          c.pdClock,
		TableActorSystem: c.tableActorSystem,
		SorterSystem:     c.sorterSystem,
		MessageServer:    c.MessageServer,
		MessageRouter:    c.MessageRouter,
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
	var ownerErr, processorErr, messageServerErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer c.AsyncClose()
		// when the campaignOwner returns an error, it means that the owner throws an unrecoverable serious errors
		// (recoverable errors are intercepted in the owner tick)
		// so we should also stop the owner and let capture restart or exit
		ownerErr = c.campaignOwner(ctx)
		log.Info("the owner routine has exited", zap.Error(ownerErr))
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer c.AsyncClose()
		conf := config.GetGlobalServerConfig()
		processorFlushInterval := time.Duration(conf.ProcessorFlushInterval)

		globalState := orchestrator.NewGlobalState()

		if c.enableNewScheduler {
			globalState.SetOnCaptureAdded(func(captureID model.CaptureID, addr string) {
				c.MessageRouter.AddPeer(captureID, addr)
			})
			globalState.SetOnCaptureRemoved(func(captureID model.CaptureID) {
				c.MessageRouter.RemovePeer(captureID)
			})
		}

		// when the etcd worker of processor returns an error, it means that the processor throws an unrecoverable serious errors
		// (recoverable errors are intercepted in the processor tick)
		// so we should also stop the processor and let capture restart or exit
		processorErr = c.runEtcdWorker(ctx, c.processorManager, globalState, processorFlushInterval, "processor")
		log.Info("the processor routine has exited", zap.Error(processorErr))
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.pdClock.Run(ctx)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.grpcPool.RecycleConn(ctx)
	}()
	if c.enableNewScheduler {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer c.AsyncClose()
			defer c.grpcService.Reset(nil)
			messageServerErr = c.MessageServer.Run(ctx)
		}()
	}
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

		ownerRev, err := c.EtcdClient.GetOwnerRevision(ctx, c.info.ID)
		if err != nil {
			if errors.Cause(err) == context.Canceled {
				return nil
			}
			return errors.Trace(err)
		}

		// We do a copy of the globalVars here to avoid
		// accidental modifications and potential race conditions.
		globalVars := *ctx.GlobalVars()
		newGlobalVars := &globalVars
		newGlobalVars.OwnerRevision = ownerRev
		ownerCtx := cdcContext.NewContext(ctx, newGlobalVars)

		log.Info("campaign owner successfully",
			zap.String("captureID", c.info.ID),
			zap.Int64("ownerRev", ownerRev))

		owner := c.newOwner(c.PDClient)
		c.setOwner(owner)

		globalState := orchestrator.NewGlobalState()

		if c.enableNewScheduler {
			globalState.SetOnCaptureAdded(func(captureID model.CaptureID, addr string) {
				c.MessageRouter.AddPeer(captureID, addr)
			})
			globalState.SetOnCaptureRemoved(func(captureID model.CaptureID) {
				c.MessageRouter.RemovePeer(captureID)
			})
		}

		err = c.runEtcdWorker(ownerCtx, owner, orchestrator.NewGlobalState(), ownerFlushInterval, "owner")
		c.setOwner(nil)
		log.Info("run owner exited", zap.Error(err))
		// if owner exits, resign the owner key
		if resignErr := c.resign(ctx); resignErr != nil {
			// if resigning owner failed, return error to let capture exits
			return errors.Annotatef(resignErr, "resign owner failed, capture: %s", c.info.ID)
		}
		if err != nil {
			// for errors, return error and let capture exits or restart
			return errors.Trace(err)
		}
		// if owner exits normally, continue the campaign loop and try to election owner again
	}
}

func (c *Capture) runEtcdWorker(
	ctx cdcContext.Context,
	reactor orchestrator.Reactor,
	reactorState orchestrator.ReactorState,
	timerInterval time.Duration,
	role string,
) error {
	etcdWorker, err := orchestrator.NewEtcdWorker(ctx.GlobalVars().EtcdClient.Client, etcd.EtcdKeyBase, reactor, reactorState)
	if err != nil {
		return errors.Trace(err)
	}
	captureAddr := c.info.AdvertiseAddr
	if err := etcdWorker.Run(ctx, c.session, timerInterval, captureAddr, role); err != nil {
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
// Note: this function should be reentrant
func (c *Capture) AsyncClose() {
	defer c.cancel()
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
	if c.regionCache != nil {
		c.regionCache.Close()
		c.regionCache = nil
	}
	if c.tableActorSystem != nil {
		err := c.tableActorSystem.Stop()
		if err != nil {
			log.Warn("stop table actor system failed", zap.Error(err))
		}
		c.tableActorSystem = nil
	}
	if c.sorterSystem != nil {
		err := c.sorterSystem.Stop()
		if err != nil {
			log.Warn("stop sorter system failed", zap.Error(err))
		}
		c.sorterSystem = nil
	}
	if c.enableNewScheduler {
		c.grpcService.Reset(nil)

		if c.MessageRouter != nil {
			c.MessageRouter.Close()
			c.MessageRouter.Wait()
			c.MessageRouter = nil
		}
	}
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
	_, captureInfos, err := c.EtcdClient.GetCaptures(ctx)
	if err != nil {
		return nil, err
	}

	ownerID, err := c.EtcdClient.GetOwnerID(ctx, etcd.CaptureOwnerKey)
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

// StatusProvider returns owner's StatusProvider.
func (c *Capture) StatusProvider() owner.StatusProvider {
	c.ownerMu.Lock()
	defer c.ownerMu.Unlock()
	if c.owner == nil {
		return nil
	}
	return c.owner.StatusProvider()
}
