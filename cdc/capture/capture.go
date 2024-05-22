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
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/controller"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/owner"
	"github.com/pingcap/tiflow/cdc/processor"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/engine/factory"
	"github.com/pingcap/tiflow/pkg/config"
	cdcContext "github.com/pingcap/tiflow/pkg/context"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/migrate"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"github.com/pingcap/tiflow/pkg/p2p"
	"github.com/pingcap/tiflow/pkg/upstream"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/pingcap/tiflow/pkg/version"
	"github.com/pingcap/tiflow/pkg/workerpool"
	pd "github.com/tikv/pd/client"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.etcd.io/etcd/server/v3/mvcc"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

const (
	cleanMetaDuration = 10 * time.Second
	// changefeedAsyncInitWorkerCount is the size of the worker pool for changefeed initialization processing.
	changefeedAsyncInitWorkerCount = 8
)

// Capture represents a Capture server, it monitors the changefeed
// information in etcd and schedules Task on it.
type Capture interface {
	Run(ctx context.Context) error
	Close()
	Drain() <-chan struct{}
	Liveness() model.Liveness

	GetOwner() (owner.Owner, error)
	GetController() (controller.Controller, error)
	GetControllerCaptureInfo(ctx context.Context) (*model.CaptureInfo, error)
	IsController() bool

	Info() (model.CaptureInfo, error)
	StatusProvider() owner.StatusProvider
	WriteDebugInfo(ctx context.Context, w io.Writer)

	GetUpstreamManager() (*upstream.Manager, error)
	GetEtcdClient() etcd.CDCEtcdClient
	// IsReady returns if the cdc server is ready
	// currently only check if ettcd data migration is done
	IsReady() bool
}

type captureImpl struct {
	// captureMu is used to protect the capture info and processorManager.
	captureMu        sync.Mutex
	info             *model.CaptureInfo
	processorManager processor.Manager
	liveness         model.Liveness
	config           *config.ServerConfig

	pdClient        pd.Client
	pdEndpoints     []string
	ownerMu         sync.Mutex
	owner           owner.Owner
	controller      controller.Controller
	upstreamManager *upstream.Manager

	// session keeps alive between the capture and etcd
	session  *concurrency.Session
	election election

	EtcdClient etcd.CDCEtcdClient

	sortEngineFactory *factory.SortEngineFactory

	// ChangefeedThreadPool is the thread pool for changefeed initialization
	ChangefeedThreadPool workerpool.AsyncPool

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

	migrator migrate.Migrator

	newProcessorManager func(
		captureInfo *model.CaptureInfo,
		upstreamManager *upstream.Manager,
		liveness *model.Liveness,
		cfg *config.SchedulerConfig,
	) processor.Manager
	newOwner      func(upstreamManager *upstream.Manager, cfg *config.SchedulerConfig) owner.Owner
	newController func(upstreamManager *upstream.Manager, captureInfo *model.CaptureInfo, client etcd.CDCEtcdClient) controller.Controller
}

// NewCapture returns a new Capture instance
func NewCapture(pdEndpoints []string,
	etcdClient etcd.CDCEtcdClient,
	grpcService *p2p.ServerWrapper,
	sortEngineMangerFactory *factory.SortEngineFactory,
	pdClient pd.Client,
) Capture {
	conf := config.GetGlobalServerConfig()
	return &captureImpl{
		config:              config.GetGlobalServerConfig(),
		liveness:            model.LivenessCaptureAlive,
		EtcdClient:          etcdClient,
		grpcService:         grpcService,
		cancel:              func() {},
		pdEndpoints:         pdEndpoints,
		newProcessorManager: processor.NewManager,
		newOwner:            owner.NewOwner,
		newController:       controller.NewController,
		info:                &model.CaptureInfo{},
		sortEngineFactory:   sortEngineMangerFactory,
		migrator:            migrate.NewMigrator(etcdClient, pdEndpoints, conf),
		pdClient:            pdClient,
	}
}

// NewCapture4Test returns a new Capture instance for test.
func NewCapture4Test(o owner.Owner) *captureImpl {
	res := &captureImpl{
		info: &model.CaptureInfo{
			ID:            "capture-for-test",
			AdvertiseAddr: "127.0.0.1",
			Version:       "test",
		},
		migrator: &migrate.NoOpMigrator{},
		config:   config.GetGlobalServerConfig(),
	}
	res.owner = o
	return res
}

// NewCaptureWithController4Test returns a new Capture instance for test.
func NewCaptureWithController4Test(o owner.Owner,
	manager controller.Controller,
) *captureImpl {
	res := &captureImpl{
		info: &model.CaptureInfo{
			ID:            "capture-for-test",
			AdvertiseAddr: "127.0.0.1",
			Version:       "test",
		},
		migrator: &migrate.NoOpMigrator{},
		config:   config.GetGlobalServerConfig(),
	}
	res.controller = manager
	res.owner = o
	return res
}

// NewCaptureWithManager4Test returns a new Capture instance for test.
func NewCaptureWithManager4Test(o owner.Owner, m *upstream.Manager) *captureImpl {
	res := &captureImpl{
		upstreamManager: m,
		migrator:        &migrate.NoOpMigrator{},
	}
	res.owner = o
	return res
}

// GetUpstreamManager is a Getter of capture's upstream manager
func (c *captureImpl) GetUpstreamManager() (*upstream.Manager, error) {
	if c.upstreamManager == nil {
		return nil, cerror.ErrUpstreamManagerNotReady
	}
	return c.upstreamManager, nil
}

func (c *captureImpl) GetEtcdClient() etcd.CDCEtcdClient {
	return c.EtcdClient
}

// reset the capture before run it.
func (c *captureImpl) reset(ctx context.Context) error {
	lease, err := c.EtcdClient.GetEtcdClient().Grant(ctx, int64(c.config.CaptureSessionTTL))
	if err != nil {
		return errors.Trace(err)
	}
	sess, err := concurrency.NewSession(
		c.EtcdClient.GetEtcdClient().Unwrap(), concurrency.WithLease(lease.ID))
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("reset session successfully", zap.Any("session", sess))

	c.captureMu.Lock()
	defer c.captureMu.Unlock()
	c.info = &model.CaptureInfo{
		ID:            uuid.New().String(),
		AdvertiseAddr: c.config.AdvertiseAddr,
		Version:       version.ReleaseVersion,
	}

	if c.upstreamManager != nil {
		c.upstreamManager.Close()
	}
	c.upstreamManager = upstream.NewManager(ctx, c.EtcdClient.GetGCServiceID())
	_, err = c.upstreamManager.AddDefaultUpstream(c.pdEndpoints, c.config.Security, c.pdClient)
	if err != nil {
		return errors.Trace(err)
	}

	c.processorManager = c.newProcessorManager(
		c.info, c.upstreamManager, &c.liveness, c.config.Debug.Scheduler)
	if c.session != nil {
		// It can't be handled even after it fails, so we ignore it.
		_ = c.session.Close()
	}
	c.session = sess
	c.election = newElection(sess, etcd.CaptureOwnerKey(c.EtcdClient.GetClusterID()))

	c.grpcService.Reset(nil)

	if c.MessageRouter != nil {
		c.MessageRouter.Close()
		c.MessageRouter = nil
	}
	messageServerConfig := c.config.Debug.Messages.ToMessageServerConfig()
	c.MessageServer = p2p.NewMessageServer(c.info.ID, messageServerConfig)
	c.grpcService.Reset(c.MessageServer)

	messageClientConfig := c.config.Debug.Messages.ToMessageClientConfig()

	// Puts the advertise-addr of the local node to the client config.
	// This is for metrics purpose only, so that the receiver knows which
	// node the connections are from.
	advertiseAddr := c.config.AdvertiseAddr
	messageClientConfig.AdvertisedAddr = advertiseAddr

	c.MessageRouter = p2p.NewMessageRouterWithLocalClient(c.info.ID, c.config.Security, messageClientConfig)
	c.ChangefeedThreadPool = workerpool.NewDefaultAsyncPool(changefeedAsyncInitWorkerCount)
	log.Info("capture initialized", zap.Any("capture", c.info))
	return nil
}

// Run runs the capture
func (c *captureImpl) Run(ctx context.Context) error {
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
		err = c.run(ctx)
		// if capture suicided, reset the capture and run again.
		// if the canceled error throw, there are two possible scenarios:
		//   1. the internal context canceled, it means some error happened in
		//      the internal, and the routine is exited, we should restart
		//      the capture.
		//   2. the parent context canceled, it means that the caller of
		//      the capture hope the capture to exit, and this loop will return
		//      in the above `select` block.
		// if there are some **internal** context deadline exceeded (IO/network
		// timeout), reset the capture and run again.
		//
		// TODO: make sure the internal cancel should return the real error
		//       instead of context.Canceled.
		if cerror.ErrCaptureSuicide.Equal(err) ||
			context.Canceled == errors.Cause(err) ||
			context.DeadlineExceeded == errors.Cause(err) {
			log.Info("capture recovered", zap.String("captureID", c.info.ID))
			continue
		}
		return errors.Trace(err)
	}
}

func (c *captureImpl) run(stdCtx context.Context) error {
	err := c.reset(stdCtx)
	if err != nil {
		log.Error("reset capture failed", zap.Error(err))
		return errors.Trace(err)
	}

	err = c.register(stdCtx)
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		timeoutCtx, cancel := context.WithTimeout(context.Background(), cleanMetaDuration)
		if err := c.EtcdClient.DeleteCaptureInfo(timeoutCtx, c.info.ID); err != nil {
			log.Warn("failed to delete capture info when capture exited",
				zap.String("captureID", c.info.ID),
				zap.Error(err))
		}
		cancel()
	}()

	defer func() {
		c.Close()
		c.grpcService.Reset(nil)
	}()

	g, stdCtx := errgroup.WithContext(stdCtx)
	stdCtx, cancel := context.WithCancel(stdCtx)

	ctx := cdcContext.NewContext(stdCtx, &cdcContext.GlobalVars{
		CaptureInfo:          c.info,
		EtcdClient:           c.EtcdClient,
		MessageServer:        c.MessageServer,
		MessageRouter:        c.MessageRouter,
		SortEngineFactory:    c.sortEngineFactory,
		ChangefeedThreadPool: c.ChangefeedThreadPool,
	})
	g.Go(func() error {
		// when the campaignOwner returns an error, it means that the owner throws
		// an unrecoverable serious errors (recoverable errors are intercepted in the owner tick)
		// so we should restart the capture.
		err := c.campaignOwner(ctx)
		if err != nil || c.liveness.Load() != model.LivenessCaptureStopping {
			log.Warn("campaign owner routine exited, restart the capture",
				zap.String("captureID", c.info.ID), zap.Error(err))
			// Throw ErrCaptureSuicide to restart capture.
			return cerror.ErrCaptureSuicide.FastGenByArgs()
		}
		return nil
	})

	g.Go(func() error {
		// Processor manager should be closed as soon as possible to prevent double write issue.
		defer func() {
			if cancel != nil {
				// Propagate the cancel signal to the owner and other goroutines.
				cancel()
			}
			if c.processorManager != nil {
				c.processorManager.Close()
			}
			log.Info("processor manager closed", zap.String("captureID", c.info.ID))
		}()
		processorFlushInterval := time.Duration(c.config.ProcessorFlushInterval)

		globalState := orchestrator.NewGlobalState(c.EtcdClient.GetClusterID(), c.config.CaptureSessionTTL)

		globalState.SetOnCaptureAdded(func(captureID model.CaptureID, addr string) {
			c.MessageRouter.AddPeer(captureID, addr)
		})
		globalState.SetOnCaptureRemoved(func(captureID model.CaptureID) {
			c.MessageRouter.RemovePeer(captureID)
		})

		// when the etcd worker of processor returns an error, it means that the processor throws an unrecoverable serious errors
		// (recoverable errors are intercepted in the processor tick)
		// so we should also stop the processor and let capture restart or exit
		err := c.runEtcdWorker(ctx, c.processorManager, globalState, processorFlushInterval, util.RoleProcessor.String())
		log.Info("processor routine exited",
			zap.String("captureID", c.info.ID), zap.Error(err))
		return err
	})

	g.Go(func() error {
		return c.MessageServer.Run(ctx, c.MessageRouter.GetLocalChannel())
	})

	poolCtx, cancelPool := context.WithCancel(stdCtx)
	defer func() {
		cancelPool()
		log.Info("workerpool exited", zap.Error(err))
	}()
	g.Go(func() error {
		return c.ChangefeedThreadPool.Run(poolCtx)
	})
	return errors.Trace(g.Wait())
}

// Info gets the capture info
func (c *captureImpl) Info() (model.CaptureInfo, error) {
	c.captureMu.Lock()
	defer c.captureMu.Unlock()
	// when c.reset has not been called yet, c.info is nil.
	if c.info != nil {
		return *c.info, nil
	}
	return model.CaptureInfo{}, cerror.ErrCaptureNotInitialized.GenWithStackByArgs()
}

func (c *captureImpl) campaignOwner(ctx cdcContext.Context) error {
	// In most failure cases, we don't return error directly, just run another
	// campaign loop. We treat campaign loop as a special background routine.
	ownerFlushInterval := time.Duration(c.config.OwnerFlushInterval)
	failpoint.Inject("ownerFlushIntervalInject", func(val failpoint.Value) {
		ownerFlushInterval = time.Millisecond * time.Duration(val.(int))
	})
	// Limit the frequency of elections to avoid putting too much pressure on the etcd server
	rl := rate.NewLimiter(rate.Every(time.Second), 1 /* burst */)
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
		// Before campaign check liveness
		if c.liveness.Load() == model.LivenessCaptureStopping {
			log.Info("do not campaign owner, liveness is stopping",
				zap.String("captureID", c.info.ID))
			return nil
		}
		// Campaign to be the owner, it blocks until it been elected.
		if err := c.campaign(ctx); err != nil {
			rootErr := errors.Cause(err)
			if rootErr == context.Canceled {
				return nil
			} else if rootErr == mvcc.ErrCompacted || isErrCompacted(rootErr) {
				log.Warn("campaign owner failed due to etcd revision "+
					"has been compacted, retry later", zap.Error(err))
				continue
			}
			log.Warn("campaign owner failed",
				zap.String("captureID", c.info.ID), zap.Error(err))
			return cerror.ErrCaptureSuicide.GenWithStackByArgs()
		}
		// After campaign check liveness again.
		// It is possible it becomes the owner right after receiving SIGTERM.
		if c.liveness.Load() == model.LivenessCaptureStopping {
			// If the capture is stopping, resign actively.
			log.Info("resign owner actively, liveness is stopping")
			if resignErr := c.resign(ctx); resignErr != nil {
				log.Warn("resign owner actively failed",
					zap.String("captureID", c.info.ID), zap.Error(resignErr))
				return errors.Trace(err)
			}
			return nil
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

		log.Info("campaign owner successfully",
			zap.String("captureID", c.info.ID),
			zap.Int64("ownerRev", ownerRev))

		controller := c.newController(c.upstreamManager, c.info, c.EtcdClient)

		owner := c.newOwner(c.upstreamManager, c.config.Debug.Scheduler)
		c.setOwner(owner)
		c.setController(controller)

		globalState := orchestrator.NewGlobalState(c.EtcdClient.GetClusterID(), c.config.CaptureSessionTTL)

		globalState.SetOnCaptureAdded(func(captureID model.CaptureID, addr string) {
			c.MessageRouter.AddPeer(captureID, addr)
		})
		globalState.SetOnCaptureRemoved(func(captureID model.CaptureID) {
			c.MessageRouter.RemovePeer(captureID)
			// If an owner is killed by "kill -19", other CDC nodes will remove that capture,
			// but the peer in the message server will not be removed, so the message server still sends
			// ack message to that peer, until the write buffer is full. So we need to deregister the peer
			// when the capture is removed.
			if err := c.MessageServer.ScheduleDeregisterPeerTask(ctx, captureID); err != nil {
				log.Warn("deregister peer failed",
					zap.String("captureID", captureID),
					zap.Error(err))
			}
		})

		g, ctx := errgroup.WithContext(ctx)
		ownerCtx := cdcContext.NewContext(ctx, newGlobalVars)
		g.Go(func() error {
			return c.runEtcdWorker(ownerCtx, owner,
				orchestrator.NewGlobalState(c.EtcdClient.GetClusterID(), c.config.CaptureSessionTTL),
				ownerFlushInterval, util.RoleOwner.String())
		})
		g.Go(func() error {
			er := c.runEtcdWorker(ownerCtx, controller,
				globalState,
				// todo: do not use owner flush interval
				ownerFlushInterval, util.RoleController.String())
			// controller has exited, stop owner.
			c.owner.AsyncStop()
			return er
		})
		err = g.Wait()
		c.owner.AsyncStop()
		c.controller.AsyncStop()
		c.setController(nil)
		c.setOwner(nil)

		if !cerror.ErrNotOwner.Equal(err) {
			// if owner exits, resign the owner key,
			// use a new context to prevent the context from being cancelled.
			resignCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			if resignErr := c.resign(resignCtx); resignErr != nil {
				if errors.Cause(resignErr) != context.DeadlineExceeded {
					log.Info("owner resign failed", zap.String("captureID", c.info.ID),
						zap.Error(resignErr), zap.Int64("ownerRev", ownerRev))
					cancel()
					return errors.Trace(resignErr)
				}

				log.Warn("owner resign timeout", zap.String("captureID", c.info.ID),
					zap.Error(resignErr), zap.Int64("ownerRev", ownerRev))
			}
			cancel()
		}

		log.Info("owner resigned successfully",
			zap.String("captureID", c.info.ID), zap.Int64("ownerRev", ownerRev))
		if err != nil {
			log.Warn("run owner exited with error",
				zap.String("captureID", c.info.ID), zap.Int64("ownerRev", ownerRev),
				zap.Error(err))
			// for errors, return error and let capture exits or restart
			return errors.Trace(err)
		}
		// if owner exits normally, continue the campaign loop and try to election owner again
		log.Info("run owner exited normally",
			zap.String("captureID", c.info.ID), zap.Int64("ownerRev", ownerRev))
	}
}

func (c *captureImpl) runEtcdWorker(
	ctx cdcContext.Context,
	reactor orchestrator.Reactor,
	reactorState orchestrator.ReactorState,
	timerInterval time.Duration,
	role string,
) error {
	etcdWorker, err := orchestrator.NewEtcdWorker(c.EtcdClient,
		etcd.BaseKey(c.EtcdClient.GetClusterID()), reactor, reactorState, c.migrator)
	if err != nil {
		return errors.Trace(err)
	}
	if err := etcdWorker.Run(ctx, c.session, timerInterval, role); err != nil {
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
		lease, inErr := c.EtcdClient.GetEtcdClient().TimeToLive(ctx, c.session.Lease())
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

func (c *captureImpl) setOwner(owner owner.Owner) {
	c.ownerMu.Lock()
	defer c.ownerMu.Unlock()
	c.owner = owner
}

func (c *captureImpl) setController(controller controller.Controller) {
	c.ownerMu.Lock()
	defer c.ownerMu.Unlock()
	c.controller = controller
}

// GetOwner returns owner if it is the owner.
func (c *captureImpl) GetOwner() (owner.Owner, error) {
	c.ownerMu.Lock()
	defer c.ownerMu.Unlock()
	if c.owner == nil {
		return nil, cerror.ErrNotOwner.GenWithStackByArgs()
	}
	return c.owner, nil
}

// GetController returns `controller.Controller` if not nil
func (c *captureImpl) GetController() (controller.Controller, error) {
	c.ownerMu.Lock()
	defer c.ownerMu.Unlock()
	if c.owner == nil {
		return nil, cerror.ErrNotOwner.GenWithStackByArgs()
	}
	return c.controller, nil
}

// campaign to be an owner.
func (c *captureImpl) campaign(ctx context.Context) error {
	// TODO: `Campaign` will get stuck when send SIGSTOP to pd leader.
	// For `Campaign`, when send SIGSTOP to pd leader, cdc maybe call `cancel`
	// (cause by `processor routine` exit). And inside `Campaign`, the routine
	// return from `waitDeletes`(https://github.com/etcd-io/etcd/blob/main/client/v3/concurrency/election.go#L93),
	// then call `Resign`(note: use `client.Ctx`) to etcd server. But the etcd server
	// (the client connects to) has entered the STOP state, which means that
	// the server cannot process the request, but will still maintain the GRPC
	// connection. So `routine` will block 'Resign'.
	return cerror.WrapError(cerror.ErrCaptureCampaignOwner, c.election.campaign(ctx, c.info.ID))
}

// resign lets an owner start a new election.
func (c *captureImpl) resign(ctx context.Context) error {
	failpoint.Inject("capture-resign-failed", func() {
		failpoint.Return(errors.New("capture resign failed"))
	})
	if c.election == nil {
		return nil
	}
	return cerror.WrapError(cerror.ErrCaptureResignOwner, c.election.resign(ctx))
}

// register the capture by put the capture's information in etcd
func (c *captureImpl) register(ctx context.Context) error {
	err := c.EtcdClient.PutCaptureInfo(ctx, c.info, c.session.Lease())
	if err != nil {
		return cerror.WrapError(cerror.ErrCaptureRegister, err)
	}
	return nil
}

// Close closes the capture by deregister it from etcd,
// it also closes the owner and processorManager
// Note: this function should be reentrant
func (c *captureImpl) Close() {
	defer c.cancel()
	// Safety: Here we mainly want to stop the owner
	// and ignore it if the owner does not exist or is not set.
	o, _ := c.GetOwner()
	if o != nil {
		o.AsyncStop()
		log.Info("owner closed", zap.String("captureID", c.info.ID))
	}

	c.captureMu.Lock()
	defer c.captureMu.Unlock()

	c.grpcService.Reset(nil)
	if c.MessageRouter != nil {
		c.MessageRouter.Close()
		c.MessageRouter = nil
	}
	log.Info("message router closed", zap.String("captureID", c.info.ID))
}

// Drain removes tables in the current TiCDC instance.
func (c *captureImpl) Drain() <-chan struct{} {
	done := make(chan struct{})
	go func() {
		// Set liveness stopping first, no matter is the owner or not.
		// this is triggered by user manually stop the TiCDC instance by sent signals.
		// It may cost a few seconds before cdc server fully stop, set it to `stopping` to prevent
		// the capture become the leader or tables dispatched to it.
		c.liveness.Store(model.LivenessCaptureStopping)

		// if the instance is the owner, resign the ownership
		if o, _ := c.GetOwner(); o != nil {
			o.AsyncStop()
		}
		close(done)
	}()
	return done
}

// Liveness returns liveness of the capture.
func (c *captureImpl) Liveness() model.Liveness {
	return c.liveness.Load()
}

// WriteDebugInfo writes the debug info into writer.
func (c *captureImpl) WriteDebugInfo(ctx context.Context, w io.Writer) {
	wait := func(done <-chan error) {
		var err error
		select {
		case <-ctx.Done():
			err = ctx.Err()
		case err = <-done:
		}
		if err != nil {
			log.Warn("write debug info failed", zap.Error(err))
		}
	}
	// Safety: Because we are mainly outputting information about the owner here,
	// if the owner does not exist or is not set, the information will not be output.
	o, _ := c.GetOwner()
	if o != nil {
		doneOwner := make(chan error, 1)
		fmt.Fprintf(w, "\n\n*** owner info ***:\n\n")
		o.WriteDebugInfo(w, doneOwner)
		// wait the debug info printed
		wait(doneOwner)
	}

	doneM := make(chan error, 1)
	c.captureMu.Lock()
	if c.processorManager != nil {
		fmt.Fprintf(w, "\n\n*** processors info ***:\n\n")
		c.processorManager.WriteDebugInfo(ctx, w, doneM)
	}
	// NOTICE: we must release the lock before wait the debug info process down.
	// Otherwise, the capture initialization and request response will compete
	// for captureMu resulting in a deadlock.
	c.captureMu.Unlock()
	// wait the debug info printed
	wait(doneM)
}

// IsController returns whether the capture is a controller
func (c *captureImpl) IsController() bool {
	c.ownerMu.Lock()
	defer c.ownerMu.Unlock()
	return c.controller != nil
}

// GetControllerCaptureInfo return the controller capture info of current TiCDC cluster
func (c *captureImpl) GetControllerCaptureInfo(ctx context.Context) (*model.CaptureInfo, error) {
	_, captureInfos, err := c.EtcdClient.GetCaptures(ctx)
	if err != nil {
		return nil, err
	}

	ownerID, err := c.EtcdClient.GetOwnerID(ctx)
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
func (c *captureImpl) StatusProvider() owner.StatusProvider {
	c.ownerMu.Lock()
	defer c.ownerMu.Unlock()
	if c.owner == nil {
		return nil
	}
	return owner.NewStatusProvider(c.owner)
}

func (c *captureImpl) IsReady() bool {
	return c.migrator.IsMigrateDone()
}

func isErrCompacted(err error) bool {
	return strings.Contains(err.Error(), "required revision has been compacted")
}
