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
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/owner"
	"github.com/pingcap/tiflow/cdc/processor"
	"github.com/pingcap/tiflow/cdc/processor/pipeline/system"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/engine/factory"
	ssystem "github.com/pingcap/tiflow/cdc/sorter/db/system"
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
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.etcd.io/etcd/server/v3/mvcc"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

const cleanMetaDuration = 10 * time.Second

type createEtcdClientFunc func() (etcd.CDCEtcdClient, error)

// Capture represents a Capture server, it monitors the changefeed
// information in etcd and schedules Task on it.
type Capture interface {
	Run(ctx context.Context) error
	AsyncClose()
	Drain() <-chan struct{}
	Liveness() model.Liveness

	GetOwner() (owner.Owner, error)
	GetOwnerCaptureInfo(ctx context.Context) (*model.CaptureInfo, error)
	IsOwner() bool

	Info() (model.CaptureInfo, error)
	StatusProvider() owner.StatusProvider
	WriteDebugInfo(ctx context.Context, w io.Writer)

	GetUpstreamManager() (*upstream.Manager, error)
	GetEtcdClient() (etcd.CDCEtcdClient, error)
	// IsReady returns if the cdc server is ready
	// currently only check if ettcd data migration is done
	IsReady() bool
}

type captureImpl struct {
	// captureMu is used to protect the capture info, processorManager and isReset.
	captureMu        sync.Mutex
	initialized      bool
	info             *model.CaptureInfo
	processorManager processor.Manager
	liveness         model.Liveness
	config           *config.ServerConfig

	pdEndpoints     []string
	ownerMu         sync.Mutex
	owner           owner.Owner
	upstreamManager *upstream.Manager

	// session keeps alive between the capture and etcd
	session  *concurrency.Session
	election election

	// createEtcdClient used to create etcd client when capture restarts
	createEtcdClient createEtcdClientFunc
	EtcdClient       etcd.CDCEtcdClient
	tableActorSystem *system.System

	// useSortEngine indicates whether to use the new pull based sort engine or
	// the old push based sorter system. the latter will be removed after all sorter
	// have been transformed into pull based sort engine.
	useSortEngine     bool
	sorterSystem      *ssystem.System
	sortEngineFactory *factory.SortEngineFactory

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
	) processor.Manager
	newOwner func(upstreamManager *upstream.Manager) owner.Owner
}

// NewCapture returns a new Capture instance
func NewCapture(pdEndpoints []string,
	createEtcdClient createEtcdClientFunc,
	grpcService *p2p.ServerWrapper,
	tableActorSystem *system.System,
	sortEngineMangerFactory *factory.SortEngineFactory,
	sorterSystem *ssystem.System,
) Capture {
	return &captureImpl{
		config:              config.GetGlobalServerConfig(),
		liveness:            model.LivenessCaptureAlive,
		initialized:         false,
		grpcService:         grpcService,
		cancel:              func() {},
		pdEndpoints:         pdEndpoints,
		tableActorSystem:    tableActorSystem,
		newProcessorManager: processor.NewManager,
		newOwner:            owner.NewOwner,
		info:                &model.CaptureInfo{},
		createEtcdClient:    createEtcdClient,

		useSortEngine:     sortEngineMangerFactory != nil,
		sortEngineFactory: sortEngineMangerFactory,
		sorterSystem:      sorterSystem,
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
		initialized: true,
		migrator:    &migrate.NoOpMigrator{},
		config:      config.GetGlobalServerConfig(),
	}
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

func (c *captureImpl) GetEtcdClient() (etcd.CDCEtcdClient, error) {
	c.captureMu.Lock()
	defer c.captureMu.Unlock()
	if !c.initialized {
		return nil, cerror.ErrCaptureNotInitialized.GenWithStackByArgs()
	}
	return c.EtcdClient, nil
}

// reset the capture before run it.
func (c *captureImpl) reset(ctx context.Context) error {
	etcdClient, err := c.createEtcdClient()
	if err != nil {
		return cerror.WrapError(cerror.ErrNewCaptureFailed, err)
	}

	c.captureMu.Lock()
	defer c.captureMu.Unlock()
	c.info = &model.CaptureInfo{
		ID:            uuid.New().String(),
		AdvertiseAddr: c.config.AdvertiseAddr,
		Version:       version.ReleaseVersion,
	}
	c.EtcdClient = etcdClient
	c.migrator = migrate.NewMigrator(c.EtcdClient, c.pdEndpoints, c.config)

	lease, err := c.EtcdClient.GetEtcdClient().Grant(ctx, int64(c.config.CaptureSessionTTL))
	if err != nil {
		return cerror.WrapError(cerror.ErrNewCaptureFailed, err)
	}

	sess, err := concurrency.NewSession(
		c.EtcdClient.GetEtcdClient().Unwrap(),
		concurrency.WithLease(lease.ID))
	if err != nil {
		return cerror.WrapError(cerror.ErrNewCaptureFailed, err)
	}

	if c.upstreamManager != nil {
		c.upstreamManager.Close()
	}
	c.upstreamManager = upstream.NewManager(ctx, c.EtcdClient.GetGCServiceID())
	_, err = c.upstreamManager.AddDefaultUpstream(c.pdEndpoints, c.config.Security)
	if err != nil {
		return cerror.WrapError(cerror.ErrNewCaptureFailed, err)
	}

	c.processorManager = c.newProcessorManager(c.info, c.upstreamManager, &c.liveness)
	if c.session != nil {
		// It can't be handled even after it fails, so we ignore it.
		_ = c.session.Close()
	}
	c.session = sess
	c.election = newElection(sess, etcd.CaptureOwnerKey(c.EtcdClient.GetClusterID()))

	c.grpcService.Reset(nil)

	if c.MessageRouter != nil {
		c.MessageRouter.Close()
		c.MessageRouter.Wait()
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

	c.MessageRouter = p2p.NewMessageRouter(c.info.ID, c.config.Security, messageClientConfig)

	c.initialized = true
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
		err = c.reset(ctx)
		if err != nil {
			log.Error("reset capture failed", zap.Error(err))
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

func (c *captureImpl) run(stdCtx context.Context) error {
	err := c.register(stdCtx)
	if err != nil {
		return errors.Trace(err)
	}

	defer func() {
		// Before call `AsyncClose`, we must wait all routine to exit.
		// So we close etcd client safely.
		c.AsyncClose()
		c.grpcService.Reset(nil)
	}()

	g, stdCtx := errgroup.WithContext(stdCtx)
	ctx := cdcContext.NewContext(stdCtx, &cdcContext.GlobalVars{
		CaptureInfo:       c.info,
		EtcdClient:        c.EtcdClient,
		TableActorSystem:  c.tableActorSystem,
		MessageServer:     c.MessageServer,
		MessageRouter:     c.MessageRouter,
		SorterSystem:      c.sorterSystem,
		SortEngineFactory: c.sortEngineFactory,
	})

	g.Go(func() error {
		// when the campaignOwner returns an error, it means that the owner throws
		// an unrecoverable serious errors (recoverable errors are intercepted in the owner tick)
		// so we should restart the capture.
		err := c.campaignOwner(ctx)
		if err != nil {
			log.Error("campaign owner routine exited with error, restart the capture",
				zap.String("captureID", c.info.ID), zap.Error(err))
		} else {
			log.Info("campaign owner routine exited, restart the capture",
				zap.String("captureID", c.info.ID))
		}
		// If we throw an ErrCaptureSuicide error, the capture will restart.
		return cerror.ErrCaptureSuicide.FastGenByArgs()
	})

	g.Go(func() error {
		processorFlushInterval := time.Duration(c.config.ProcessorFlushInterval)

		globalState := orchestrator.NewGlobalState(c.EtcdClient.GetClusterID())

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
		return c.MessageServer.Run(ctx)
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
			switch errors.Cause(err) {
			case context.Canceled:
				return nil
			case mvcc.ErrCompacted:
				// the revision we requested is compacted, just retry
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
		ownerCtx := cdcContext.NewContext(ctx, newGlobalVars)

		log.Info("campaign owner successfully",
			zap.String("captureID", c.info.ID),
			zap.Int64("ownerRev", ownerRev))

		owner := c.newOwner(c.upstreamManager)
		c.setOwner(owner)

		globalState := orchestrator.NewGlobalState(c.EtcdClient.GetClusterID())

		globalState.SetOnCaptureAdded(func(captureID model.CaptureID, addr string) {
			c.MessageRouter.AddPeer(captureID, addr)
		})
		globalState.SetOnCaptureRemoved(func(captureID model.CaptureID) {
			c.MessageRouter.RemovePeer(captureID)
		})

		err = c.runEtcdWorker(ownerCtx, owner,
			orchestrator.NewGlobalState(c.EtcdClient.GetClusterID()),
			ownerFlushInterval, util.RoleOwner.String())
		c.owner.AsyncStop()
		c.setOwner(nil)

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
		// before a new cycle starts, we need resign
		if err := c.resign(ctx); err != nil {
			log.Info("owner resign failed", zap.String("captureID", c.info.ID),
				zap.Error(err), zap.Int64("ownerRev", ownerRev))
			return err
		}
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

// GetOwner returns owner if it is the owner.
func (c *captureImpl) GetOwner() (owner.Owner, error) {
	c.ownerMu.Lock()
	defer c.ownerMu.Unlock()
	if c.owner == nil {
		return nil, cerror.ErrNotOwner.GenWithStackByArgs()
	}
	return c.owner, nil
}

// campaign to be an owner.
func (c *captureImpl) campaign(ctx context.Context) error {
	failpoint.Inject("capture-campaign-compacted-error", func() {
		failpoint.Return(errors.Trace(mvcc.ErrCompacted))
	})
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

// AsyncClose closes the capture by deregister it from etcd
// Note: this function should be reentrant
func (c *captureImpl) AsyncClose() {
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
	if c.processorManager != nil {
		c.processorManager.AsyncClose()
	}
	log.Info("processor manager closed", zap.String("captureID", c.info.ID))

	c.grpcService.Reset(nil)
	if c.MessageRouter != nil {
		c.MessageRouter.Close()
		c.MessageRouter.Wait()
		c.MessageRouter = nil
	}
	log.Info("message router closed", zap.String("captureID", c.info.ID))

	if c.EtcdClient != nil {
		timeoutCtx, cancel := context.WithTimeout(context.Background(), cleanMetaDuration)
		if err := c.resign(timeoutCtx); err != nil {
			log.Warn("failed to regin", zap.String("captureID", c.info.ID),
				zap.Error(err))
		}
		cancel()

		timeoutCtx, cancel = context.WithTimeout(context.Background(), cleanMetaDuration)
		if err := c.EtcdClient.DeleteCaptureInfo(timeoutCtx, c.info.ID); err != nil {
			log.Warn("failed to delete capture info when capture exited",
				zap.String("captureID", c.info.ID),
				zap.Error(err))
		}
		cancel()

		err := c.EtcdClient.Close()
		if err != nil {
			log.Warn("failed to close etcd client", zap.Error(err))
		}
		c.EtcdClient = nil
		c.migrator = nil
	}
	c.initialized = false
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

// IsOwner returns whether the capture is an owner
func (c *captureImpl) IsOwner() bool {
	c.ownerMu.Lock()
	defer c.ownerMu.Unlock()
	return c.owner != nil
}

// GetOwnerCaptureInfo return the owner capture info of current TiCDC cluster
func (c *captureImpl) GetOwnerCaptureInfo(ctx context.Context) (*model.CaptureInfo, error) {
	c.ownerMu.Lock()
	defer c.ownerMu.Unlock()
	if !c.initialized {
		return nil, cerror.ErrCaptureNotInitialized.GenWithStackByArgs()
	}

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
	c.captureMu.Lock()
	defer c.captureMu.Unlock()
	return c.initialized && c.migrator.IsMigrateDone()
}
