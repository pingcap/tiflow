// Copyright 2023 PingCAP, Inc.
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
	"io"
	"sync"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/capture"
	"github.com/pingcap/tiflow/cdc/controller"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/owner"
	"github.com/pingcap/tiflow/cdc/processor"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/engine/factory"
	controllerv2 "github.com/pingcap/tiflow/cdcv2/controller"
	"github.com/pingcap/tiflow/cdcv2/metadata"
	"github.com/pingcap/tiflow/cdcv2/metadata/memory"
	ownerv2 "github.com/pingcap/tiflow/cdcv2/owner"
	"github.com/pingcap/tiflow/pkg/config"
	cdcContext "github.com/pingcap/tiflow/pkg/context"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"github.com/pingcap/tiflow/pkg/p2p"
	"github.com/pingcap/tiflow/pkg/upstream"
	"github.com/pingcap/tiflow/pkg/version"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

// NewCapture returns a new Capture instance
func NewCapture(pdEndpoints []string,
	etcdClient etcd.CDCEtcdClient,
	grpcService *p2p.ServerWrapper,
	sortEngineMangerFactory *factory.SortEngineFactory,
	pdClient pd.Client,
) capture.Capture {
	return &captureImpl{
		config:            config.GetGlobalServerConfig(),
		liveness:          model.LivenessCaptureAlive,
		EtcdClient:        etcdClient,
		grpcService:       grpcService,
		cancel:            func() {},
		pdEndpoints:       pdEndpoints,
		info:              &model.CaptureInfo{},
		sortEngineFactory: sortEngineMangerFactory,
		pdClient:          pdClient,
	}
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
	owner           *ownerv2.OwnerImpl
	controller      controller.Controller
	upstreamManager *upstream.Manager

	EtcdClient etcd.CDCEtcdClient

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

	storage            *memory.Storage
	captureDB          metadata.CaptureObservation
	controllerObserver metadata.ControllerObservation
}

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
	//todo: start register to metadata store
	//err = c.register(stdCtx)
	if err != nil {
		return errors.Trace(err)
	}

	defer func() {
		c.Close()
		c.grpcService.Reset(nil)
	}()

	g, stdCtx := errgroup.WithContext(stdCtx)
	stdCtx, cancel := context.WithCancel(stdCtx)

	ctx := cdcContext.NewContext(stdCtx, &cdcContext.GlobalVars{
		CaptureInfo:       c.info,
		EtcdClient:        c.EtcdClient,
		MessageServer:     c.MessageServer,
		MessageRouter:     c.MessageRouter,
		SortEngineFactory: c.sortEngineFactory,
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

		// run processors
		//err := c.runEtcdWorker(ctx, c.processorManager, globalState, processorFlushInterval, util.RoleProcessor.String())
		log.Info("processor routine exited",
			zap.String("captureID", c.info.ID), zap.Error(err))
		return err
	})

	g.Go(func() error {
		return c.MessageServer.Run(ctx, c.MessageRouter.GetLocalChannel())
	})

	g.Go(func() error {
		return c.captureDB.Run(ctx, func(ctx context.Context,
			controllerObserver metadata.ControllerObservation) error {
			c.controllerObserver = controllerObserver
			c.controller = controllerv2.NewController(
				c.upstreamManager,
				c.info, controllerObserver)
			return nil
		})
	})
	g.Go(func() error {
		return c.owner.Run(ctx)
	})
	return errors.Trace(g.Wait())
}

// reset the capture before run it.
func (c *captureImpl) reset(ctx context.Context) error {

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
	_, err := c.upstreamManager.AddDefaultUpstream(c.pdEndpoints, c.config.Security, c.pdClient)
	if err != nil {
		return errors.Trace(err)
	}

	c.processorManager = processor.NewManager(
		c.info, c.upstreamManager, &c.liveness, c.config.Debug.Scheduler)

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

	c.storage = memory.NewStorage()
	captureDB, err := memory.NewCaptureObservation("/tmp/cdc_m", c.storage, c.info)
	c.captureDB = captureDB
	if err != nil {
		return errors.Trace(err)
	}
	c.owner = ownerv2.NewOwner(c.config.Debug.Scheduler, captureDB, captureDB, c.storage)

	log.Info("capture initialized", zap.Any("capture", c.info))
	return nil
}

func (c captureImpl) Close() {
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

// Drain does nothing for now.
func (c *captureImpl) Drain() <-chan struct{} {
	done := make(chan struct{})
	close(done)
	return done
}

func (c *captureImpl) Liveness() model.Liveness {
	//TODO implement me
	panic("implement me")
}

func (c *captureImpl) GetOwner() (owner.Owner, error) {
	//TODO implement me
	panic("implement me")
}

func (c *captureImpl) GetController() (controller.Controller, error) {
	c.ownerMu.Lock()
	defer c.ownerMu.Unlock()
	if c.owner == nil {
		return nil, cerror.ErrNotOwner.GenWithStackByArgs()
	}
	return c.controller, nil
}

func (c *captureImpl) GetControllerCaptureInfo(ctx context.Context) (*model.CaptureInfo, error) {
	//TODO implement me
	panic("implement me")
}

func (c *captureImpl) IsController() bool {
	c.captureMu.Lock()
	defer c.captureMu.Unlock()
	return c.controller != nil
}

func (c *captureImpl) Info() (model.CaptureInfo, error) {
	c.captureMu.Lock()
	defer c.captureMu.Unlock()
	// when c.reset has not been called yet, c.info is nil.
	if c.info != nil {
		return *c.info, nil
	}
	return model.CaptureInfo{}, cerror.ErrCaptureNotInitialized.GenWithStackByArgs()
}

func (c *captureImpl) StatusProvider() owner.StatusProvider {
	//TODO implement me
	panic("implement me")
}

func (c *captureImpl) WriteDebugInfo(ctx context.Context, w io.Writer) {
	//TODO implement me
	panic("implement me")
}

func (c *captureImpl) GetUpstreamManager() (*upstream.Manager, error) {
	if c.upstreamManager == nil {
		return nil, cerror.ErrUpstreamManagerNotReady
	}
	return c.upstreamManager, nil
}

func (c *captureImpl) GetEtcdClient() etcd.CDCEtcdClient {
	return c.EtcdClient
}

func (c *captureImpl) IsReady() bool {
	return true
}
