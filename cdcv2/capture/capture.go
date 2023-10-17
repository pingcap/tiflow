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

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/capture"
	"github.com/pingcap/tiflow/cdc/controller"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/owner"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/engine/factory"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/p2p"
	"github.com/pingcap/tiflow/pkg/upstream"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
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
	captureMu sync.Mutex
	info      *model.CaptureInfo
	liveness  model.Liveness
	config    *config.ServerConfig

	pdClient        pd.Client
	pdEndpoints     []string
	ownerMu         sync.Mutex
	owner           owner.Owner
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
	if err != nil {
		return errors.Trace(err)
	}
	// todo:  run capture logic
	return nil
}

// reset the capture before run it.
func (c *captureImpl) reset(ctx context.Context) error {
	return nil
}

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

// Drain does nothing for now.
func (c *captureImpl) Drain() <-chan struct{} {
	done := make(chan struct{})
	close(done)
	return done
}

func (c *captureImpl) Liveness() model.Liveness {
	return c.liveness
}

func (c *captureImpl) GetOwner() (owner.Owner, error) {
	c.ownerMu.Lock()
	defer c.ownerMu.Unlock()
	return c.owner, nil
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
	c.ownerMu.Lock()
	defer c.ownerMu.Unlock()
	if c.owner == nil {
		return nil
	}
	panic("implement me")
}

func (c *captureImpl) WriteDebugInfo(ctx context.Context, w io.Writer) {
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
	panic("implement me")
}

func (c *captureImpl) GetUpstreamInfo(ctx context.Context,
	id model.UpstreamID,
	s string,
) (*model.UpstreamInfo, error) {
	panic("implement me")
}
