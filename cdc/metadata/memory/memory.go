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

package memory

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/tiflow/cdc/metadata"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/util"
)

var (
	captureObAssertion    metadata.CaptureObservation    = &captureOb{}
	controllerObAssertion metadata.ControllerObservation = &controllerOb{}
)

type cfKey struct {
	model.ChangeFeedID
	epoch uint64
}

type storage struct {
	entities struct {
		sync.RWMutex
		cfs map[model.ChangeFeedID]*metadata.ChangefeedInfo
		ups map[uint64]*metadata.UpstreamInfo
	}

	sync.RWMutex
	keepalive struct {
		captures   map[string]*metadata.CaptureInfo
		heartbeats map[string]time.Time
	}
	schedule struct {
		controller string
		owners     map[cfKey]*metadata.ScheduledOwner
		processors map[cfKey]map[string]*metadata.ScheduledProcessor
	}
	advance struct {
		progresses map[cfKey]metadata.ChangefeedProgress
	}
}

func newStorage() *storage {
	s := &storage{}
	s.entities.cfs = make(map[model.ChangeFeedID]*metadata.ChangefeedInfo)
	s.entities.ups = make(map[uint64]*metadata.UpstreamInfo)
	s.keepalive.captures = make(map[string]*metadata.CaptureInfo)
	s.keepalive.heartbeats = make(map[string]time.Time)
	s.schedule.owners = make(map[cfKey]*metadata.ScheduledOwner)
	s.schedule.processors = make(map[cfKey]map[string]*metadata.ScheduledProcessor)
	return s
}

type contextManager struct {
	sync.RWMutex
	pctx   context.Context
	ctx    context.Context
	cancel context.CancelFunc
}

func (c *contextManager) fetchContext() (ctx context.Context) {
	c.RLock()
	defer c.RUnlock()
	ctx = c.ctx
	return
}

func (c *contextManager) refreshContext() {
	c.Lock()
	defer c.Unlock()
	c.ctx, c.cancel = context.WithTimeout(c.pctx, 10*time.Second)
}

type captureOb struct {
	s *storage
	c *metadata.CaptureInfo

	wg         sync.WaitGroup
	owners     chan []metadata.ScheduledOwner
	processors chan []metadata.ScheduledProcessor

	contextManager *contextManager
}

func (c *captureOb) CaptureInfo() *metadata.CaptureInfo {
	return c.c
}

func (c *captureOb) GetChangefeeds(...model.ChangeFeedID) ([]*metadata.ChangefeedInfo, error) {
	return nil, nil
}

func (c *captureOb) GetCaptures(...string) ([]*metadata.CaptureInfo, error) {
	return nil, nil
}

func (c *captureOb) Heartbeat(context.Context) error {
	c.s.Lock()
	defer c.s.Unlock()
	c.s.keepalive.captures[c.c.ID] = c.c
	c.s.keepalive.heartbeats[c.c.ID] = time.Now()

	c.contextManager.refreshContext()
	return nil
}

func (c *captureOb) TakeControl() (metadata.ControllerObservation, error) {
	for {
		util.Hang(c.contextManager.fetchContext(), time.Second)

		c.s.Lock()
		if _, ok := c.s.keepalive.heartbeats[c.c.ID]; !ok {
			c.s.Unlock()
			continue
		}
		controller := c.s.schedule.controller
		if len(controller) > 0 {
			heartbeat, ok := c.s.keepalive.heartbeats[controller]
			if ok && time.Since(heartbeat) <= 12*time.Second {
				c.s.Unlock()
				continue
			}
		}
		c.s.schedule.controller = c.c.ID
		c.s.Unlock()
		return nil, nil
	}
}

func (c *captureOb) Advance(cfs []*metadata.ChangefeedInfo, progresses []*metadata.ChangefeedProgress) error {
	c.s.Lock()
	defer c.s.Unlock()
	for i, cf := range cfs {
		key := cfKey{ChangeFeedID: cf.ChangefeedID, epoch: cf.Epoch}
		if owner, ok := c.s.schedule.owners[key]; ok && owner.State == metadata.SchedLaunched {
			key := cfKey{ChangeFeedID: cf.ChangefeedID, epoch: cf.Epoch}
			c.s.advance.progresses[key] = *progresses[i]
		}
	}
	return nil
}

func (c *captureOb) RefreshOwners() <-chan []metadata.ScheduledOwner {
	return c.owners
}

func (c *captureOb) PostOwnerStopped(cf *metadata.ChangefeedInfo) error {
	c.s.Lock()
	defer c.s.Unlock()
	key := cfKey{ChangeFeedID: cf.ChangefeedID, epoch: cf.Epoch}
	if owner, ok := c.s.schedule.owners[key]; ok {
		owner.State = metadata.SchedStopped
	}
	return nil
}

func (c *captureOb) RefreshProcessors() <-chan []metadata.ScheduledProcessor {
	return c.processors
}

func (c *captureOb) PostProcessorStopped(cf *metadata.ChangefeedInfo) error {
	c.s.Lock()
	defer c.s.Unlock()
	key := cfKey{ChangeFeedID: cf.ChangefeedID, epoch: cf.Epoch}
	if processors, ok := c.s.schedule.processors[key]; ok {
		if processor, ok := processors[c.c.ID]; ok {
			processor.State = metadata.SchedStopped
		}
	}
	return nil
}

type controllerOb struct {
	s    *storage
	c    *metadata.CaptureInfo
	pctx context.Context

	wg       sync.WaitGroup
	captures chan []*metadata.CaptureInfo

	contextManager *contextManager
}

func (c *controllerOb) CreateChangefeed(cf *metadata.ChangefeedInfo, up *metadata.UpstreamInfo) error {
	return nil
}

func (c *controllerOb) RemoveChangefeed(cf *metadata.ChangefeedInfo) error {
	return nil
}

func (c *controllerOb) RefreshCaptures() <-chan []*metadata.CaptureInfo {
	return c.captures
}

func (c *controllerOb) SetOwner(cf *metadata.ChangefeedInfo, target metadata.ScheduledOwner) (done <-chan struct{}, err error) {
	return nil, nil
}

func (c *controllerOb) SetProcessors(cf *metadata.ChangefeedInfo, workers []metadata.ScheduledProcessor) (done <-chan struct{}, err error) {
	return nil, nil
}

func (c *controllerOb) GetChangefeedSchedule() ([]metadata.ChangefeedSchedule, error) {
	return nil, nil
}
