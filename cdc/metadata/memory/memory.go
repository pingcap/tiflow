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

// nolint
package memory

import (
	"context"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/tiflow/cdc/capture"
	"github.com/pingcap/tiflow/cdc/metadata"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/chann"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/pkg/util"
)

const (
    lease time.Duration = 10*time.Second
    heartbeatDeadline time.Duration = time.Second
    networkJitter time.Duration = time.Second
)

var (
	_ metadata.CaptureObservation    = &captureOb{}
	_ metadata.ControllerObservation = &controllerOb{}
	_ metadata.OwnerObservation      = &ownerOb{}
)

// sorted `ScheduledChangefeed`s, with a version to simplify diff check.
type sortedScheduledChangefeeds struct {
    version int
    v []metadata.ScheduledChangefeed
    compare func (a, b metadata.ScheduledChangefeed) int
}

type storage struct {
    epoch atomic.Uint64

	entities struct {
		sync.RWMutex
		cfs map[model.ChangeFeedID]*metadata.ChangefeedInfo
        cfids map[model.ChangeFeedID]metadata.ChangefeedID
		ups map[uint64]*metadata.UpstreamInfo
	}

	sync.RWMutex
	keepalive struct {
		captures   map[string]*metadata.CaptureInfo
		heartbeats map[string]time.Time
	}
	schedule struct {
        // CaptureID of the global controller.
		controller string
        // owners by ChangefeedID.
		owners     map[metadata.ChangefeedID]metadata.ScheduledChangefeed
        // processors by ChangefeedID, watched by owner.
		processors map[metadata.ChangefeedID]sortedScheduledChangefeeds
        // owners by CaptureID, watched by captures.
        ownersByCapture map[string]sortedScheduledChangefeeds
        // processors by CaptureID, watched by captures.
        processorsByCapture map[string]sortedScheduledChangefeeds
	}
    progresses map[metadata.ChangefeedID]metadata.ChangefeedProgress
}

func newStorage() *storage {
	s := &storage{}
	s.entities.cfs = make(map[model.ChangeFeedID]*metadata.ChangefeedInfo)
	s.entities.cfids = make(map[model.ChangeFeedID]metadata.ChangefeedID)
	s.entities.ups = make(map[uint64]*metadata.UpstreamInfo)
	s.keepalive.captures = make(map[string]*metadata.CaptureInfo)
	s.keepalive.heartbeats = make(map[string]time.Time)
	s.schedule.owners = make(map[metadata.ChangefeedID]metadata.ScheduledChangefeed)
	s.schedule.processors = make(map[metadata.ChangefeedID]sortedScheduledChangefeeds)
    s.schedule.ownersByCapture = make(map[string]sortedScheduledChangefeeds)
    s.schedule.processorsByCapture = make(map[string]sortedScheduledChangefeeds)
    s.progresses = make(map[metadata.ChangefeedID]metadata.ChangefeedProgress)
	return s
}

type contextManager struct {
	pctx   context.Context

	sync.RWMutex
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
    ctx, cancel := context.WithTimeout(c.pctx, 10*time.Second)

	c.Lock()
	defer c.Unlock()
    if c.ctx != nil {
        c.cancel()
        c.ctx = nil
        c.cancel = nil
    }
	c.ctx, c.cancel = ctx, cancel
}

func (c *contextManager) cancel() {
	c.Lock()
	defer c.Unlock()
    if c.ctx != nil {
        c.cancel()
        c.ctx = nil
        c.cancel = nil
    }
}

type captureOb struct {
	s *storage
	c *metadata.CaptureInfo
	contextManager *contextManager

    roles struct {
        sync.RWMutex
        owners sortedScheduledChangefeeds
        processors sortedScheduledChangefeeds
    }

	wg         sync.WaitGroup
	owners     *chann.DrainableChann[metadata.ScheduledChangefeed]
	processors *chann.DrainableChann[metadata.ScheduledChangefeed]
}

func newCaptureObservation(ctx context.Context, s *storage, c *metadata.CaptureInfo) (metadata.CaptureObservation, error) {
    contextManager := &contextManager{ pctx: ctx }
    captureOb := &captureOb{
        s:s,
        c: c,
        contextManager: contextManager,
        owners: chann.NewAutoDrainChann[metadata.ScheduledChangefeed](),
        processors: chann.NewAutoDrainChann[metadata.ScheduledChangefeed](),
    }

    if err := captureOb.Heartbeat(ctx); err != nil {
        // Do a heartbeat to refresh context.
        return nil, err
    }

    captureOb.wg.Add(1)
    go func() {
        defer captureOb.wg.Done()
        ticker := time.NewTicker(time.Second)
        defer ticker.Stop()
        for {
            updatedCtx := captureOb.contextManager.fetchContext()
            select {
            case <-updatedCtx.Done():
                return
            case <-ticker.C:
            }
            hbCtx, cancel := context.WithTimeout(updatedCtx, heartbeatDeadline)
            _ = captureOb.Heartbeat(hbCtx)
            cancel()
        }
    }()

    captureOb.wg.Add(1)
    go func() {
        defer captureOb.wg.Done()
        ticker := time.NewTicker(200*time.Millisecond)
        defer ticker.Stop()
        for {
            updatedCtx := captureOb.contextManager.fetchContext()
            select {
            case <-updatedCtx.Done():
                return
            case <-ticker.C:
            }

            if err := captureOb.handleRoleChanges(updatedCtx, true); err != nil {
                panic("handleRoleChanges(owner)")
            }
            if err := captureOb.handleRoleChanges(updatedCtx, false); err != nil {
                panic("handleRoleChanges(processors)")
            }
        }
    }()

    return captureOb, nil
}

func stopCaptureObservation(c *captureOb) {
    c.contextManager.cancel()
    c.wg.Wait()
}

func (c *captureOb) handleRoleChanges(ctx context.Context, isOwner bool) error {
    var roles sortedScheduledChangefeeds
    var exists bool
    var localRoles *sortedScheduledChangefeeds
    var emitEvents chan <-metadata.ScheduledChangefeed

    if isOwner {
        c.s.RLock()
        roles, exists = c.s.schedule.ownersByCapture[c.Self().ID]
        c.s.RUnlock()

        c.roles.Lock()
        defer c.roles.Unlock()
        localRoles = &c.roles.owners
        emitEvents = c.owners.In()
    } else {
        c.s.RLock()
        roles, exists = c.s.schedule.processorsByCapture[c.Self().ID]
        c.s.RUnlock()

        c.roles.Lock()
        defer c.roles.Unlock()
        localRoles = &c.roles.processors
        emitEvents = c.processors.In()
    }
    if !exists {
        // No scheudle information for the capture.
        return nil
    }

    if localRoles.version < roles.version {
        changes, err := metadata.DiffScheduledChangefeeds(localRoles.v, roles.v, sortedByChangefeedID)
        if err != nil {
            return err
        }
        for _, change := range changes {
            if change.State == metadata.SchedRemoved {
                continue
            }
            emitEvents <- change
        }
        *localRoles = roles
    }

    return nil
}

func (c *captureOb) Self() *metadata.CaptureInfo {
	return c.c
}

func (c *captureOb) GetChangefeeds(cfs ...model.ChangeFeedID) ([]*ChangefeedInfo, []ChangefeedID, error) {
    c.s.entities.RLock()
    defer c.s.entities.RUnlock()

    length := len(cfs)
    if length > 0 {
        infos := make([]*metadata.ChangefeedInfo, 0, length)
        ids := make([]metadata.ChangefeedID, 0, length)
        for _, id := range cfs {
            infos = append(infos, c.s.entities.cfs[id])
            ids = append(ids, c.s.entities.cfids[id])
        }
        return infos, ids, nil
    }

    length = len(c.s.entities.cfs)
    infos := make([]*metadata.ChangefeedInfo, 0, length)
    ids := make([]metadata.ChangefeedID, 0, length)
    for id, info := range c.s.entities.cfs {
        infos = append(infos, info)
        ids = append(ids, c.s.entities.cfids[id])
    }
	return nil, nil
}

func (c *captureOb) GetCaptures(cps ...string) ([]*metadata.CaptureInfo, error) {
    c.s.RLock()
    defer c.s.RUnlock()

    length := len(cps)
    if length > 0 {
        infos := make([]*metadata.CaptureInfo, 0, length)
        for _, id := range cps {
            infos = append(infos, c.s.keepalive.captures[id])
        }
        return infos, nil
    }

    length = len(c.s.keepalive.captures)
    for id, info := range c.s.keepalive.captures {
        infos = append(infos, info)
    }
	return infos, nil
}

func (c *captureOb) Heartbeat(context.Context) error {
	c.s.Lock()
	defer c.s.Unlock()
	c.s.keepalive.captures[c.Self().ID] = c.c
	c.s.keepalive.heartbeats[c.Self().ID] = time.Now()

	c.contextManager.refreshContext()
	return nil
}

func (c *captureOb) TakeControl() (metadata.ControllerObservation, error) {
	for {
		if err := util.Hang(c.contextManager.fetchContext(), time.Second); err != nil {
			return nil, err
		}

		c.s.Lock()
		if _, ok := c.s.keepalive.heartbeats[c.Self().ID]; !ok {
			c.s.Unlock()
			continue
		}
		controller := c.s.schedule.controller
		if len(controller) > 0 {
			heartbeat, ok := c.s.keepalive.heartbeats[controller]
			if ok && time.Since(heartbeat) <= lease + heartbeatDeadline + networkJitter {
				c.s.Unlock()
				continue
			}
		}
		c.s.schedule.controller = c.Self().ID
		c.s.Unlock()
        return newControllerObservation(c.contextManager, c.s, c.c), nil
	}
}

func (c *captureOb) Advance(cfs []metadata.ChangefeedID, progresses []metadata.ChangefeedProgress) error {
	c.s.Lock()
	defer c.s.Unlock()
	for i, cf := range cfs {
		if owner, ok := c.s.schedule.owners[cf]; ok && owner.State == metadata.SchedLaunched {
			c.s.advance.progresses[cf] = progresses[i]
		}
	}
	return nil
}

func (c *captureOb) OwnerChanges() <-chan metadata.ScheduledChangefeed {
	return c.owners.Out()
}

func (c *captureOb) PostOwnerRemoved(cf metadata.ChangefeedID) error {
	c.s.Lock()
	defer c.s.Unlock()
	if owner, ok := c.s.schedule.owners[cf]; ok {
		owner.State = metadata.SchedRemoved
	}
	return nil
}

func (c *captureOb) ProcessorChanges() <-chan metadata.ScheduledChangefeed {
	return c.processors.Out()
}

func (c *captureOb) PostProcessorRemoved(cf metadata.ChangefeedID) error {
	c.s.Lock()
	defer c.s.Unlock()
	if processors, ok := c.s.schedule.processors[cf]; ok {
        processors.version += 1
        processors.v = postProcessorRemoved(processors.v, c.Self().ID)
        c.s.schedule.processors[cf] = processors
	}
    if processors, ok := c.s.schedule.processorsByCapture[c.Self().ID]; ok {
        processors.version += 1
        processors.v = postProcessorRemoved(processors.v, c.Self().ID)
        c.s.schedule.processorsByCapture[c.Self().ID] = processors
    }
	return nil
}

type controllerOb struct {
	s    *storage
	c    *metadata.CaptureInfo
	contextManager *contextManager

    aliveCaptures struct {
        sync.Mutex
        inited bool
        v []metadata.CaptureInfo
        hash uint64
    }

	wg       sync.WaitGroup
    captures *chann.DrainableChann[[]*metadata.CaptureInfo]
}

func newControllerObservation(ctxMgr *contextManager, s *storage, c *metadata.CaptureInfo) (metadata.ControllerObservation, error) {
    controllerOb := &controllerOb{
        s: s,
        c: c,
        contextManager: contextManager
        captures: chann.NewAutoDrainChann[[]*metadata.CaptureInfo](),
    }

    controllerOb.wg.Add(1)
    go func() {
        defer controllerOb.wg.Done()
        ticker := time.NewTicker(200*time.Millisecond)
        defer ticker.Stop()
        var preAliveHash uint64 = 0
        for {
            updatedCtx := controllerOb.contextManager.fetchContext()
            select {
            case <-updatedCtx.Done():
                return
            case <-ticker.C:
            }

            controllerOb.handleAliveCaptures(updatedCtx)
        }
    }()

    return controllerOb, nil
}

func (c *controllerOb) handleAliveCaptures(ctx context.Context) {
    c.aliveCaptures.Lock()
    defer c.aliveCaptures.Unlock()
    if !c.aliveCaptures.inited {
        return nil
    }

    alives := make([]*metadata.CaptureInfo)
    c.s.RLock()
    for id, info := range c.s.keepalive.captures {
        if hb, ok := c.s.keepalive.heartbeats[id]; !ok || time.Since(hb) > lease {
            continue
        }
        alives = append(alives, info)
    }
    c.s.RUnlock()

    hash := sortAndHashCaptureList(alives)

    c.aliveCaptures.Lock()
    defer c.aliveCaptures.Unlock()
    if c.aliveCaptures.inited && c.aliveCaptures.hash != hash {
        c.aliveCaptures.v = alives
        c.aliveCaptures.hash = hash
        c.captures.In() <- alives
    }
}

func (c *controllerOb) CreateChangefeed(cf *metadata.ChangefeedInfo, up *metadata.UpstreamInfo) (metadata.ChangefeedID, error) {
    c.s.entities.Lock()
    defer c.s.entities.Unlock()

    if _, ok := c.s.entities.cfs[cf.ID]; ok {
        return metadata.ChangefeedID{}, errors.New("changefeed exists")
    }

    c.s.entities.cfs[cf.ID] = cf
    c.s.entities.ups[up.ID] = up

    id := metadata.ChangefeedID{ ChangeFeedID: cf.ID, Epoch: c.s.epoch.Add(1)}
    id.Comparable = fmt.Sprintf("%s.%d", id.String(), id.Epoch)
    c.s.entities.cfids[cf.ID] = id
    return id, nil
}

func (c *controllerOb) RemoveChangefeed(cf metadata.ChangefeedID) error {
    c.s.entities.Lock()
    defer c.s.entities.Unlock()
    delete(c.s.entities.cfids, cf)
    delete(c.s.entities.cfs[cf.ChangeFeedID])
	return nil
}

func (c *controllerOb) RefreshCaptures() <-chan []*metadata.CaptureInfo {
	return c.captures.Out()
}

func (c *controllerOb) SetOwner(cf metadata.ChangefeedID, target metadata.ScheduledChangefeed) error {
	c.s.Lock()
	defer c.s.Unlock()

    target.TaskPosition = c.s.progresses[cf]

    origin := c.s.schedule.owners[cf]
    if err := metadata.CheckScheduleState(origin, target); err != nil {
        return err
    }
	c.s.schedule.owners[cf] = target

    byCapture := c.s.schedule.ownersByCapture[target.CaptureID]
    if byCapture.compare == nil {
        byCapture.compare == compareByChangefeed
    }
    byCapture.upsert(target)
    c.s.schedule.ownersByCapture[target.CaptureID] = byCapture

	return nil
}

func (c *controllerOb) SetProcessors(cf metadata.ChangefeedID, workers []metadata.ScheduledChangefeed) error {
	c.s.Lock()
	defer c.s.Unlock()

    origin := c.s.schedule.processors[cf]
    diffs, err := metadata.DiffScheduledChangefeeds(origin.v, byCapture.v)
    if err != nil {
        return err
    }

    byCapture := sortedScheduledChangefeeds{ version: origin.version + 1, v: workers, compare: compareByCaptureID }
    byCapture.sort()
    c.s.schedule.processors[cf] = byCapture

    for _, diff := range diffs {
        if diff.State != metadata.SchedRemoved {
            x := c.s.schedule.processorsByCapture[diff.CaptureID]
            if x.compare == nil {
                x.compare = compareByChangefeed
            }
            x.upsert(diff)
            c.s.schedule.processorsByCapture[diff.CaptureID] = x
        }
    }

	return nil
}

func (c *controllerOb) GetChangefeedSchedule(cf metadata.ChangefeedID) (s metadata.ChangefeedSchedule, err error) {
    c.s.RLock()
    defer c.s.RUnlock()
    s.Owner = c.s.schedule.owners[cf]
    s.Processors = c.s.schedule.processors[cf].v
    return
}

func (c *controllerOb) ScheduleSnapshot() (ss []ChangefeedSchedule, cs []*CaptureInfo, err error) {
    c.s.RLock()
    ss = make([]metadata.ChangefeedSchedule, 0, len(c.s.entities.cfids))
    cs = make([]*metadata.CaptureInfo, 0, len(c.s.keepalive.captures))
    for _, cf := range c.s.entities.cfids {
        var s metadata.ChangefeedSchedule
        s.Owner = c.s.scheudle.owners[cf]
        s.Processors = c.s.schedule.processors[cf].v
        ss = append(ss, s)
    }
    for id, info := range c.s.keepalive.captures {
        if hb, ok := c.s.keepalive.heartbeats[id]; !ok || time.Since(hb) > lease {
            continue
        }
        cs = append(cs, info)
    }
    c.s.RUnlock()

    hash := sortAndHashCaptureList(cs)
    c.aliveCaptures.Lock()
    defer c.aliveCaptures.Unlock()
    c.aliveCaptures.inited = true
    c.aliveCaptures.v = cs
    c.aliveCaptures.hash
    return
}

type ownerOb struct {
	s    *storage
	c    *metadata.ChangefeedInfo
	contextManager *contextManager

	wg         sync.WaitGroup
    processors *chann.DrainableChann
	processors chan []metadata.ScheduledProcessor

}

func (o *ownerOb) ChangefeedInfo() *metadata.ChangefeedInfo {
	return o.c
}

func (o *ownerOb) PauseChangefeed() error {
	return nil
}

func (o *ownerOb) ResumeChangefeed() error {
	return nil
}

func (o *ownerOb) UpdateChangefeed(info *metadata.ChangefeedInfo) error {
	o.s.entities.Lock()
	defer o.s.entities.Unlock()
	copied := &metadata.ChangefeedInfo{}
	*copied = *info
	o.s.entities.cfs[info.ChangefeedID] = copied
	return nil
}

func (o *ownerOb) SetChangefeedFinished() error {
	return nil
}

func (o *ownerOb) SetChangefeedFailed(err model.RunningError) error {
	return nil
}

func (o *ownerOb) SetChangefeedWarning(warn model.RunningError) error {
	return nil
}

func (o *ownerOb) SetChangefeedPending() error {
	return nil
}

func (o *ownerOb) RefreshProcessors() <-chan []metadata.ScheduledProcessor {
	return o.processors
}

func compareByChangefeed(a, b metadata.ScheduledChangefeed) int {
    return strings.Compare(a.ChangefeedID.Comparable, b.ChangefeedID.Comparable)
}

func compareByCaptureID(a, b metadata.ScheduledChangefeed) int {
    return strings.Compare(a.CaptureID, b.CaptureID)
}

func (s *sortedScheduledChangefeeds) sort() {
    sort.Slice(s.v, func(i, j int) bool { return s.compare(s.v[i], s.v[j]) })
}

func (s *sortedScheduledChangefeeds) upsert(target metadata.ScheduledChangefeed) {
    i := sort.Search(len(s.v), func(i int) bool { return s.compare(s.v[i], target) >= 0 })
    if i > 0 && i < len(s.v) && s.compare(s.v[i], target) == 0 {
        s.v[i] = target
    } else {
        s.v = append(s.v, target)
        s.sort()
    }
    s.version += 1
}

func (s *sortedScheduledChangefeeds) remove(target metadata.ScheduledChangefeed) {
    i := sort.Search(len(s.v), func(i int) bool { return s.compare(s.v[i], target) >= 0 })
    if i > 0 && i < len(s.v) && s.compare(s.v[i], target) == 0 {
        s.v = append(s.v[:i-1], s.v[i:]...)
        s.version += 1
    }
}

func (s *sortedScheduledChangefeeds) update(target metadata.ScheduledChangefeed) {
    i := sort.Search(len(s.v), func(i int) bool { return s.compare(s.v[i], target) >= 0 })
    if i > 0 && i < len(s.v) && s.compare(s.v[i], target) == 0 {
        s.v[i] = target
        s.version += 1
    }
}

func sortAndHashCaptureList(cs []*metadata.CaptureInfo) uint64 {
    hasher := fnv.New64()
    sort.Slice(cs, func(i, j int) bool { return strings.Compare(cs[i].ID, cs[j].ID) < 0 })
    for _, info := range cs {
        hasher.Write([]byte(info.ID))
    }
    return hasher.Sum64()
}
