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

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdcv2/metadata"
	"github.com/pingcap/tiflow/pkg/chann"
	"github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

const (
	lease             time.Duration = 10 * time.Second
	heartbeatDeadline time.Duration = time.Second
	networkJitter     time.Duration = time.Second
)

var (
	_ metadata.CaptureObservation    = &CaptureOb{}
	_ metadata.ControllerObservation = &ControllerOb{}
	_ metadata.OwnerObservation      = &ownerOb{}
)

// Storage is used by metadata interfaces.
type Storage struct {
	epoch atomic.Uint64

	entities struct {
		sync.RWMutex
		cfs   map[model.ChangeFeedID]*metadata.ChangefeedInfo
		cfids map[model.ChangeFeedID]metadata.ChangefeedID
		ups   map[uint64]*metadata.UpstreamInfo

		// 0: normal; 1: paused; 2: finished;
		cfstates map[metadata.ChangefeedID]int
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
		owners map[metadata.ChangefeedID]metadata.ScheduledChangefeed
		// processors by ChangefeedID, watched by owner.
		processors map[metadata.ChangefeedID]sortedScheduledChangefeeds
		// owners by CaptureID, watched by captures.
		ownersByCapture map[string]sortedScheduledChangefeeds
		// processors by CaptureID, watched by captures.
		processorsByCapture map[string]sortedScheduledChangefeeds
	}
	progresses map[metadata.ChangefeedID]metadata.ChangefeedProgress
}

func newStorage() *Storage {
	s := &Storage{}
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

func (s *Storage) setOwner(cf metadata.ChangefeedID, target metadata.ScheduledChangefeed) error {
	target.TaskPosition = s.progresses[cf]

	origin := s.schedule.owners[cf]
	if err := metadata.CheckScheduleState(origin, target); err != nil {
		return err
	}
	s.schedule.owners[cf] = target

	byCapture := s.schedule.ownersByCapture[target.CaptureID]
	if byCapture.compare == nil {
		byCapture.compare = compareByChangefeed
	}
	byCapture.upsert(target)
	s.schedule.ownersByCapture[target.CaptureID] = byCapture

	return nil
}

func (s *Storage) setProcessors(cf metadata.ChangefeedID, workers []metadata.ScheduledChangefeed) error {
	origin := s.schedule.processors[cf]
	target := sortedScheduledChangefeeds{version: origin.version + 1, v: workers, compare: compareByCaptureID}
	target.sort()

	diffs, err := metadata.DiffScheduledChangefeeds(origin.v, target.v, target.compare)
	if err != nil {
		return err
	}

	s.schedule.processors[cf] = target
	for _, diff := range diffs {
		if diff.State != metadata.SchedRemoved {
			x := s.schedule.processorsByCapture[diff.CaptureID]
			if x.compare == nil {
				x.compare = compareByChangefeed
			}
			x.upsert(diff)
			s.schedule.processorsByCapture[diff.CaptureID] = x
		}
	}

	return nil
}

type contextManager struct {
	pctx    context.Context
	pcancel context.CancelFunc
	wg      sync.WaitGroup

	sync.RWMutex
	ctx context.Context
}

func newContextManager(ctx context.Context) *contextManager {
	pctx, pcancel := context.WithCancel(ctx)
	return &contextManager{pctx: pctx, pcancel: pcancel}
}

func (c *contextManager) fetchContext() (ctx context.Context) {
	c.RLock()
	defer c.RUnlock()
	ctx = c.ctx
	return
}

func (c *contextManager) refreshContext() {
	ctx, _ := context.WithTimeout(c.pctx, 10*time.Second)
	c.Lock()
	defer c.Unlock()
	c.ctx = ctx
}

func (c *contextManager) stop() {
	if c.pcancel != nil {
		c.pcancel()
	}
}

// CaptureOb is an implement for metadata.CaptureObservation.
type CaptureOb struct {
	s              *Storage
	c              *metadata.CaptureInfo
	contextManager *contextManager

	tasks struct {
		sync.RWMutex
		owners     sortedScheduledChangefeeds
		processors sortedScheduledChangefeeds
	}

	ownerChanges     *chann.DrainableChann[metadata.ScheduledChangefeed]
	processorChanges *chann.DrainableChann[metadata.ScheduledChangefeed]
}

// NewCaptureObservation creates a capture observation.
func NewCaptureObservation(s *Storage, c *metadata.CaptureInfo) *CaptureOb {
	return &CaptureOb{
		s:                s,
		c:                c,
		ownerChanges:     chann.NewAutoDrainChann[metadata.ScheduledChangefeed](),
		processorChanges: chann.NewAutoDrainChann[metadata.ScheduledChangefeed](),
	}
}

// Run runs the given CaptureOb.
func (c *CaptureOb) Run(ctx context.Context) error {
	c.contextManager = newContextManager(ctx)

	if err := c.Heartbeat(ctx); err != nil {
		// Do a heartbeat to refresh context.
		return err
	}

	c.contextManager.wg.Add(1)
	go func() {
		defer c.contextManager.wg.Done()
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			updatedCtx := c.contextManager.fetchContext()
			select {
			case <-updatedCtx.Done():
				err := updatedCtx.Err()
				log.Info("capture stops heartbeat", zap.String("capture", c.c.ID), zap.Error(err))
				return
			case <-ticker.C:
			}

			hbCtx, cancel := context.WithTimeout(updatedCtx, heartbeatDeadline)
			if err := c.Heartbeat(hbCtx); err != nil {
				log.Warn("capture heartbeat fail", zap.String("capture", c.c.ID), zap.Error(err))
				c.contextManager.stop()
				return
			}
			cancel()
		}
	}()

	c.contextManager.wg.Add(1)
	go func() {
		defer c.contextManager.wg.Done()
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			updatedCtx := c.contextManager.fetchContext()
			select {
			case <-updatedCtx.Done():
				err := updatedCtx.Err()
				log.Info("capture stops handle task changes", zap.String("capture", c.c.ID), zap.Error(err))
				return
			case <-ticker.C:
			}

			if err := c.handleTaskChanges(updatedCtx, true); err != nil {
				log.Warn("capture handle task changes fail", zap.String("capture", c.c.ID), zap.Error(err))
				c.contextManager.stop()
				return
			}
			if err := c.handleTaskChanges(updatedCtx, false); err != nil {
				log.Warn("capture handle task changes fail", zap.String("capture", c.c.ID), zap.Error(err))
				c.contextManager.stop()
			}
		}
	}()

	c.contextManager.wg.Wait()
	return nil
}

func (c *CaptureOb) handleTaskChanges(ctx context.Context, isOwner bool) error {
	var tasks sortedScheduledChangefeeds
	var exists bool
	var localTasks *sortedScheduledChangefeeds
	var emitEvents chan<- metadata.ScheduledChangefeed

	if isOwner {
		c.s.RLock()
		tasks, exists = c.s.schedule.ownersByCapture[c.Self().ID]
		c.s.RUnlock()

		c.tasks.Lock()
		defer c.tasks.Unlock()
		localTasks = &c.tasks.owners
		emitEvents = c.ownerChanges.In()
	} else {
		c.s.RLock()
		tasks, exists = c.s.schedule.processorsByCapture[c.Self().ID]
		c.s.RUnlock()

		c.tasks.Lock()
		defer c.tasks.Unlock()
		localTasks = &c.tasks.processors
		emitEvents = c.processorChanges.In()
	}
	if !exists {
		// No scheudle information for the capture.
		return nil
	}

	if localTasks.version < tasks.version {
		changes, err := metadata.DiffScheduledChangefeeds(localTasks.v, tasks.v, compareByChangefeed)
		if err != nil {
			return err
		}
		for _, change := range changes {
			if change.State != metadata.SchedRemoved {
				emitEvents <- change
			}
		}
		*localTasks = tasks
	}

	return nil
}

func (c *CaptureOb) Self() *metadata.CaptureInfo {
	return c.c
}

func (c *CaptureOb) Heartbeat(context.Context) error {
	c.s.Lock()
	defer c.s.Unlock()
	c.s.keepalive.captures[c.Self().ID] = c.c
	c.s.keepalive.heartbeats[c.Self().ID] = time.Now()

	c.contextManager.refreshContext()
	return nil
}

func (c *CaptureOb) TakeControl() (metadata.ControllerObservation, error) {
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
			if ok && time.Since(heartbeat) <= lease+heartbeatDeadline+networkJitter {
				c.s.Unlock()
				continue
			}
		}
		c.s.schedule.controller = c.Self().ID
		c.s.Unlock()
		controllerOb := newControllerObservation(c.contextManager, c.s, c.c)
		c.contextManager.wg.Add(1)
		go func() {
			defer c.contextManager.wg.Done()
			controllerOb.run()
		}()
	}
}

func (c *CaptureOb) Advance(cfs []metadata.ChangefeedID, progresses []metadata.ChangefeedProgress) error {
	c.s.Lock()
	defer c.s.Unlock()
	for i, cf := range cfs {
		if owner, ok := c.s.schedule.owners[cf]; ok && owner.State == metadata.SchedLaunched {
			c.s.progresses[cf] = progresses[i]
		}
	}
	return nil
}

func (c *CaptureOb) OwnerChanges() <-chan metadata.ScheduledChangefeed {
	return c.ownerChanges.Out()
}

func (c *CaptureOb) PostOwnerRemoved(cf metadata.ChangefeedID) error {
	c.s.Lock()
	defer c.s.Unlock()
	if _, ok := c.s.schedule.owners[cf]; ok {
		delete(c.s.schedule.owners, cf)
	}
	return nil
}

func (c *CaptureOb) ProcessorChanges() <-chan metadata.ScheduledChangefeed {
	return c.processorChanges.Out()
}

func (c *CaptureOb) PostProcessorRemoved(cf metadata.ChangefeedID) error {
	c.s.Lock()
	defer c.s.Unlock()
	if processors, ok := c.s.schedule.processors[cf]; ok {
		processors.version += 1
		// processors.v = postProcessorRemoved(processors.v, c.Self().ID)
		c.s.schedule.processors[cf] = processors
	}
	if processors, ok := c.s.schedule.processorsByCapture[c.Self().ID]; ok {
		processors.version += 1
		// processors.v = postProcessorRemoved(processors.v, c.Self().ID)
		c.s.schedule.processorsByCapture[c.Self().ID] = processors
	}
	return nil
}

// ControllerOb is an implement for metadata.ControllerObservation.
type ControllerOb struct {
	s              *Storage
	c              *metadata.CaptureInfo
	contextManager *contextManager

	aliveCaptures struct {
		sync.Mutex
		outgoing     []*metadata.CaptureInfo
		incoming     []*metadata.CaptureInfo
		outgoingHash uint64
		incomingHash uint64
	}

	captures *chann.DrainableChann[[]*metadata.CaptureInfo]
}

func newControllerObservation(ctxMgr *contextManager, s *Storage, c *metadata.CaptureInfo) *ControllerOb {
	return &ControllerOb{
		s:              s,
		c:              c,
		contextManager: ctxMgr,
		captures:       chann.NewAutoDrainChann[[]*metadata.CaptureInfo](),
	}
}

func (c *ControllerOb) run() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		updatedCtx := c.contextManager.fetchContext()
		select {
		case <-updatedCtx.Done():
			err := updatedCtx.Err()
			log.Info("controller stops handle alive captures ", zap.String("capture", c.c.ID), zap.Error(err))
			return
		case <-ticker.C:
		}

		if err := c.handleAliveCaptures(updatedCtx); err != nil {
			log.Warn("controller handle alive captures fail", zap.String("capture", c.c.ID), zap.Error(err))
			c.contextManager.stop()
			return
		}
	}
}

func (c *ControllerOb) handleAliveCaptures(ctx context.Context) error {
	alives := make([]*metadata.CaptureInfo, 0)
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
	c.aliveCaptures.incomingHash = hash
	c.aliveCaptures.incoming = alives
	return nil
}

func (c *ControllerOb) CreateChangefeed(cf *metadata.ChangefeedInfo, up *metadata.UpstreamInfo) (metadata.ChangefeedID, error) {
	c.s.entities.Lock()
	defer c.s.entities.Unlock()

	if _, ok := c.s.entities.cfs[cf.ID]; ok {
		return metadata.ChangefeedID{}, errors.New("changefeed exists")
	}

	c.s.entities.cfs[cf.ID] = cf
	c.s.entities.ups[up.ID] = up

	id := metadata.ChangefeedID{ChangeFeedID: cf.ID, Epoch: c.s.epoch.Add(1)}
	id.Comparable = fmt.Sprintf("%s.%d", id.String(), id.Epoch)
	c.s.entities.cfids[cf.ID] = id
	return id, nil
}

func (c *ControllerOb) RemoveChangefeed(cf metadata.ChangefeedID) error {
	c.s.entities.Lock()
	defer c.s.entities.Unlock()
	c.s.Lock()
	defer c.s.Unlock()

	delete(c.s.entities.cfids, cf.ChangeFeedID)
	delete(c.s.entities.cfs, cf.ChangeFeedID)

	// TODO: we need to rollback partial changes on fails.
	if owner, ok := c.s.schedule.owners[cf]; ok {
		owner.State = metadata.SchedRemoving
		if err := c.s.setOwner(cf, owner); err != nil {
			return err
		}
	}
	if processors, ok := c.s.schedule.processors[cf]; ok {
		if err := c.s.setProcessors(cf, processors.v); err != nil {
			return err
		}
	}

	return nil
}

func (c *ControllerOb) RefreshCaptures() (captures []*metadata.CaptureInfo, changed bool) {
	c.aliveCaptures.Lock()
	defer c.aliveCaptures.Unlock()
	if c.aliveCaptures.outgoingHash != c.aliveCaptures.incomingHash {
		c.aliveCaptures.outgoingHash = c.aliveCaptures.incomingHash
		c.aliveCaptures.outgoing = c.aliveCaptures.incoming
	}
	captures = make([]*metadata.CaptureInfo, len(c.aliveCaptures.outgoing))
	copy(captures, c.aliveCaptures.outgoing)
	return
}

func (c *ControllerOb) SetOwner(cf metadata.ChangefeedID, target metadata.ScheduledChangefeed) error {
	c.s.Lock()
	defer c.s.Unlock()
	return c.s.setOwner(cf, target)
}

func (c *ControllerOb) SetProcessors(cf metadata.ChangefeedID, workers []metadata.ScheduledChangefeed) error {
	c.s.Lock()
	defer c.s.Unlock()
	return c.s.setProcessors(cf, workers)
}

func (c *ControllerOb) GetChangefeedSchedule(cf metadata.ChangefeedID) (s metadata.ChangefeedSchedule, err error) {
	c.s.RLock()
	defer c.s.RUnlock()
	s.Owner = c.s.schedule.owners[cf]
	s.Processors = c.s.schedule.processors[cf].v
	return
}

func (c *ControllerOb) ScheduleSnapshot() (ss []metadata.ChangefeedSchedule, cs []*metadata.CaptureInfo, err error) {
	c.s.RLock()
	ss = make([]metadata.ChangefeedSchedule, 0, len(c.s.entities.cfids))
	cs = make([]*metadata.CaptureInfo, 0, len(c.s.keepalive.captures))
	for _, cf := range c.s.entities.cfids {
		var s metadata.ChangefeedSchedule
		s.Owner = c.s.schedule.owners[cf]
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
	c.aliveCaptures.outgoingHash = hash
	c.aliveCaptures.outgoing = cs
	return
}

type ownerOb struct {
	s              *Storage
	c              *metadata.ChangefeedInfo
	id             metadata.ChangefeedID
	contextManager *contextManager

	processors struct {
		sync.Mutex
		outgoing []metadata.ScheduledChangefeed
		incoming []metadata.ScheduledChangefeed
	}
}

func (o *ownerOb) Self() (*metadata.ChangefeedInfo, metadata.ChangefeedID) {
	return o.c, o.id
}

func (o *ownerOb) PauseChangefeed() error {
	o.s.entities.Lock()
	defer o.s.entities.Unlock()
	o.s.Lock()
	defer o.s.Unlock()

	o.s.entities.cfstates[o.id] = 1
	return o.clearSchedule()
}

func (o *ownerOb) ResumeChangefeed() error {
	o.s.entities.Lock()
	defer o.s.entities.Unlock()

	o.s.entities.cfstates[o.id] = 0
	return nil
}

func (o *ownerOb) UpdateChangefeed(info *metadata.ChangefeedInfo) error {
	o.s.entities.Lock()
	defer o.s.entities.Unlock()

	copied := new(metadata.ChangefeedInfo)
	*copied = *info
	copied.Config = info.Config.Clone()
	o.s.entities.cfs[info.ID] = copied
	return nil
}

func (o *ownerOb) SetChangefeedFinished() error {
	o.s.entities.Lock()
	defer o.s.entities.Unlock()
	o.s.Lock()
	defer o.s.Unlock()

	o.s.entities.Lock()
	defer o.s.entities.Unlock()
	o.s.Lock()
	defer o.s.Unlock()

	o.s.entities.cfstates[o.id] = 2
	return o.clearSchedule()
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

func (o *ownerOb) clearSchedule() error {
	cf := o.id
	if owner, ok := o.s.schedule.owners[cf]; ok {
		owner.State = metadata.SchedRemoving
		if err := o.s.setOwner(cf, owner); err != nil {
			return err
		}
	}
	if processors, ok := o.s.schedule.processors[cf]; ok {
		if err := o.s.setProcessors(cf, processors.v); err != nil {
			return err
		}
	}
	return nil
}

func (o *ownerOb) RefreshProcessors() (captures []metadata.ScheduledChangefeed, changed bool) {
	o.processors.Lock()
	defer o.processors.Unlock()
	return o.processors.outgoing, true
}

func (c *CaptureOb) GetChangefeeds(cfs ...model.ChangeFeedID) ([]*metadata.ChangefeedInfo, []metadata.ChangefeedID, error) {
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
	return infos, ids, nil
}

func (c *CaptureOb) GetCaptures(cps ...string) ([]*metadata.CaptureInfo, error) {
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
	infos := make([]*metadata.CaptureInfo, 0, length)
	for _, info := range c.s.keepalive.captures {
		infos = append(infos, info)
	}
	return infos, nil
}

func compareByChangefeed(a, b metadata.ScheduledChangefeed) int {
	return strings.Compare(a.ChangefeedID.Comparable, b.ChangefeedID.Comparable)
}

func compareByCaptureID(a, b metadata.ScheduledChangefeed) int {
	return strings.Compare(a.CaptureID, b.CaptureID)
}

// sorted `ScheduledChangefeed`s, with a version to simplify diff check.
type sortedScheduledChangefeeds struct {
	version int
	v       []metadata.ScheduledChangefeed
	compare func(a, b metadata.ScheduledChangefeed) int
}

func (s *sortedScheduledChangefeeds) sort() {
	sort.Slice(s.v, func(i, j int) bool { return s.compare(s.v[i], s.v[j]) < 0 })
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
