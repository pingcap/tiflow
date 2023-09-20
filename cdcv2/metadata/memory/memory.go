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
	"hash/fnv"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdcv2/metadata"
	"github.com/pingcap/tiflow/pkg/chann"
	"github.com/pingcap/tiflow/pkg/election"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
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
		cfids map[model.ChangeFeedID]metadata.ChangefeedIDWithEpoch
		ups   map[uint64]*model.UpstreamInfo

		// 0: normal; 1: paused; 2: finished;
		cfstates map[metadata.ChangefeedIDWithEpoch]int
	}

	sync.RWMutex
	schedule struct {
		// owners by ChangefeedID.
		owners map[metadata.ChangefeedIDWithEpoch]metadata.ScheduledChangefeed
		// processors by ChangefeedID, watched by owner.
		processors map[metadata.ChangefeedIDWithEpoch]sortedScheduledChangefeeds
		// owners by CaptureID, watched by captures.
		ownersByCapture map[string]sortedScheduledChangefeeds
		// processors by CaptureID, watched by captures.
		processorsByCapture map[string]sortedScheduledChangefeeds
	}
	progresses map[metadata.ChangefeedIDWithEpoch]metadata.ChangefeedProgress
}

func NewStorage() *Storage {
	s := &Storage{}
	s.entities.cfs = make(map[model.ChangeFeedID]*metadata.ChangefeedInfo)
	s.entities.cfids = make(map[model.ChangeFeedID]metadata.ChangefeedIDWithEpoch)
	s.entities.ups = make(map[uint64]*model.UpstreamInfo)
	s.schedule.owners = make(map[metadata.ChangefeedIDWithEpoch]metadata.ScheduledChangefeed)
	s.schedule.processors = make(map[metadata.ChangefeedIDWithEpoch]sortedScheduledChangefeeds)
	s.schedule.ownersByCapture = make(map[string]sortedScheduledChangefeeds)
	s.schedule.processorsByCapture = make(map[string]sortedScheduledChangefeeds)
	s.progresses = make(map[metadata.ChangefeedIDWithEpoch]metadata.ChangefeedProgress)
	return s
}

func (s *Storage) setOwner(cf metadata.ChangefeedIDWithEpoch, target metadata.ScheduledChangefeed) error {
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

func (s *Storage) setProcessors(cf metadata.ChangefeedIDWithEpoch, workers []metadata.ScheduledChangefeed) error {
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

// CaptureOb is an implement for metadata.CaptureObservation.
type CaptureOb struct {
	// election related fields.
	metadata.Elector
	selfInfo *model.CaptureInfo

	storage *Storage

	tasks struct {
		sync.RWMutex
		owners     sortedScheduledChangefeeds
		processors sortedScheduledChangefeeds
	}

	ownerChanges     *chann.DrainableChann[metadata.ScheduledChangefeed]
	processorChanges *chann.DrainableChann[metadata.ScheduledChangefeed]
}

// NewCaptureObservation creates a capture observation.
func NewCaptureObservation(
	electionDSN string, /* only for election */
	storage *Storage, /* only for meta storage */
	selfInfo *model.CaptureInfo,
) (*CaptureOb, error) {
	inMemoryStorage, err := election.NewInMemorySQLStorage(electionDSN, "election")
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &CaptureOb{
		selfInfo:         selfInfo,
		storage:          storage,
		Elector:          metadata.NewElector(selfInfo, inMemoryStorage),
		ownerChanges:     chann.NewAutoDrainChann[metadata.ScheduledChangefeed](),
		processorChanges: chann.NewAutoDrainChann[metadata.ScheduledChangefeed](),
	}, nil
}

// Run runs the given CaptureOb.
func (c *CaptureOb) Run(
	ctx context.Context,
	controllerCallback func(context.Context, metadata.ControllerObservation) error,
) (err error) {
	onTakeControl := func(electorCtx context.Context) error {
		controllerOb := newControllerObservation(c.storage, c.selfInfo, c.getAllCaptures)

		ctrlErrGroup, ctrlCtx := errgroup.WithContext(electorCtx)
		ctrlErrGroup.Go(func() error {
			return controllerOb.run(ctrlCtx)
		})
		ctrlErrGroup.Go(func() error {
			return controllerCallback(ctrlCtx, controllerOb)
		})
		return ctrlErrGroup.Wait()
	}

	eg, ctx := errgroup.WithContext(ctx)

	eg.Go(func() error {
		return c.Elector.RunElection(ctx, onTakeControl)
	})

	eg.Go(func() error {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				err := ctx.Err()
				log.Info("capture stops handle task changes", zap.String("capture", c.selfInfo.ID), zap.Error(err))
				return err
			case <-ticker.C:
			}

			if err := c.handleTaskChanges(ctx, true); err != nil {
				log.Warn("capture handle task changes fail", zap.String("capture", c.selfInfo.ID), zap.Error(err))
				return err
			}
			if err := c.handleTaskChanges(ctx, false); err != nil {
				log.Warn("capture handle task changes fail", zap.String("capture", c.selfInfo.ID), zap.Error(err))
				return err
			}
		}
	})

	return eg.Wait()
}

func (c *CaptureOb) handleTaskChanges(ctx context.Context, isOwner bool) error {
	var tasks sortedScheduledChangefeeds
	var exists bool
	var localTasks *sortedScheduledChangefeeds
	var emitEvents chan<- metadata.ScheduledChangefeed

	if isOwner {
		c.storage.RLock()
		tasks, exists = c.storage.schedule.ownersByCapture[c.Self().ID]
		c.storage.RUnlock()

		c.tasks.Lock()
		defer c.tasks.Unlock()
		localTasks = &c.tasks.owners
		emitEvents = c.ownerChanges.In()
	} else {
		c.storage.RLock()
		tasks, exists = c.storage.schedule.processorsByCapture[c.Self().ID]
		c.storage.RUnlock()

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

func (c *CaptureOb) Advance(cfs []metadata.ChangefeedIDWithEpoch, progresses []metadata.ChangefeedProgress) error {
	c.storage.Lock()
	defer c.storage.Unlock()
	for i, cf := range cfs {
		if owner, ok := c.storage.schedule.owners[cf]; ok && owner.State == metadata.SchedLaunched {
			c.storage.progresses[cf] = progresses[i]
		}
	}
	return nil
}

func (c *CaptureOb) OwnerChanges() <-chan metadata.ScheduledChangefeed {
	return c.ownerChanges.Out()
}

func (c *CaptureOb) PostOwnerRemoved(cf metadata.ChangefeedIDWithEpoch) error {
	c.storage.Lock()
	defer c.storage.Unlock()
	if _, ok := c.storage.schedule.owners[cf]; ok {
		delete(c.storage.schedule.owners, cf)
	}
	return nil
}

func (c *CaptureOb) ProcessorChanges() <-chan metadata.ScheduledChangefeed {
	return c.processorChanges.Out()
}

func (c *CaptureOb) PostProcessorRemoved(cf metadata.ChangefeedIDWithEpoch) error {
	c.storage.Lock()
	defer c.storage.Unlock()
	if processors, ok := c.storage.schedule.processors[cf]; ok {
		processors.version += 1
		// processors.v = postProcessorRemoved(processors.v, c.Self().ID)
		c.storage.schedule.processors[cf] = processors
	}
	if processors, ok := c.storage.schedule.processorsByCapture[c.Self().ID]; ok {
		processors.version += 1
		// processors.v = postProcessorRemoved(processors.v, c.Self().ID)
		c.storage.schedule.processorsByCapture[c.Self().ID] = processors
	}
	return nil
}

func (c *CaptureOb) GetChangefeeds(cfs ...model.ChangeFeedID) ([]*metadata.ChangefeedInfo, []metadata.ChangefeedIDWithEpoch, error) {
	c.storage.entities.RLock()
	defer c.storage.entities.RUnlock()

	length := len(cfs)
	if length > 0 {
		infos := make([]*metadata.ChangefeedInfo, 0, length)
		ids := make([]metadata.ChangefeedIDWithEpoch, 0, length)
		for _, id := range cfs {
			infos = append(infos, c.storage.entities.cfs[id])
			ids = append(ids, c.storage.entities.cfids[id])
		}
		return infos, ids, nil
	}

	length = len(c.storage.entities.cfs)
	infos := make([]*metadata.ChangefeedInfo, 0, length)
	ids := make([]metadata.ChangefeedIDWithEpoch, 0, length)
	for id, info := range c.storage.entities.cfs {
		infos = append(infos, info)
		ids = append(ids, c.storage.entities.cfids[id])
	}
	return infos, ids, nil
}

func (c *CaptureOb) getAllCaptures() []*model.CaptureInfo {
	infos, _ := c.GetCaptures()
	return infos
}

// ControllerOb is an implement for metadata.ControllerObservation.
type ControllerOb struct {
	storage  *Storage
	selfInfo *model.CaptureInfo

	aliveCaptures struct {
		sync.Mutex
		outgoing     []*model.CaptureInfo
		incoming     []*model.CaptureInfo
		outgoingHash uint64
		incomingHash uint64
	}

	getAllCaptures func() []*model.CaptureInfo
}

func newControllerObservation(
	storage *Storage,
	selfInfo *model.CaptureInfo,
	getAllCaptures func() []*model.CaptureInfo,
) *ControllerOb {
	return &ControllerOb{
		storage:        storage,
		selfInfo:       selfInfo,
		getAllCaptures: getAllCaptures,
	}
}

func (c *ControllerOb) run(ctx context.Context) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			err := ctx.Err()
			log.Info("controller stops handle alive captures ", zap.String("capture", c.selfInfo.ID), zap.Error(err))
			return err
		case <-ticker.C:
		}

		if err := c.handleAliveCaptures(ctx); err != nil {
			log.Warn("controller handle alive captures fail", zap.String("capture", c.selfInfo.ID), zap.Error(err))
			return err
		}
	}
}

func (c *ControllerOb) handleAliveCaptures(ctx context.Context) error {
	alives := c.getAllCaptures()
	hash := sortAndHashCaptureList(alives)

	c.aliveCaptures.Lock()
	defer c.aliveCaptures.Unlock()
	c.aliveCaptures.incomingHash = hash
	c.aliveCaptures.incoming = alives
	return nil
}

func (c *ControllerOb) CreateChangefeed(cf *metadata.ChangefeedInfo, up *model.UpstreamInfo) (metadata.ChangefeedIDWithEpoch, error) {
	c.storage.entities.Lock()
	defer c.storage.entities.Unlock()

	if _, ok := c.storage.entities.cfs[cf.ID]; ok {
		return metadata.ChangefeedIDWithEpoch{}, errors.ErrChangeFeedAlreadyExists.GenWithStackByArgs(cf.ID)
	}

	c.storage.entities.cfs[cf.ID] = cf
	c.storage.entities.ups[up.ID] = up

	id := metadata.ChangefeedIDWithEpoch{ID: cf.ID, Epoch: c.storage.epoch.Add(1)}
	c.storage.entities.cfids[cf.ID] = id
	return id, nil
}

func (c *ControllerOb) RemoveChangefeed(cf metadata.ChangefeedIDWithEpoch) error {
	c.storage.entities.Lock()
	defer c.storage.entities.Unlock()
	c.storage.Lock()
	defer c.storage.Unlock()

	delete(c.storage.entities.cfids, cf.ID)
	delete(c.storage.entities.cfs, cf.ID)

	// TODO: we need to rollback partial changes on fails.
	if owner, ok := c.storage.schedule.owners[cf]; ok {
		owner.State = metadata.SchedRemoving
		if err := c.storage.setOwner(cf, owner); err != nil {
			return err
		}
	}
	if processors, ok := c.storage.schedule.processors[cf]; ok {
		if err := c.storage.setProcessors(cf, processors.v); err != nil {
			return err
		}
	}

	return nil
}

func (c *ControllerOb) RefreshCaptures() (captures []*model.CaptureInfo, changed bool) {
	c.aliveCaptures.Lock()
	defer c.aliveCaptures.Unlock()
	if c.aliveCaptures.outgoingHash != c.aliveCaptures.incomingHash {
		c.aliveCaptures.outgoingHash = c.aliveCaptures.incomingHash
		c.aliveCaptures.outgoing = c.aliveCaptures.incoming
	}
	captures = make([]*model.CaptureInfo, len(c.aliveCaptures.outgoing))
	copy(captures, c.aliveCaptures.outgoing)
	return
}

func (c *ControllerOb) SetOwner(cf metadata.ChangefeedIDWithEpoch, target metadata.ScheduledChangefeed) error {
	c.storage.Lock()
	defer c.storage.Unlock()
	return c.storage.setOwner(cf, target)
}

func (c *ControllerOb) SetProcessors(cf metadata.ChangefeedIDWithEpoch, workers []metadata.ScheduledChangefeed) error {
	c.storage.Lock()
	defer c.storage.Unlock()
	return c.storage.setProcessors(cf, workers)
}

func (c *ControllerOb) GetChangefeedSchedule(cf metadata.ChangefeedIDWithEpoch) (s metadata.ChangefeedSchedule, err error) {
	c.storage.RLock()
	defer c.storage.RUnlock()
	s.Owner = c.storage.schedule.owners[cf]
	s.Processors = c.storage.schedule.processors[cf].v
	return
}

func (c *ControllerOb) ScheduleSnapshot() (ss []metadata.ChangefeedSchedule, cs []*model.CaptureInfo, err error) {
	c.storage.RLock()
	ss = make([]metadata.ChangefeedSchedule, 0, len(c.storage.entities.cfids))
	for _, cf := range c.storage.entities.cfids {
		var s metadata.ChangefeedSchedule
		s.Owner = c.storage.schedule.owners[cf]
		s.Processors = c.storage.schedule.processors[cf].v
		ss = append(ss, s)
	}
	c.storage.RUnlock()
	cs = c.getAllCaptures()

	hash := sortAndHashCaptureList(cs)

	c.aliveCaptures.Lock()
	defer c.aliveCaptures.Unlock()
	c.aliveCaptures.outgoingHash = hash
	c.aliveCaptures.outgoing = cs
	return
}

type ownerOb struct {
	s  *Storage
	c  *metadata.ChangefeedInfo
	id metadata.ChangefeedIDWithEpoch

	processors struct {
		sync.Mutex
		outgoing []metadata.ScheduledChangefeed
		incoming []metadata.ScheduledChangefeed
	}
}

func NewOwnerDB(s *Storage,
	c *metadata.ChangefeedInfo,
	id metadata.ChangefeedIDWithEpoch) *ownerOb {
	return &ownerOb{
		s:  s,
		c:  c,
		id: id,
		processors: struct {
			sync.Mutex
			outgoing []metadata.ScheduledChangefeed
			incoming []metadata.ScheduledChangefeed
		}{
			outgoing: make([]metadata.ScheduledChangefeed, 0, 1),
			incoming: make([]metadata.ScheduledChangefeed, 0, 1),
		},
	}
}

func (o *ownerOb) Self() (*metadata.ChangefeedInfo, metadata.ChangefeedIDWithEpoch) {
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

func compareByChangefeed(a, b metadata.ScheduledChangefeed) int {
	return a.ChangefeedID.Compare(b.ChangefeedID)
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

func sortAndHashCaptureList(cs []*model.CaptureInfo) uint64 {
	hasher := fnv.New64()
	sort.Slice(cs, func(i, j int) bool { return strings.Compare(cs[i].ID, cs[j].ID) < 0 })
	for _, info := range cs {
		hasher.Write([]byte(info.ID))
	}
	return hasher.Sum64()
}
