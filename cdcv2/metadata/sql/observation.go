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

package sql

import (
	"context"
	"database/sql"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdcv2/metadata"
	ormUtil "github.com/pingcap/tiflow/engine/pkg/orm"
	"github.com/pingcap/tiflow/pkg/chann"
	"github.com/pingcap/tiflow/pkg/election"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/security"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"gorm.io/gorm"
)

var (
	_ metadata.Querier               = &CaptureOb[*gorm.DB]{}
	_ metadata.CaptureObservation    = &CaptureOb[*gorm.DB]{}
	_ metadata.ControllerObservation = &ControllerOb[*gorm.DB]{}
	_ metadata.OwnerObservation      = &OwnerOb[*gorm.DB]{}
)

// CaptureOb is an implement for metadata.CaptureObservation.
type CaptureOb[T TxnContext] struct {
	// election related fields.
	metadata.Elector
	selfInfo *model.CaptureInfo

	// TODO(CharlesCheung): handle ctx properly.
	egCtx         context.Context
	client        client[T]
	uuidGenerator uuidGenerator

	tasks *entity[metadata.ChangefeedUUID, *ScheduleDO]

	// TODO: remove processorChanges.
	ownerChanges     *chann.DrainableChann[metadata.ScheduledChangefeed]
	processorChanges *chann.DrainableChann[metadata.ScheduledChangefeed]
}

// NewCaptureObservation creates a capture observation.
func NewCaptureObservation(
	backendDB *sql.DB, selfInfo *model.CaptureInfo, opts ...ClientOptionFunc,
) (*CaptureOb[*gorm.DB], error) {
	db, err := ormUtil.NewGormDB(backendDB, "mysql")
	if err != nil {
		return nil, err
	}
	electionStorage, err := election.NewORMStorage(db, "election")
	if err != nil {
		return nil, errors.Trace(err)
	}
	ormClient := NewORMClient(selfInfo.ID, db)

	if err := AutoMigrate(db); err != nil {
		return nil, errors.Trace(err)
	}
	return &CaptureOb[*gorm.DB]{
		selfInfo:         selfInfo,
		client:           newClient(electionStorage, ormClient, opts...),
		uuidGenerator:    NewUUIDGenerator("orm", db),
		tasks:            newEntity[metadata.ChangefeedUUID, *ScheduleDO](defaultMaxExecTime),
		Elector:          metadata.NewElector(selfInfo, electionStorage),
		ownerChanges:     chann.NewAutoDrainChann[metadata.ScheduledChangefeed](),
		processorChanges: chann.NewAutoDrainChann[metadata.ScheduledChangefeed](),
	}, nil
}

// Run runs the given CaptureOb.
func (c *CaptureOb[T]) Run(
	egCtx context.Context,
	controllerCallback func(context.Context, metadata.ControllerObservation) error,
) (err error) {
	eg, egCtx := errgroup.WithContext(egCtx)
	c.egCtx = egCtx

	err = c.client.Txn(egCtx, func(tx T) error {
		return c.client.createProgress(tx, &ProgressDO{
			CaptureID: c.selfInfo.ID,
			Progress:  nil,
			Version:   1,
		})
	})
	if err != nil {
		return errors.Trace(err)
	}

	eg.Go(func() error {
		return c.Elector.RunElection(egCtx, c.onTakeControl(controllerCallback))
	})

	eg.Go(func() error {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-egCtx.Done():
				err := egCtx.Err()
				log.Info("capture stops handle task changes", zap.String("capture", c.selfInfo.ID), zap.Error(err))
				return err
			case <-ticker.C:
				if err := c.handleTaskChanges(egCtx); err != nil {
					log.Warn("capture handle task changes fail", zap.String("capture", c.selfInfo.ID), zap.Error(err))
					return err
				}
			}
		}
	})

	// TODO: add runWithEg function to reduce the wait goroutine.
	return eg.Wait()
}

func (c *CaptureOb[T]) onTakeControl(
	controllerCallback func(context.Context, metadata.ControllerObservation) error,
) func(context.Context) error {
	return func(ctx context.Context) error {
		controllerOb := newControllerObservation(c.client, c.uuidGenerator, c.selfInfo, c.getAllCaptures)

		eg, egCtx := errgroup.WithContext(ctx)
		eg.Go(func() error {
			return controllerOb.run(egCtx)
		})
		eg.Go(func() error {
			return controllerCallback(egCtx, controllerOb)
		})
		return eg.Wait()
	}
}

func (c *CaptureOb[T]) handleTaskChanges(ctx context.Context) error {
	var err error
	var schedItems []*ScheduleDO

	err = c.client.Txn(ctx, func(tx T) error {
		lastSafePoint := c.tasks.getSafePoint()
		schedItems, err = c.client.querySchedulesByOwnerIDAndUpdateAt(tx, c.Self().ID, lastSafePoint)
		return err
	})
	if err != nil {
		return errors.Trace(err)
	}

	if len(schedItems) == 0 {
		// No scheudle information for the capture.
		return nil
	}

	c.tasks.doUpsert(schedItems, func(newV *ScheduleDO) (skip bool) {
		if newV.OwnerState == metadata.SchedRemoved {
			return true
		}
		c.ownerChanges.In() <- newV.ScheduledChangefeed
		c.processorChanges.In() <- newV.ScheduledChangefeed
		return false
	})

	return nil
}

// Advance updates the progress of the capture.
func (c *CaptureOb[T]) Advance(cp metadata.CaptureProgress) error {
	return c.client.Txn(c.egCtx, func(tx T) error {
		pr, err := c.client.queryProgressByCaptureID(tx, c.selfInfo.ID)
		if err != nil {
			return errors.Trace(err)
		}
		if pr == nil {
			erMsg := fmt.Sprintf("expect non-empty progress for capture %s", c.selfInfo.ID)
			return errors.ErrMetaInvalidState.GenWithStackByArgs(erMsg)
		}
		return c.client.updateProgress(tx, &ProgressDO{
			CaptureID: c.selfInfo.ID,
			Progress:  &cp,
			Version:   pr.Version,
		})
	})
}

// OwnerChanges returns a channel that receives changefeeds when the owner of the changefeed changes.
func (c *CaptureOb[T]) OwnerChanges() <-chan metadata.ScheduledChangefeed {
	return c.ownerChanges.Out()
}

// OnOwnerLaunched is called when the owner of a changefeed is launched.
func (c *CaptureOb[T]) OnOwnerLaunched(cf metadata.ChangefeedUUID) metadata.OwnerObservation {
	return newOwnerObservation[T](c, cf)
}

// PostOwnerRemoved is called when the owner of a changefeed is removed.
func (c *CaptureOb[T]) PostOwnerRemoved(cf metadata.ChangefeedUUID, taskPosition metadata.ChangefeedProgress) error {
	sc := c.tasks.get(cf)
	if sc == nil {
		errMsg := fmt.Sprintf("remove owner for a changefeed %d that is not owned by the capture", cf)
		return errors.ErrInconsistentMetaCache.GenWithStackByArgs(errMsg)
	}

	sc.TaskPosition = taskPosition
	return c.client.TxnWithOwnerLock(c.egCtx, cf, func(tx T) error {
		return c.client.updateSchedule(tx, sc)
	})
}

// ProcessorChanges returns a channel that receives changefeeds when the changefeed changes.
func (c *CaptureOb[T]) ProcessorChanges() <-chan metadata.ScheduledChangefeed {
	return c.processorChanges.Out()
}

// GetChangefeed returns the changefeeds with the given UUIDs.
func (c *CaptureOb[T]) GetChangefeed(cfs ...metadata.ChangefeedUUID) (infos []*metadata.ChangefeedInfo, err error) {
	var cfDOs []*ChangefeedInfoDO
	err = c.client.Txn(c.egCtx, func(tx T) error {
		if len(cfs) == 0 {
			cfDOs, err = c.client.queryChangefeedInfos(tx)
			return err
		}
		cfDOs, err = c.client.queryChangefeedInfosByUUIDs(tx, cfs...)
		return err
	})
	if err != nil {
		return nil, errors.Trace(err)
	}

	for _, cfDO := range cfDOs {
		infos = append(infos, &cfDO.ChangefeedInfo)
	}
	return
}

// GetChangefeedState returns the state of the changefeed with the given UUID.
func (c *CaptureOb[T]) GetChangefeedState(cfs ...metadata.ChangefeedUUID) (states []*metadata.ChangefeedState, err error) {
	var cfDOs []*ChangefeedStateDO
	err = c.client.Txn(c.egCtx, func(tx T) error {
		if len(cfs) == 0 {
			cfDOs, err = c.client.queryChangefeedStates(tx)
			return err
		}
		cfDOs, err = c.client.queryChangefeedStateByUUIDs(tx, cfs...)
		return err
	})
	if err != nil {
		return nil, errors.Trace(err)
	}

	for _, cfDO := range cfDOs {
		states = append(states, &cfDO.ChangefeedState)
	}
	return
}

// GetChangefeedProgress returns the progress of the changefeed with the given UUID.
func (c *CaptureOb[T]) GetChangefeedProgress(
	cfs ...metadata.ChangefeedUUID,
) (progresses map[metadata.ChangefeedUUID]metadata.ChangefeedProgress, err error) {
	var prDOs []*ProgressDO
	var scDOs []*ScheduleDO
	err = c.client.Txn(c.egCtx, func(tx T) error {
		prDOs, err = c.client.queryProgresses(tx)
		if err != nil {
			return err
		}

		scDOs, err = c.client.querySchedules(tx)
		return err
	})
	if err != nil {
		return nil, errors.Trace(err)
	}

	cfMap := make(map[metadata.ChangefeedUUID]struct{})
	for _, cf := range cfs {
		cfMap[cf] = struct{}{}
	}
	queryAll := len(cfMap) == 0

	progresses = make(map[metadata.ChangefeedUUID]metadata.ChangefeedProgress)
	for _, prDO := range prDOs {
		if prDO.Progress != nil {
			for cf, pos := range *prDO.Progress {
				if _, ok := cfMap[cf]; ok || queryAll {
					progresses[cf] = pos
				}
			}
		}
	}

	if queryAll || len(progresses) < len(cfMap) {
		for _, scDO := range scDOs {
			if _, alreadyFound := progresses[scDO.ChangefeedUUID]; alreadyFound {
				continue
			}
			if _, ok := cfMap[scDO.ChangefeedUUID]; ok || queryAll {
				progresses[scDO.ChangefeedUUID] = scDO.TaskPosition
			}
		}
	}

	return
}

func (c *CaptureOb[T]) getAllCaptures() []*model.CaptureInfo {
	infos, _ := c.GetCaptures()
	return infos
}

// ControllerOb is an implement for metadata.ControllerObservation.
type ControllerOb[T TxnContext] struct {
	selfInfo *model.CaptureInfo
	client   client[T]

	// TODO(CharlesCheung): handle ctx properly.
	// egCtx is the inner ctx of elector.
	egCtx         context.Context
	uuidGenerator uuidGenerator

	aliveCaptures struct {
		sync.Mutex
		outgoing     []*model.CaptureInfo
		incoming     []*model.CaptureInfo
		outgoingHash uint64
		incomingHash uint64
	}

	getAllCaptures func() []*model.CaptureInfo
}

func newControllerObservation[T TxnContext](
	client client[T],
	uuidGenerator uuidGenerator,
	selfInfo *model.CaptureInfo,
	getAllCaptures func() []*model.CaptureInfo,
) *ControllerOb[T] {
	return &ControllerOb[T]{
		client:         client,
		selfInfo:       selfInfo,
		getAllCaptures: getAllCaptures,
		uuidGenerator:  uuidGenerator,
	}
}

func (c *ControllerOb[T]) run(ctx context.Context) error {
	c.egCtx = ctx
	if err := c.init(); err != nil {
		return errors.Trace(err)
	}

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

		if err := c.handleAliveCaptures(); err != nil {
			log.Warn("controller handle alive captures fail", zap.String("capture", c.selfInfo.ID), zap.Error(err))
			return err
		}
	}
}

func (c *ControllerOb[T]) init() error {
	var (
		captureOfflined   []model.CaptureID
		captureOnline     = make(map[model.CaptureID]struct{})
		capturesScheduled []model.CaptureID
		err               error
	)
	err = c.client.Txn(c.egCtx, func(tx T) error {
		capturesScheduled, err = c.client.querySchedulesUinqueOwnerIDs(tx)
		return err
	})
	if err != nil {
		return errors.Trace(err)
	}

	currentCaptures := c.getAllCaptures()
	for _, capture := range currentCaptures {
		captureOnline[capture.ID] = struct{}{}
	}

	for _, captureID := range capturesScheduled {
		if _, ok := captureOnline[captureID]; !ok {
			captureOfflined = append(captureOfflined, captureID)
		}
	}
	return c.onCaptureOffline(captureOfflined...)
}

func (c *ControllerOb[T]) handleAliveCaptures() error {
	alives := c.getAllCaptures()
	hash := sortAndHashCaptureList(alives)

	c.aliveCaptures.Lock()
	defer c.aliveCaptures.Unlock()
	c.aliveCaptures.incomingHash = hash
	c.aliveCaptures.incoming = alives
	return nil
}

func (c *ControllerOb[T]) upsertUpstream(tx T, up *model.UpstreamInfo) error {
	newUp := &UpstreamDO{
		ID:        up.ID,
		Endpoints: up.PDEndpoints,
		Config: &security.Credential{
			CAPath:        up.CAPath,
			CertPath:      up.CertPath,
			KeyPath:       up.KeyPath,
			CertAllowedCN: up.CertAllowedCN,
		},
		Version: 1,
	}

	// Create or update the upstream info.
	oldUp, err := c.client.queryUpstreamByID(tx, up.ID)
	if errors.Is(errors.Cause(err), gorm.ErrRecordNotFound) {
		return c.client.createUpstream(tx, newUp)
	} else if err != nil {
		return errors.Trace(err)
	}

	if oldUp == nil {
		errMsg := fmt.Sprintf("expect non-empty upstream info for id %d", up.ID)
		return errors.Trace(errors.ErrMetaInvalidState.GenWithStackByArgs(errMsg))
	}
	if !oldUp.equal(newUp) {
		newUp.Version = oldUp.Version
		if err := c.client.updateUpstream(tx, newUp); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (c *ControllerOb[T]) txnWithLeaderLock(fn func(T) error) error {
	return c.client.TxnWithLeaderLock(c.egCtx, c.selfInfo.ID, fn)
}

// CreateChangefeed initializes the changefeed info, schedule info and state info of the given changefeed. It also
// updates or creates the upstream info depending on whether the upstream info exists.
func (c *ControllerOb[T]) CreateChangefeed(cf *metadata.ChangefeedInfo, up *model.UpstreamInfo) (metadata.ChangefeedIdent, error) {
	if cf.UpstreamID != up.ID {
		errMsg := fmt.Sprintf("changefeed %s has different upstream id %d from the given upstream id %d",
			cf.ChangefeedIdent, cf.UpstreamID, up.ID)
		return cf.ChangefeedIdent, errors.ErrMetaInvalidState.GenWithStackByArgs(errMsg)
	}
	uuid, err := c.uuidGenerator.GenChangefeedUUID(c.egCtx)
	if err != nil {
		return cf.ChangefeedIdent, errors.Trace(err)
	}

	cf.ChangefeedIdent.UUID = uuid
	err = c.txnWithLeaderLock(func(tx T) error {
		if err := c.upsertUpstream(tx, up); err != nil {
			return errors.Trace(err)
		}

		if err := c.client.createChangefeedInfo(tx, &ChangefeedInfoDO{
			ChangefeedInfo: *cf,
			Version:        1,
		}); err != nil {
			return errors.Trace(err)
		}

		if err := c.client.createSchedule(tx, &ScheduleDO{
			ScheduledChangefeed: metadata.ScheduledChangefeed{
				ChangefeedUUID: cf.UUID,
				Owner:          nil,
				OwnerState:     metadata.SchedRemoved,
				Processors:     nil,
				TaskPosition: metadata.ChangefeedProgress{
					CheckpointTs:      cf.StartTs,
					MinTableBarrierTs: cf.StartTs,
				},
			},
			Version: 1,
		}); err != nil {
			return errors.Trace(err)
		}

		return c.client.createChangefeedState(tx, &ChangefeedStateDO{
			ChangefeedState: metadata.ChangefeedState{
				ChangefeedUUID: cf.UUID,
				State:          model.StateUnInitialized,
				Error:          nil,
				Warning:        nil,
			},
			Version: 1,
		})
	})
	return cf.ChangefeedIdent, err
}

// RemoveChangefeed removes the changefeed info
func (c *ControllerOb[T]) RemoveChangefeed(cf metadata.ChangefeedUUID) error {
	return c.txnWithLeaderLock(func(tx T) error {
		oldInfo, err := c.client.queryChangefeedInfoByUUID(tx, cf)
		if err != nil {
			return errors.Trace(err)
		}
		err = c.client.markChangefeedRemoved(tx, &ChangefeedInfoDO{
			ChangefeedInfo: metadata.ChangefeedInfo{
				ChangefeedIdent: metadata.ChangefeedIdent{
					UUID: cf,
				},
			},
			Version: oldInfo.Version,
		})
		if err != nil {
			return errors.Trace(err)
		}

		sc, err := c.client.queryScheduleByUUID(tx, cf)
		if err != nil {
			return errors.Trace(err)
		}
		if sc.OwnerState == metadata.SchedLaunched {
			err = c.client.updateScheduleOwnerState(tx, &ScheduleDO{
				ScheduledChangefeed: metadata.ScheduledChangefeed{
					ChangefeedUUID: cf,
					OwnerState:     metadata.SchedRemoving,
				},
				Version: sc.Version,
			})
			if err != nil {
				return errors.Trace(err)
			}
		}
		return nil
	})
}

// CleanupChangefeed removes the changefeed info, schedule info and state info of the given changefeed.
// Note that this function should only be called when the owner is removed and changefeed is marked as removed.
func (c *ControllerOb[T]) CleanupChangefeed(cf metadata.ChangefeedUUID) error {
	return c.txnWithLeaderLock(func(tx T) error {
		err := c.client.deleteChangefeedInfo(tx, &ChangefeedInfoDO{
			ChangefeedInfo: metadata.ChangefeedInfo{
				ChangefeedIdent: metadata.ChangefeedIdent{
					UUID: cf,
				},
			},
		})
		if err != nil {
			return errors.Trace(err)
		}

		err = c.client.deleteChangefeedState(tx, &ChangefeedStateDO{
			ChangefeedState: metadata.ChangefeedState{
				ChangefeedUUID: cf,
			},
		})
		if err != nil {
			return errors.Trace(err)
		}

		err = c.client.deleteSchedule(tx, &ScheduleDO{
			ScheduledChangefeed: metadata.ScheduledChangefeed{
				ChangefeedUUID: cf,
			},
		})
		if err != nil {
			return errors.Trace(err)
		}

		return nil
	})
}

// RefreshCaptures Fetch the latest capture list in the TiCDC cluster.
func (c *ControllerOb[T]) RefreshCaptures() (captures []*model.CaptureInfo, changed bool) {
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

// onCaptureOffline is called when a capture is offline.
func (c *ControllerOb[T]) onCaptureOffline(ids ...model.CaptureID) error {
	// TODO(CharlesCheung): use multiple statements to reduce the number of round trips.
	// Note currently we only handle single capture offline, so it is not a big deal.
	for _, id := range ids {
		err := c.txnWithLeaderLock(func(tx T) error {
			prs, err := c.client.queryProgressByCaptureIDsWithLock(tx, []model.CaptureID{id})
			if err != nil {
				return errors.Trace(err)
			}
			prMap := prs[0]
			if prMap.Progress == nil || len(*prMap.Progress) == 0 {
				log.Info("no progress is updated by the capture", zap.String("capture", id))
			} else {
				for cf, taskPosition := range *prMap.Progress {
					// TODO(CharlesCheung): maybe such operation should be done in background.
					oldSc, err := c.client.queryScheduleByUUID(tx, cf)
					if err != nil {
						return errors.Trace(err)
					}
					if *oldSc.Owner != id || oldSc.OwnerState == metadata.SchedRemoved {
						continue
					}
					newSc := &ScheduleDO{
						ScheduledChangefeed: metadata.ScheduledChangefeed{
							ChangefeedUUID: oldSc.ChangefeedUUID,
							Owner:          nil,
							OwnerState:     metadata.SchedRemoved,
							Processors:     nil,
							TaskPosition:   taskPosition,
						},
						Version: oldSc.Version,
					}
					// TODO: use Model to prevent nil value from being ignored.
					err = c.client.updateSchedule(tx, newSc)
					if err != nil {
						return errors.Trace(err)
					}
				}
			}

			// If capture fails before a owner progress is updated for the first time, the corresponding
			// taskPosition will not stored in Progress. In this case, we also need to set owner removed.
			// Version would not be checked here because multiple rows may be updated.
			err = c.client.updateScheduleOwnerStateByOwnerID(tx, metadata.SchedRemoved, id)
			if err != nil {
				return errors.Trace(err)
			}

			return c.client.deleteProgress(tx, prMap)
		})
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// SetOwner Schedule a changefeed owner to a given target.
func (c *ControllerOb[T]) SetOwner(target metadata.ScheduledChangefeed) error {
	return c.txnWithLeaderLock(func(tx T) error {
		old, err := c.client.queryScheduleByUUID(tx, target.ChangefeedUUID)
		if err != nil {
			return errors.Trace(err)
		}
		if err := metadata.CheckScheduleState(old.ScheduledChangefeed, target); err != nil {
			return errors.Trace(err)
		}
		return c.client.updateSchedule(tx, &ScheduleDO{
			ScheduledChangefeed: target,
			Version:             old.Version,
		})
	})
}

// GetChangefeedSchedule Get current schedule of the given changefeed.
func (c *ControllerOb[T]) GetChangefeedSchedule(cf metadata.ChangefeedUUID) (metadata.ScheduledChangefeed, error) {
	var ret metadata.ScheduledChangefeed
	err := c.client.Txn(c.egCtx, func(tx T) error {
		sc, inErr := c.client.queryScheduleByUUID(tx, cf)
		if inErr != nil {
			return errors.Trace(inErr)
		}
		ret = sc.ScheduledChangefeed
		return nil
	})
	return ret, err
}

// ScheduleSnapshot Get a snapshot of all changefeeds current schedule.
func (c *ControllerOb[T]) ScheduleSnapshot() (ss []metadata.ScheduledChangefeed, cs []*model.CaptureInfo, err error) {
	err = c.client.Txn(c.egCtx, func(tx T) error {
		scs, inErr := c.client.querySchedules(tx)
		if inErr != nil {
			return errors.Trace(inErr)
		}
		for _, sc := range scs {
			ss = append(ss, sc.ScheduledChangefeed)
		}
		return err
	})

	cs = c.getAllCaptures()
	hash := sortAndHashCaptureList(cs)
	c.aliveCaptures.Lock()
	defer c.aliveCaptures.Unlock()
	c.aliveCaptures.outgoingHash = hash
	c.aliveCaptures.outgoing = cs
	return
}

// OwnerOb is an implement for metadata.OwnerObservation.
type OwnerOb[T TxnContext] struct {
	egCtx  context.Context
	client client[T]
	cfUUID metadata.ChangefeedUUID
}

func newOwnerObservation[T TxnContext](c *CaptureOb[T], cf metadata.ChangefeedUUID) *OwnerOb[T] {
	return &OwnerOb[T]{
		egCtx:  c.egCtx,
		client: c.client,
		cfUUID: cf,
	}
}

// Self returns the changefeed info of the owner.
// nolint:unused
func (o *OwnerOb[T]) Self() metadata.ChangefeedUUID {
	return o.cfUUID
}

func (o *OwnerOb[T]) updateChangefeedState(
	state model.FeedState,
	cfErr *model.RunningError,
	cfWarn *model.RunningError,
) error {
	return o.client.TxnWithOwnerLock(o.egCtx, o.cfUUID, func(tx T) error {
		oldState, err := o.client.queryChangefeedStateByUUID(tx, o.cfUUID)
		if err != nil {
			return errors.Trace(err)
		}

		newState := &ChangefeedStateDO{
			ChangefeedState: metadata.ChangefeedState{
				ChangefeedUUID: o.cfUUID,
				State:          state,
				Error:          oldState.Error,
				Warning:        oldState.Warning,
			},
			Version: oldState.Version,
		}
		if cfErr != nil {
			newState.Error = cfErr
		}
		if cfWarn != nil {
			newState.Warning = cfWarn
		}

		return o.client.updateChangefeedState(tx, newState)
	})
}

// UpdateChangefeed updates changefeed metadata, must be called on a paused one.
// nolint:unused
func (o *OwnerOb[T]) UpdateChangefeed(info *metadata.ChangefeedInfo) error {
	return o.client.TxnWithOwnerLock(o.egCtx, o.cfUUID, func(tx T) error {
		state, err := o.client.queryChangefeedStateByUUIDWithLock(tx, o.cfUUID)
		if err != nil {
			return errors.Trace(err)
		}
		if state.State != model.StateStopped && state.State != model.StateFailed {
			return errors.ErrChangefeedUpdateRefused.GenWithStackByArgs(
				"can only update changefeed config when it is stopped or failed",
			)
		}

		oldInfo, err := o.client.queryChangefeedInfoByUUID(tx, o.cfUUID)
		if err != nil {
			return errors.Trace(err)
		}
		return o.client.updateChangefeedInfo(tx, &ChangefeedInfoDO{
			ChangefeedInfo: *info,
			Version:        oldInfo.Version,
		})
	})
}

// ResumeChangefeed resumes a changefeed.
// nolint:unused
func (o *OwnerOb[T]) ResumeChangefeed() error {
	return o.updateChangefeedState(model.StateNormal, nil, nil)
}

// SetChangefeedPending sets the changefeed to state pending.
// nolint:unused
func (o *OwnerOb[T]) SetChangefeedPending(err *model.RunningError) error {
	return o.updateChangefeedState(model.StatePending, err, nil)
}

// SetChangefeedFailed set the changefeed to state failed.
// nolint:unused
func (o *OwnerOb[T]) SetChangefeedFailed(err *model.RunningError) error {
	return o.updateChangefeedState(model.StateFailed, err, nil)
}

// PauseChangefeed pauses a changefeed.
// nolint:unused
func (o *OwnerOb[T]) PauseChangefeed() error {
	return o.updateChangefeedState(model.StateStopped, nil, nil)
}

// SetChangefeedRemoved set the changefeed to state removed.
// nolint:unused
func (o *OwnerOb[T]) SetChangefeedRemoved() error {
	return o.updateChangefeedState(model.StateRemoved, nil, nil)
}

// SetChangefeedFinished set the changefeed to state finished.
// nolint:unused
func (o *OwnerOb[T]) SetChangefeedFinished() error {
	return o.updateChangefeedState(model.StateFinished, nil, nil)
}

// SetChangefeedWarning set the changefeed to state warning.
// nolint:unused
func (o *OwnerOb[T]) SetChangefeedWarning(warn *model.RunningError) error {
	return o.updateChangefeedState(model.StateWarning, nil, warn)
}

func sortAndHashCaptureList(cs []*model.CaptureInfo) uint64 {
	hasher := fnv.New64()
	sort.Slice(cs, func(i, j int) bool { return strings.Compare(cs[i].ID, cs[j].ID) < 0 })
	for _, info := range cs {
		hasher.Write([]byte(info.ID))
	}
	return hasher.Sum64()
}
