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

	"github.com/ngaut/log"
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

// CaptureOb is an implement for metadata.CaptureObservation.
type CaptureOb[T TxnContext] struct {
	// election related fields.
	metadata.Elector
	selfInfo *model.CaptureInfo
	// TODO(CharlesCheung): handle ctx properly.
	egCtx context.Context

	client client[T]

	tasks *entity[metadata.ChangefeedUUID, *ScheduleDO]

	// TODO: remove processorChanges.
	ownerChanges     *chann.DrainableChann[metadata.ScheduledChangefeed]
	processorChanges *chann.DrainableChann[metadata.ScheduledChangefeed]
}

// NewCaptureObservation creates a capture observation.
func NewCaptureObservation(
	backendDB *sql.DB, selfInfo *model.CaptureInfo,
) (*CaptureOb[*gorm.DB], error) {
	db, err := ormUtil.NewGormDB(backendDB, "mysql")
	if err != nil {
		return nil, err
	}
	electionStorage, err := election.NewORMStorage(db, "election")
	if err != nil {
		return nil, errors.Trace(err)
	}
	if err := AutoMigrate(db); err != nil {
		return nil, errors.Trace(err)
	}
	return &CaptureOb[*gorm.DB]{
		selfInfo:         selfInfo,
		client:           NewORMClient(selfInfo.ID, db),
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
		checker, ok := c.Elector.(LeaderChecker[T])
		if !ok {
			return errors.New("capture elector is not a leader checker")
		}
		controllerOb := newControllerObservation(checker, c.client, c.selfInfo, c.getAllCaptures)

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

func (c *CaptureOb[T]) Advance(cp metadata.CaptureProgress) error {
	return c.client.Txn(c.egCtx, func(tx T) error {
		return c.client.updateProgress(tx, &ProgressDO{
			CaptureID: c.selfInfo.ID,
			Progress:  &cp,
		})
	})
}

func (c *CaptureOb[T]) OwnerChanges() <-chan metadata.ScheduledChangefeed {
	return c.ownerChanges.Out()
}

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

func (c *CaptureOb[T]) ProcessorChanges() <-chan metadata.ScheduledChangefeed {
	return c.processorChanges.Out()
}

func (c *CaptureOb[T]) GetChangefeeds(cfs ...metadata.ChangefeedUUID) (infos []*metadata.ChangefeedInfo, err error) {
	var cfDOs []*ChangefeedInfoDO
	err = c.client.Txn(c.egCtx, func(tx T) error {
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

func (c *CaptureOb[T]) getAllCaptures() []*model.CaptureInfo {
	infos, _ := c.GetCaptures()
	return infos
}

// ControllerOb is an implement for metadata.ControllerObservation.
type ControllerOb[T TxnContext] struct {
	selfInfo      *model.CaptureInfo
	leaderChecker LeaderChecker[T]
	client        client[T]

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
	leaderChecker LeaderChecker[T],
	client client[T],
	selfInfo *model.CaptureInfo,
	getAllCaptures func() []*model.CaptureInfo,
) *ControllerOb[T] {
	return &ControllerOb[T]{
		leaderChecker:  leaderChecker,
		client:         client,
		selfInfo:       selfInfo,
		getAllCaptures: getAllCaptures,
		uuidGenerator:  NewUUIDGenerator(),
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

		if err := c.handleAliveCaptures(ctx); err != nil {
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

func (c *ControllerOb[T]) handleAliveCaptures(ctx context.Context) error {
	alives := c.getAllCaptures()
	hash := sortAndHashCaptureList(alives)

	c.aliveCaptures.Lock()
	defer c.aliveCaptures.Unlock()
	c.aliveCaptures.incomingHash = hash
	c.aliveCaptures.incoming = alives
	return nil
}

// CreateChangefeed initializes the changefeed info, schedule info and state info of the given changefeed. It also
// updates or creates the upstream info depending on whether the upstream info exists.
func (c *ControllerOb[T]) CreateChangefeed(cf *metadata.ChangefeedInfo, up *model.UpstreamInfo) (metadata.ChangefeedIdent, error) {
	cf.ChangefeedIdent.UUID = c.uuidGenerator.GenChangefeedUUID()

	err := c.leaderChecker.TxnWithLeaderLock(c.egCtx, c.selfInfo.ID, func(tx T) error {
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

		oldUp, err := c.client.queryUpstreamByID(tx, up.ID)
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.client.createUpstream(tx, newUp)
		} else if err != nil {
			return errors.Trace(err)
		}

		if !oldUp.equal(newUp) {
			newUp.Version = oldUp.Version
			c.client.updateUpstream(tx, newUp)
		}

		err = c.client.createChangefeedInfo(tx, &ChangefeedInfoDO{
			ChangefeedInfo: *cf,
			Version:        1,
		})
		if err != nil {
			return errors.Trace(err)
		}

		err = c.client.createSchedule(tx, &ScheduleDO{
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
		})
		if err != nil {
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

func (c *ControllerOb[T]) RemoveChangefeed(cf metadata.ChangefeedUUID) error {
	return c.leaderChecker.TxnWithLeaderLock(c.egCtx, c.selfInfo.ID, func(tx T) error {
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
	return c.leaderChecker.TxnWithLeaderLock(c.egCtx, c.selfInfo.ID, func(tx T) error {
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
		err := c.leaderChecker.TxnWithLeaderLock(c.egCtx, c.selfInfo.ID, func(tx T) error {
			prs, err := c.client.queryProgressByCaptureIDsWithLock(tx, []model.CaptureID{id})
			if err != nil {
				return errors.Trace(err)
			}
			prMap := prs[0]
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
				}
				// TODO: use Model to prevent nil value from being ignored.
				err = c.client.updateSchedule(tx, newSc)
				if err != nil {
					return errors.Trace(err)
				}
			}

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

func (c *ControllerOb[T]) SetOwner(target metadata.ScheduledChangefeed) error {
	return c.leaderChecker.TxnWithLeaderLock(c.egCtx, c.selfInfo.ID, func(tx T) error {
		old, err := c.client.queryScheduleByUUID(tx, target.ChangefeedUUID)
		if err != nil {
			return errors.Trace(err)
		}
		if err := metadata.CheckScheduleState(old.ScheduledChangefeed, target); err != nil {
			return errors.Trace(err)
		}
		return c.client.updateScheduleOwnerState(tx, &ScheduleDO{
			ScheduledChangefeed: target,
			Version:             old.Version,
		})
	})
}

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

type ownerOb[T TxnContext] struct {
	egCtx    context.Context
	client   client[T]
	selfInfo *model.CaptureInfo
	cf       *metadata.ChangefeedInfo
}

func (o *ownerOb[T]) Self() *metadata.ChangefeedInfo {
	return o.cf
}

func (o *ownerOb[T]) updateChangefeedState(
	state model.FeedState,
	cfErr *model.RunningError,
	cfWarn *model.RunningError,
) error {
	return o.client.TxnWithOwnerLock(o.egCtx, o.cf.UUID, func(tx T) error {
		oldState, err := o.client.queryChangefeedStateByUUID(tx, o.cf.UUID)
		if err != nil {
			return errors.Trace(err)
		}

		newState := &ChangefeedStateDO{
			ChangefeedState: metadata.ChangefeedState{
				ChangefeedUUID: o.cf.UUID,
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

func (o *ownerOb[T]) UpdateChangefeed(info *metadata.ChangefeedInfo) error {
	return o.client.TxnWithOwnerLock(o.egCtx, o.cf.UUID, func(tx T) error {
		state, err := o.client.queryChangefeedStateByUUIDWithLock(tx, o.cf.UUID)
		if err != nil {
			return errors.Trace(err)
		}
		if state.State != model.StateStopped && state.State != model.StateFailed {
			return errors.ErrChangefeedUpdateRefused.GenWithStackByArgs(
				"can only update changefeed config when it is stopped or failed",
			)
		}

		oldInfo, err := o.client.queryChangefeedInfoByUUID(tx, o.cf.UUID)
		if err != nil {
			return errors.Trace(err)
		}
		return o.client.updateChangefeedInfo(tx, &ChangefeedInfoDO{
			ChangefeedInfo: *info,
			Version:        oldInfo.Version,
		})
	})
}

func (o *ownerOb[T]) ResumeChangefeed() error {
	return o.updateChangefeedState(model.StateNormal, nil, nil)
}

func (o *ownerOb[T]) SetChangefeedPending(err *model.RunningError) error {
	return o.updateChangefeedState(model.StatePending, err, nil)
}

func (o *ownerOb[T]) SetChangefeedFailed(err *model.RunningError) error {
	return o.updateChangefeedState(model.StateFailed, err, nil)
}

func (o *ownerOb[T]) PauseChangefeed() error {
	return o.updateChangefeedState(model.StateStopped, nil, nil)
}

func (o *ownerOb[T]) SetChangefeedRemoved() error {
	return o.updateChangefeedState(model.StateRemoved, nil, nil)
}

func (o *ownerOb[T]) SetChangefeedFinished() error {
	return o.updateChangefeedState(model.StateFinished, nil, nil)
}

func (o *ownerOb[T]) SetChangefeedWarning(warn *model.RunningError) error {
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
