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
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdcv2/metadata"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	_ checker[*gorm.DB] = &ormClient{}

	_ upstreamClient[*gorm.DB]        = &ormClient{}
	_ changefeedInfoClient[*gorm.DB]  = &ormClient{}
	_ changefeedStateClient[*gorm.DB] = &ormClient{}
	_ scheduleClient[*gorm.DB]        = &ormClient{}
	_ progressClient[*gorm.DB]        = &ormClient{}
)

type ormClient struct {
	selfID model.CaptureID
	db     *gorm.DB
}

// NewORMClient creates a new ORM client.
func NewORMClient(selfID model.CaptureID, db *gorm.DB) *ormClient {
	return &ormClient{
		selfID: selfID,
		db:     db,
	}
}

// Txn executes the given transaction action in a transaction.
func (c *ormClient) Txn(ctx context.Context, fn ormTxnAction) error {
	return c.db.WithContext(ctx).Transaction(fn)
}

// TxnWithOwnerLock executes the given transaction action in a transaction with owner lock.
func (c *ormClient) TxnWithOwnerLock(ctx context.Context, uuid metadata.ChangefeedUUID, fn ormTxnAction) error {
	return c.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		sc := &ScheduleDO{}
		ret := tx.Where("changefeed_uuid = ? and owner = ? and owner_state != ?", uuid, c.selfID, metadata.SchedRemoved).
			Clauses(clause.Locking{
				Strength: "SHARE",
				Table:    clause.Table{Name: clause.CurrentTable},
			}).Limit(1).Find(&sc)
		if err := handleSingleOpErr(ret, 1, "TxnWithOwnerLock"); err != nil {
			return errors.Trace(err)
		}
		return fn(tx)
	})
}

// ================================ Upstream Client =================================

// createUpstream implements the upstreamClient interface.
func (c *ormClient) createUpstream(tx *gorm.DB, up *UpstreamDO) error {
	ret := tx.Create(up)
	if err := handleSingleOpErr(ret, 1, "CreateUpstream"); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// deleteUpstream implements the upstreamClient interface.
func (c *ormClient) deleteUpstream(tx *gorm.DB, up *UpstreamDO) error {
	ret := tx.Delete(up)
	if err := handleSingleOpErr(ret, 1, "DeleteUpstream"); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// updateUpstream implements the upstreamClient interface.
func (c *ormClient) updateUpstream(tx *gorm.DB, up *UpstreamDO) error {
	ret := tx.Where("id = ? and version = ?", up.ID, up.Version).
		Updates(UpstreamDO{
			Endpoints: up.Endpoints,
			Config:    up.Config,
			Version:   up.Version + 1,
		})
	if err := handleSingleOpErr(ret, 1, "UpdateUpstream"); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// queryUpstreams implements the upstreamClient interface.
//
//nolint:unused
func (c *ormClient) queryUpstreams(tx *gorm.DB) ([]*UpstreamDO, error) {
	ups := []*UpstreamDO{}
	ret := tx.Find(&ups)
	if err := handleSingleOpErr(ret, -1, "QueryUpstreams"); err != nil {
		return nil, errors.Trace(err)
	}
	return ups, nil
}

// queryUpstreamsByUpdateAt implements the upstreamClient interface.
//
//nolint:unused
func (c *ormClient) queryUpstreamsByUpdateAt(tx *gorm.DB, lastUpdateAt time.Time) ([]*UpstreamDO, error) {
	ups := []*UpstreamDO{}
	ret := tx.Where("update_at > ?", lastUpdateAt).Find(&ups)
	if err := handleSingleOpErr(ret, -1, "QueryUpstreamsByUpdateAt"); err != nil {
		return nil, errors.Trace(err)
	}
	return ups, nil
}

// queryUpstreamByID implements the upstreamClient interface.
//
//nolint:unused
func (c *ormClient) queryUpstreamByID(tx *gorm.DB, id uint64) (*UpstreamDO, error) {
	up := &UpstreamDO{}
	ret := tx.Where("id = ?", id).First(up)
	if err := handleSingleOpErr(ret, 1, "QueryUpstreamsByUpdateAt"); err != nil {
		return nil, errors.Trace(err)
	}
	return up, nil
}

// ================================ ChangefeedInfo Client =================================

// createChangefeedInfo implements the changefeedInfoClient interface.
func (c *ormClient) createChangefeedInfo(tx *gorm.DB, info *ChangefeedInfoDO) error {
	ret := tx.Create(info)
	if err := handleSingleOpErr(ret, 1, "CreateChangefeedInfo"); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// deleteChangefeedInfo implements the changefeedInfoClient interface.
func (c *ormClient) deleteChangefeedInfo(tx *gorm.DB, info *ChangefeedInfoDO) error {
	ret := tx.Delete(info)
	if err := handleSingleOpErr(ret, 1, "DeleteChangefeedInfo"); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// markChangefeedRemoved implements the changefeedInfoClient interface.
//
//nolint:unused
func (c *ormClient) markChangefeedRemoved(tx *gorm.DB, info *ChangefeedInfoDO) error {
	// TODO: maybe we should usethe mysql function `now(6)` to get the current time.
	removeTime := time.Now()
	ret := tx.Where("uuid = ? and version = ?", info.UUID, info.Version).
		Updates(ChangefeedInfoDO{
			RemovedAt: &removeTime,
			Version:   info.Version + 1,
		})
	if err := handleSingleOpErr(ret, 1, "markChangefeedRemoved"); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// updateChangefeedInfo implements the changefeedInfoClient interface.
func (c *ormClient) updateChangefeedInfo(tx *gorm.DB, info *ChangefeedInfoDO) error {
	ret := tx.Where("uuid = ? and version = ?", info.UUID, info.Version).
		Updates(&ChangefeedInfoDO{
			ChangefeedInfo: metadata.ChangefeedInfo{
				SinkURI:  info.SinkURI,
				StartTs:  info.StartTs,
				TargetTs: info.TargetTs,
				Config:   info.Config,
			},
			Version: info.Version + 1,
		})
	if err := handleSingleOpErr(ret, 1, "UpdateChangefeedInfo"); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// queryChangefeedInfos implements the changefeedInfoClient interface.
//
//nolint:unused
func (c *ormClient) queryChangefeedInfos(tx *gorm.DB) ([]*ChangefeedInfoDO, error) {
	infos := []*ChangefeedInfoDO{}
	ret := tx.Find(&infos)
	if err := handleSingleOpErr(ret, -1, "QueryChangefeedInfos"); err != nil {
		return nil, errors.Trace(err)
	}
	return infos, nil
}

// queryChangefeedInfosByUpdateAt implements the changefeedInfoClient interface.
// TODO(CharlesCheung): query data before lastUpdateAt to avoid data loss.
//
//nolint:unused
func (c *ormClient) queryChangefeedInfosByUpdateAt(tx *gorm.DB, lastUpdateAt time.Time) ([]*ChangefeedInfoDO, error) {
	infos := []*ChangefeedInfoDO{}
	ret := tx.Where("update_at > ?", lastUpdateAt).Find(&infos)
	if err := handleSingleOpErr(ret, -1, "QueryChangefeedInfosByUpdateAt"); err != nil {
		return nil, errors.Trace(err)
	}
	return infos, nil
}

// queryChangefeedInfosByUUIDs implements the changefeedInfoClient interface.
// nolint:unused
func (c *ormClient) queryChangefeedInfosByUUIDs(tx *gorm.DB, uuids ...uint64) ([]*ChangefeedInfoDO, error) {
	infos := []*ChangefeedInfoDO{}
	ret := tx.Where("uuid in (?)", uuids).Find(&infos)
	if err := handleSingleOpErr(ret, int64(len(uuids)), "QueryChangefeedInfosByUUIDs"); err != nil {
		// TODO: optimize the behavior when some uuids are not found.
		return infos, errors.Trace(err)
	}
	return infos, nil
}

// queryChangefeedInfoByUUID implements the changefeedInfoClient interface.
//
//nolint:unused
func (c *ormClient) queryChangefeedInfoByUUID(tx *gorm.DB, uuid uint64) (*ChangefeedInfoDO, error) {
	info := &ChangefeedInfoDO{}
	ret := tx.Where("uuid = ?", uuid).First(info)

	// TODO(CharlesCheung): handle record not found error.
	if err := handleSingleOpErr(ret, 1, "QueryChangefeedInfoByUUID"); err != nil {
		return nil, errors.Trace(err)
	}
	return info, nil
}

// ================================ ChangefeedState Client =================================

// createChangefeedState implements the changefeedStateClient interface.
func (c *ormClient) createChangefeedState(tx *gorm.DB, state *ChangefeedStateDO) error {
	ret := tx.Create(state)
	if err := handleSingleOpErr(ret, 1, "CreateChangefeedState"); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// deleteChangefeedState implements the changefeedStateClient interface.
func (c *ormClient) deleteChangefeedState(tx *gorm.DB, state *ChangefeedStateDO) error {
	ret := tx.Delete(state)
	if err := handleSingleOpErr(ret, 1, "DeleteChangefeedState"); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// changefeedStateModel is used to update changefeed state, which prevents
// nil values (error and warning) from being ignored.
var changefeedStateModel = &ChangefeedStateDO{}

// updateChangefeedState implements the changefeedStateClient interface.
func (c *ormClient) updateChangefeedState(tx *gorm.DB, state *ChangefeedStateDO) error {
	ret := tx.Model(changefeedStateModel).Select("state", "warning", "error", "version").
		Where("changefeed_uuid = ? and version = ?", state.ChangefeedUUID, state.Version).
		Updates(ChangefeedStateDO{
			ChangefeedState: metadata.ChangefeedState{
				State:   state.State,
				Warning: state.Warning,
				Error:   state.Error,
			},
			Version: state.Version + 1,
		})
	if err := handleSingleOpErr(ret, 1, "updateChangefeedState"); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// queryChangefeedStates implements the changefeedStateClient interface.
//
//nolint:unused
func (c *ormClient) queryChangefeedStates(tx *gorm.DB) ([]*ChangefeedStateDO, error) {
	states := []*ChangefeedStateDO{}
	ret := tx.Find(&states)
	if err := handleSingleOpErr(ret, -1, "QueryChangefeedStates"); err != nil {
		return nil, errors.Trace(err)
	}
	return states, nil
}

// queryChangefeedStatesByUpdateAt implements the changefeedStateClient interface.
//
//nolint:unused
func (c *ormClient) queryChangefeedStatesByUpdateAt(tx *gorm.DB, lastUpdateAt time.Time) ([]*ChangefeedStateDO, error) {
	states := []*ChangefeedStateDO{}
	ret := tx.Where("update_at > ?", lastUpdateAt).Find(&states)
	if err := handleSingleOpErr(ret, -1, "QueryChangefeedStatesByUpdateAt"); err != nil {
		return nil, errors.Trace(err)
	}
	return states, nil
}

// queryChangefeedStateByUUID implements the changefeedStateClient interface.
//
//nolint:unused
func (c *ormClient) queryChangefeedStateByUUID(tx *gorm.DB, uuid uint64) (*ChangefeedStateDO, error) {
	state := &ChangefeedStateDO{}
	ret := tx.Where("changefeed_uuid = ?", uuid).First(state)
	if err := handleSingleOpErr(ret, 1, "QueryChangefeedStateByUUID"); err != nil {
		return nil, errors.Trace(err)
	}
	return state, nil
}

// queryChangefeedStateByUUIDs implements the changefeedStateClient interface.
// nolint:unused
func (c *ormClient) queryChangefeedStateByUUIDs(tx *gorm.DB, uuids ...uint64) ([]*ChangefeedStateDO, error) {
	states := []*ChangefeedStateDO{}
	ret := tx.Where("changefeed_uuid in (?)", uuids).Find(&states)
	if err := handleSingleOpErr(ret, int64(len(uuids)), "QueryChangefeedInfosByUUIDs"); err != nil {
		// TODO: optimize the behavior when some uuids are not found.
		return states, errors.Trace(err)
	}
	return states, nil
}

// queryChangefeedStateByUUIDWithLock implements the changefeedStateClient interface.
// nolint:unused
func (c *ormClient) queryChangefeedStateByUUIDWithLock(tx *gorm.DB, uuid uint64) (*ChangefeedStateDO, error) {
	state := &ChangefeedStateDO{}
	ret := tx.Where("changefeed_uuid = ?", uuid).
		Clauses(clause.Locking{
			Strength: "SHARE",
			Table:    clause.Table{Name: clause.CurrentTable},
		}).First(state)
	if err := handleSingleOpErr(ret, 1, "QueryChangefeedStateByUUIDWithLock"); err != nil {
		return nil, errors.Trace(err)
	}
	return state, nil
}

// ================================ Schedule Client =================================

// createSchedule implements the scheduleClient interface.
func (c *ormClient) createSchedule(tx *gorm.DB, sc *ScheduleDO) error {
	ret := tx.Create(sc)
	if err := handleSingleOpErr(ret, 1, "CreateSchedule"); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// deleteSchedule implements the scheduleClient interface.
func (c *ormClient) deleteSchedule(tx *gorm.DB, sc *ScheduleDO) error {
	ret := tx.Delete(sc)
	if err := handleSingleOpErr(ret, 1, "DeleteSchedule"); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// updateSchedule implements the scheduleClient interface.
func (c *ormClient) updateSchedule(tx *gorm.DB, sc *ScheduleDO) error {
	ret := tx.Where("changefeed_uuid = ? and version = ?", sc.ChangefeedUUID, sc.Version).
		Updates(ScheduleDO{
			ScheduledChangefeed: metadata.ScheduledChangefeed{
				Owner:        sc.Owner,
				OwnerState:   sc.OwnerState,
				Processors:   sc.Processors,
				TaskPosition: sc.TaskPosition,
			},
			Version: sc.Version + 1,
		})
	if err := handleSingleOpErr(ret, 1, "UpdateSchedule"); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// updateScheduleOwnerState implements the scheduleClient interface.
// TODO(CharlesCheung): check if the version needs to be checked.
func (c *ormClient) updateScheduleOwnerState(tx *gorm.DB, sc *ScheduleDO) error {
	ret := tx.Where("changefeed_uuid = ? and version = ?", sc.ChangefeedUUID, sc.Version).
		Updates(ScheduleDO{
			ScheduledChangefeed: metadata.ScheduledChangefeed{
				OwnerState: sc.OwnerState,
			},
			Version: sc.Version + 1,
		})
	if err := handleSingleOpErr(ret, 1, "UpdateScheduleOwnerState"); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// updateScheduleOwnerStateByOwnerID implements the scheduleClient interface.
func (c *ormClient) updateScheduleOwnerStateByOwnerID(tx *gorm.DB, state metadata.SchedState, ownerID model.CaptureID) error {
	ret := tx.Model(&ScheduleDO{}).Select("owner", "owner_state", "processors", "version").
		Where("owner = ?", ownerID).
		Updates(map[string]interface{}{
			"owner":       nil,
			"owner_state": state,
			"processors":  nil,
			"version":     gorm.Expr("version + ?", 1),
		})
	if err := handleSingleOpErr(ret, -1, "UpdateScheduleOwnerStateByOwnerID"); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// querySchedules implements the scheduleClient interface.
//
//nolint:unused
func (c *ormClient) querySchedules(tx *gorm.DB) ([]*ScheduleDO, error) {
	schedules := []*ScheduleDO{}
	ret := tx.Find(&schedules)
	if err := handleSingleOpErr(ret, -1, "QuerySchedules"); err != nil {
		return nil, errors.Trace(err)
	}
	return schedules, nil
}

// querySchedulesByUpdateAt implements the scheduleClient interface.
//
//nolint:unused
func (c *ormClient) querySchedulesByUpdateAt(tx *gorm.DB, lastUpdateAt time.Time) ([]*ScheduleDO, error) {
	schedules := []*ScheduleDO{}
	ret := tx.Where("update_at > ?", lastUpdateAt).Find(&schedules)
	if err := handleSingleOpErr(ret, -1, "QuerySchedulesByUpdateAt"); err != nil {
		return nil, errors.Trace(err)
	}
	return schedules, nil
}

// querySchedulesByOwnerIDAndUpdateAt implements the scheduleClient interface.
//
//nolint:unused
func (c *ormClient) querySchedulesByOwnerIDAndUpdateAt(tx *gorm.DB, captureID string, lastUpdateAt time.Time) ([]*ScheduleDO, error) {
	schedules := []*ScheduleDO{}
	ret := tx.Where("owner = ? and update_at > ?", captureID, lastUpdateAt).Find(&schedules)
	if err := handleSingleOpErr(ret, -1, "QuerySchedulesByOwnerIDAndUpdateAt"); err != nil {
		return nil, errors.Trace(err)
	}
	return schedules, nil
}

// queryScheduleByUUID implements the scheduleClient interface.
//
//nolint:unused
func (c *ormClient) queryScheduleByUUID(tx *gorm.DB, uuid uint64) (*ScheduleDO, error) {
	schedule := &ScheduleDO{}
	ret := tx.Where("changefeed_uuid = ?", uuid).First(schedule)
	if err := handleSingleOpErr(ret, 1, "QueryScheduleByUUID"); err != nil {
		return nil, errors.Trace(err)
	}
	return schedule, nil
}

// querySchedulesUinqueOwnerIDs implements the scheduleClient interface.
// nolint:unused
func (c *ormClient) querySchedulesUinqueOwnerIDs(tx *gorm.DB) ([]model.CaptureID, error) {
	captureIDs := []model.CaptureID{}
	ret := tx.Model(&ScheduleDO{}).Select("owner").Where("owner IS NOT NULL").Distinct().Find(&captureIDs)
	if err := handleSingleOpErr(ret, -1, "QuerySchedulesUinqueOwnerIDs"); err != nil {
		return nil, errors.Trace(err)
	}
	return captureIDs, nil
}

// ================================ Progress Client =================================

// createProgress implements the progressClient interface.
func (c *ormClient) createProgress(tx *gorm.DB, pr *ProgressDO) error {
	ret := tx.Create(pr)
	if err := handleSingleOpErr(ret, 1, "CreateProgress"); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// deleteProgress implements the progressClient interface.
func (c *ormClient) deleteProgress(tx *gorm.DB, pr *ProgressDO) error {
	ret := tx.Delete(pr)
	if err := handleSingleOpErr(ret, 1, "DeleteProgress"); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// updateProgress implements the progressClient interface.
func (c *ormClient) updateProgress(tx *gorm.DB, pr *ProgressDO) error {
	ret := tx.Where("capture_id = ? and version = ?", pr.CaptureID, pr.Version).
		Updates(ProgressDO{
			Progress: pr.Progress,
			Version:  pr.Version + 1,
		})
	if err := handleSingleOpErr(ret, 1, "UpdateProgress"); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// queryProgresss implements the progressClient interface.
//
//nolint:unused
func (c *ormClient) queryProgresss(tx *gorm.DB) ([]*ProgressDO, error) {
	progresses := []*ProgressDO{}
	ret := tx.Find(&progresses)
	if err := handleSingleOpErr(ret, -1, "QueryProgresss"); err != nil {
		return nil, errors.Trace(err)
	}
	return progresses, nil
}

// queryProgresssByUpdateAt implements the progressClient interface.
//
//nolint:unused
func (c *ormClient) queryProgresssByUpdateAt(tx *gorm.DB, lastUpdateAt time.Time) ([]*ProgressDO, error) {
	progresses := []*ProgressDO{}
	ret := tx.Where("update_at > ?", lastUpdateAt).Find(&progresses)
	if err := handleSingleOpErr(ret, -1, "QueryProgresssByUpdateAt"); err != nil {
		return nil, errors.Trace(err)
	}
	return progresses, nil
}

// queryProgressByCaptureID implements the progressClient interface.
//
//nolint:unused
func (c *ormClient) queryProgressByCaptureID(tx *gorm.DB, id string) (*ProgressDO, error) {
	progress := &ProgressDO{}
	ret := tx.Where("capture_id = ?", id).First(progress)
	if err := handleSingleOpErr(ret, 1, "QueryProgressByCaptureID"); err != nil {
		return nil, errors.Trace(err)
	}
	return progress, nil
}

// queryProgressByCaptureIDsWithLock implements the progressClient interface.
//
//nolint:unused
func (c *ormClient) queryProgressByCaptureIDsWithLock(tx *gorm.DB, ids []string) ([]*ProgressDO, error) {
	progresses := []*ProgressDO{}
	ret := tx.Where("capture_id in (?)", ids).Clauses(clause.Locking{
		Strength: "SHARE",
		Table:    clause.Table{Name: clause.CurrentTable},
	}).Find(&progresses)
	if err := handleSingleOpErr(ret, -1, "QueryProgressByCaptureIDsWithLock"); err != nil {
		return nil, errors.Trace(err)
	}
	return progresses, nil
}

func handleSingleOpErr(ret *gorm.DB, targetRows int64, opName string) (err error) {
	defer func() {
		if err != nil {
			log.Error("run unary meta operation failed", zap.Error(err))
		}
	}()

	if ret.Error != nil {
		return errors.WrapError(errors.ErrMetaOpFailed, ret.Error, opName)
	}
	if targetRows >= 0 && ret.RowsAffected != targetRows {
		return errors.ErrMetaRowsAffectedNotMatch.GenWithStackByArgs(opName, targetRows, ret.RowsAffected)
	}
	return nil
}
