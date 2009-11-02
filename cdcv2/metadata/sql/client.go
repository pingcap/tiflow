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
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

var _ upstreamClient = &meatStorageClient{}
var _ changefeedInfoClient = &meatStorageClient{}
var _ changefeedStateClient = &meatStorageClient{}
var _ scheduleClient = &meatStorageClient{}
var _ progressClient = &meatStorageClient{}

type upstreamClient interface {
	createUpstream(tx *gorm.DB, up *UpstreamDO) error
	deleteUpstream(tx *gorm.DB, up *UpstreamDO) error
	updateUpstream(tx *gorm.DB, up *UpstreamDO) error
	queryUpstreams(tx *gorm.DB) ([]*UpstreamDO, error)
	queryUpstreamsByUpdateAt(tx *gorm.DB, lastUpdateAt time.Time) ([]*UpstreamDO, error)
	queryUpstreamByID(tx *gorm.DB, id uint64) (*UpstreamDO, error)
}

type changefeedInfoClient interface {
	createChangefeedInfo(tx *gorm.DB, info *ChangefeedInfoDO) error
	deleteChangefeedInfo(tx *gorm.DB, info *ChangefeedInfoDO) error
	softDeleteChangefeedInfo(tx *gorm.DB, info *ChangefeedInfoDO) error
	updateChangefeedInfo(tx *gorm.DB, info *ChangefeedInfoDO) error
	queryChangefeedInfos(tx *gorm.DB) ([]*ChangefeedInfoDO, error)
	queryChangefeedInfosByUpdateAt(tx *gorm.DB, lastUpdateAt time.Time) ([]*ChangefeedInfoDO, error)
	queryChangefeedInfoByUUID(tx *gorm.DB, uuid uint64) (*ChangefeedInfoDO, error)
}

type changefeedStateClient interface {
	createChangefeedState(tx *gorm.DB, state *ChangefeedStateDO) error
	deleteChangefeedState(tx *gorm.DB, state *ChangefeedStateDO) error
	updateChangeFeedState(tx *gorm.DB, state *ChangefeedStateDO) error
	queryChangefeedStates(tx *gorm.DB) ([]*ChangefeedStateDO, error)
	queryChangefeedStatesByUpdateAt(tx *gorm.DB, lastUpdateAt time.Time) ([]*ChangefeedStateDO, error)
	queryChangefeedStateByUUID(tx *gorm.DB, uuid uint64) (*ChangefeedStateDO, error)
}

type scheduleClient interface {
	createSchedule(tx *gorm.DB, sc *ScheduleDO) error
	deleteSchedule(tx *gorm.DB, sc *ScheduleDO) error
	updateSchedule(tx *gorm.DB, sc *ScheduleDO) error
	querySchedules(tx *gorm.DB) ([]*ScheduleDO, error)
	querySchedulesByUpdateAt(tx *gorm.DB, lastUpdateAt time.Time) ([]*ScheduleDO, error)
	querySchedulesByOwnerIDAndUpdateAt(tx *gorm.DB, captureID string, lastUpdateAt time.Time) ([]*ScheduleDO, error)
	queryScheduleByUUID(tx *gorm.DB, uuid uint64) (*ScheduleDO, error)
}

type progressClient interface {
	createProgress(tx *gorm.DB, pr *ProgressDO) error
	deleteProgress(tx *gorm.DB, pr *ProgressDO) error
	updateProgress(tx *gorm.DB, pr *ProgressDO) error
	queryProgresss(tx *gorm.DB) ([]*ProgressDO, error)
	queryProgresssByUpdateAt(tx *gorm.DB, lastUpdateAt time.Time) ([]*ProgressDO, error)
	queryProgressByCaptureID(tx *gorm.DB, id string) (*ProgressDO, error)
}

type meatStorageClient struct {
	db *gorm.DB
}

// ================================ Upstream Client =================================

// createUpstream implements the upstreamClient interface.
func (s *meatStorageClient) createUpstream(tx *gorm.DB, up *UpstreamDO) error {
	ret := tx.Create(up)
	if err := handleSingleOpErr(ret, 1, "CreateUpstream"); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// deleteUpstream implements the upstreamClient interface.
func (s *meatStorageClient) deleteUpstream(tx *gorm.DB, up *UpstreamDO) error {
	ret := tx.Delete(up)
	if err := handleSingleOpErr(ret, 1, "DeleteUpstream"); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// updateUpstream implements the upstreamClient interface.
func (s *meatStorageClient) updateUpstream(tx *gorm.DB, up *UpstreamDO) error {
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
func (s *meatStorageClient) queryUpstreams(tx *gorm.DB) ([]*UpstreamDO, error) {
	var ups []*UpstreamDO
	ret := tx.Find(&ups)
	if err := handleSingleOpErr(ret, -1, "QueryUpstreams"); err != nil {
		return nil, errors.Trace(err)
	}
	return ups, nil
}

// queryUpstreamsByUpdateAt implements the upstreamClient interface.
func (s *meatStorageClient) queryUpstreamsByUpdateAt(tx *gorm.DB, lastUpdateAt time.Time) ([]*UpstreamDO, error) {
	var ups []*UpstreamDO
	ret := tx.Where("updated_at > ?", lastUpdateAt).Find(&ups)
	if err := handleSingleOpErr(ret, -1, "QueryUpstreamsByUpdateAt"); err != nil {
		return nil, errors.Trace(err)
	}
	return ups, nil
}

// queryUpstreamByID implements the upstreamClient interface.
func (s *meatStorageClient) queryUpstreamByID(tx *gorm.DB, id uint64) (*UpstreamDO, error) {
	var up *UpstreamDO
	ret := tx.Where("id = ?", id).First(up)
	if err := handleSingleOpErr(ret, 1, "QueryUpstreamsByUpdateAt"); err != nil {
		return nil, errors.Trace(err)
	}
	return up, nil
}

// ================================ ChangefeedInfo Client =================================

// createChangefeedInfo implements the changefeedInfoClient interface.
func (s *meatStorageClient) createChangefeedInfo(tx *gorm.DB, info *ChangefeedInfoDO) error {
	ret := tx.Create(info)
	if err := handleSingleOpErr(ret, 1, "CreateChangefeedInfo"); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// deleteChangefeedInfo implements the changefeedInfoClient interface.
func (s *meatStorageClient) deleteChangefeedInfo(tx *gorm.DB, info *ChangefeedInfoDO) error {
	ret := tx.Delete(info)
	if err := handleSingleOpErr(ret, 1, "DeleteChangefeedInfo"); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// softDeleteChangefeedInfo implements the changefeedInfoClient interface.
func (s *meatStorageClient) softDeleteChangefeedInfo(tx *gorm.DB, info *ChangefeedInfoDO) error {
	removeTime := time.Now()
	ret := tx.Where("uuid = ? and version = ?", info.UUID, info.Version).
		Updates(ChangefeedInfoDO{
			RemovedAt: &removeTime,
			Version:   info.Version + 1,
		})
	if err := handleSingleOpErr(ret, 1, "SoftDeleteChangefeedInfo"); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// updateChangefeedInfo implements the changefeedInfoClient interface.
func (s *meatStorageClient) updateChangefeedInfo(tx *gorm.DB, info *ChangefeedInfoDO) error {
	ret := tx.Where("uuid = ? and version = ?", info.UUID, info.Version).
		Updates(ChangefeedInfoDO{
			SinkURI:  info.SinkURI,
			StartTs:  info.StartTs,
			TargetTs: info.TargetTs,
			Config:   info.Config,
			Version:  info.Version + 1,
		})
	if err := handleSingleOpErr(ret, 1, "UpdateChangefeedInfo"); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// queryChangefeedInfos implements the changefeedInfoClient interface.
func (s *meatStorageClient) queryChangefeedInfos(tx *gorm.DB) ([]*ChangefeedInfoDO, error) {
	var infos []*ChangefeedInfoDO
	ret := tx.Find(&infos)
	if err := handleSingleOpErr(ret, -1, "QueryChangefeedInfos"); err != nil {
		return nil, errors.Trace(err)
	}
	return infos, nil
}

// queryChangefeedInfosByUpdateAt implements the changefeedInfoClient interface.
// TODO(CharlesCheung): query data before lastUpdateAt to avoid data loss.
func (s *meatStorageClient) queryChangefeedInfosByUpdateAt(tx *gorm.DB, lastUpdateAt time.Time) ([]*ChangefeedInfoDO, error) {
	var infos []*ChangefeedInfoDO
	ret := tx.Where("updated_at > ?", lastUpdateAt).Find(&infos)
	if err := handleSingleOpErr(ret, -1, "QueryChangefeedInfosByUpdateAt"); err != nil {
		return nil, errors.Trace(err)
	}
	return infos, nil
}

// queryChangefeedInfoByUUID implements the changefeedInfoClient interface.
func (s *meatStorageClient) queryChangefeedInfoByUUID(tx *gorm.DB, uuid uint64) (*ChangefeedInfoDO, error) {
	var info *ChangefeedInfoDO
	ret := tx.Where("uuid = ?", uuid).First(info)

	// TODO(CharlesCheung): handle record not found error.
	if err := handleSingleOpErr(ret, 1, "QueryChangefeedInfoByUUID"); err != nil {
		return nil, errors.Trace(err)
	}
	return info, nil
}

// ================================ ChangefeedState Client =================================

// createChangefeedState implements the changefeedStateClient interface.
func (s *meatStorageClient) createChangefeedState(tx *gorm.DB, state *ChangefeedStateDO) error {
	ret := tx.Create(state)
	if err := handleSingleOpErr(ret, 1, "CreateChangefeedState"); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// deleteChangefeedState implements the changefeedStateClient interface.
func (s *meatStorageClient) deleteChangefeedState(tx *gorm.DB, state *ChangefeedStateDO) error {
	ret := tx.Delete(state)
	if err := handleSingleOpErr(ret, 1, "DeleteChangefeedState"); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// updateChangeFeedState implements the changefeedStateClient interface.
func (s *meatStorageClient) updateChangeFeedState(tx *gorm.DB, state *ChangefeedStateDO) error {
	ret := tx.Where("changefeed_uuid = ? and version = ?", state.ChangefeedUUID, state.Version).
		Updates(ChangefeedStateDO{
			State:   state.State,
			Warning: state.Warning,
			Error:   state.Error,
			Version: state.Version + 1,
		})
	if err := handleSingleOpErr(ret, 1, "UpdateChangeFeedState"); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// queryChangefeedStates implements the changefeedStateClient interface.
func (s *meatStorageClient) queryChangefeedStates(tx *gorm.DB) ([]*ChangefeedStateDO, error) {
	var states []*ChangefeedStateDO
	ret := tx.Find(&states)
	if err := handleSingleOpErr(ret, -1, "QueryChangefeedStates"); err != nil {
		return nil, errors.Trace(err)
	}
	return states, nil
}

// queryChangefeedStatesByUpdateAt implements the changefeedStateClient interface.
func (s *meatStorageClient) queryChangefeedStatesByUpdateAt(tx *gorm.DB, lastUpdateAt time.Time) ([]*ChangefeedStateDO, error) {
	var states []*ChangefeedStateDO
	ret := tx.Where("updated_at > ?", lastUpdateAt).Find(&states)
	if err := handleSingleOpErr(ret, -1, "QueryChangefeedStatesByUpdateAt"); err != nil {
		return nil, errors.Trace(err)
	}
	return states, nil
}

// queryChangefeedStateByUUID implements the changefeedStateClient interface.
func (s *meatStorageClient) queryChangefeedStateByUUID(tx *gorm.DB, uuid uint64) (*ChangefeedStateDO, error) {
	var state *ChangefeedStateDO
	ret := tx.Where("changefeed_uuid = ?", uuid).First(state)
	if err := handleSingleOpErr(ret, 1, "QueryChangefeedStateByUUID"); err != nil {
		return nil, errors.Trace(err)
	}
	return state, nil
}

// ================================ Schedule Client =================================

// createSchedule implements the scheduleClient interface.
func (s *meatStorageClient) createSchedule(tx *gorm.DB, sc *ScheduleDO) error {
	ret := tx.Create(sc)
	if err := handleSingleOpErr(ret, 1, "CreateSchedule"); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// deleteSchedule implements the scheduleClient interface.
func (s *meatStorageClient) deleteSchedule(tx *gorm.DB, sc *ScheduleDO) error {
	ret := tx.Delete(sc)
	if err := handleSingleOpErr(ret, 1, "DeleteSchedule"); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// updateSchedule implements the scheduleClient interface.
func (s *meatStorageClient) updateSchedule(tx *gorm.DB, sc *ScheduleDO) error {
	ret := tx.Where("changefeed_uuid = ? and version = ?", sc.ChangefeedUUID, sc.Version).
		Updates(ScheduleDO{
			Owner:        sc.Owner,
			OwnerState:   sc.OwnerState,
			Processors:   sc.Processors,
			TaskPosition: sc.TaskPosition,
			Version:      sc.Version + 1,
		})
	if err := handleSingleOpErr(ret, 1, "UpdateSchedule"); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// querySchedules implements the scheduleClient interface.
func (s *meatStorageClient) querySchedules(tx *gorm.DB) ([]*ScheduleDO, error) {
	var schedules []*ScheduleDO
	ret := tx.Find(&schedules)
	if err := handleSingleOpErr(ret, -1, "QuerySchedules"); err != nil {
		return nil, errors.Trace(err)
	}
	return schedules, nil
}

// querySchedulesByUpdateAt implements the scheduleClient interface.
func (s *meatStorageClient) querySchedulesByUpdateAt(tx *gorm.DB, lastUpdateAt time.Time) ([]*ScheduleDO, error) {
	var schedules []*ScheduleDO
	ret := tx.Where("updated_at > ?", lastUpdateAt).Find(&schedules)
	if err := handleSingleOpErr(ret, -1, "QuerySchedulesByUpdateAt"); err != nil {
		return nil, errors.Trace(err)
	}
	return schedules, nil
}

// querySchedulesByOwnerIDAndUpdateAt implements the scheduleClient interface.
func (s *meatStorageClient) querySchedulesByOwnerIDAndUpdateAt(tx *gorm.DB, captureID string, lastUpdateAt time.Time) ([]*ScheduleDO, error) {
	var schedules []*ScheduleDO
	ret := tx.Where("owner = ? and updated_at > ?", captureID, lastUpdateAt).Find(&schedules)
	if err := handleSingleOpErr(ret, -1, "QuerySchedulesByOwnerIDAndUpdateAt"); err != nil {
		return nil, errors.Trace(err)
	}
	return schedules, nil
}

// queryScheduleByUUID implements the scheduleClient interface.
func (s *meatStorageClient) queryScheduleByUUID(tx *gorm.DB, uuid uint64) (*ScheduleDO, error) {
	var schedule *ScheduleDO
	ret := tx.Where("changefeed_uuid = ?", uuid).First(schedule)
	if err := handleSingleOpErr(ret, 1, "QueryScheduleByUUID"); err != nil {
		return nil, errors.Trace(err)
	}
	return schedule, nil
}

// ================================ Progress Client =================================

// createProgress implements the progressClient interface.
func (s *meatStorageClient) createProgress(tx *gorm.DB, pr *ProgressDO) error {
	ret := tx.Create(pr)
	if err := handleSingleOpErr(ret, 1, "CreateProgress"); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// deleteProgress implements the progressClient interface.
func (s *meatStorageClient) deleteProgress(tx *gorm.DB, pr *ProgressDO) error {
	ret := tx.Delete(pr)
	if err := handleSingleOpErr(ret, 1, "DeleteProgress"); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// updateProgress implements the progressClient interface.
func (s *meatStorageClient) updateProgress(tx *gorm.DB, pr *ProgressDO) error {
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
func (s *meatStorageClient) queryProgresss(tx *gorm.DB) ([]*ProgressDO, error) {
	var progresses []*ProgressDO
	ret := tx.Find(&progresses)
	if err := handleSingleOpErr(ret, -1, "QueryProgresss"); err != nil {
		return nil, errors.Trace(err)
	}
	return progresses, nil
}

// queryProgresssByUpdateAt implements the progressClient interface.
func (s *meatStorageClient) queryProgresssByUpdateAt(tx *gorm.DB, lastUpdateAt time.Time) ([]*ProgressDO, error) {
	var progresses []*ProgressDO
	ret := tx.Where("updated_at > ?", lastUpdateAt).Find(&progresses)
	if err := handleSingleOpErr(ret, -1, "QueryProgresssByUpdateAt"); err != nil {
		return nil, errors.Trace(err)
	}
	return progresses, nil
}

// queryProgressByCaptureID implements the progressClient interface.
func (s *meatStorageClient) queryProgressByCaptureID(tx *gorm.DB, id string) (*ProgressDO, error) {
	var progress *ProgressDO
	ret := tx.Where("capture_id = ?", id).First(progress)
	if err := handleSingleOpErr(ret, 1, "QueryProgressByCaptureID"); err != nil {
		return nil, errors.Trace(err)
	}
	return progress, nil
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
		return errors.ErrMetaOpIgnored.GenWithStackByArgs(opName)
	}
	return nil
}
