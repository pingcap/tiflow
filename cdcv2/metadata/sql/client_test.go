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
	"database/sql/driver"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdcv2/metadata"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/stretchr/testify/require"
)

// ================================ Test Create/Update/Delete =================================

// Note that updateAt is not included in the test because it is automatically updated by gorm.
// TODO(CharlesCheung): add test to verify the correctness of updateAt.
func runMockExecTest(
	t *testing.T, mock sqlmock.Sqlmock,
	expectedSQL string, args []driver.Value,
	fn func() error,
	skipCheck ...bool, /* 0: test ErrMetaRowsAffectedNotMatch, 1: test ErrMetaOpFailed */
) {
	testErr := errors.New("test error")

	// Test normal execution
	mock.ExpectExec(expectedSQL).WithArgs(args...).WillReturnResult(sqlmock.NewResult(1, 1))
	err := fn()
	require.NoError(t, err)

	// Test rows affected not match
	mock.ExpectExec(expectedSQL).WithArgs(args...).WillReturnResult(sqlmock.NewResult(1, 0))
	err = fn()
	if len(skipCheck) < 1 || !skipCheck[0] {
		require.ErrorIs(t, err, errors.ErrMetaRowsAffectedNotMatch)
	}

	// Test op failed
	mock.ExpectExec(expectedSQL).WithArgs(args...).WillReturnError(testErr)
	err = fn()
	if len(skipCheck) < 2 || !skipCheck[1] {
		require.ErrorIs(t, err, errors.ErrMetaOpFailed)
		require.ErrorContains(t, err, testErr.Error())
	}
}

func TestUpstreamClientExecSQL(t *testing.T) {
	t.Parallel()

	backendDB, db, mock := newMockDB(t)
	defer backendDB.Close()
	cient := NewORMClient("test-upstream-client", db)

	up := &UpstreamDO{
		ID:        1,
		Endpoints: strings.Join([]string{"endpoint1", "endpoint2"}, ","),
		Config: &security.Credential{
			CAPath: "ca-path",
		},
		Version: 1,
	}
	config, err := up.Config.Value()
	require.NoError(t, err)

	// Test createUpstream
	runMockExecTest(
		t, mock,
		"INSERT INTO `upstream` (`endpoints`,`config`,`version`,`update_at`,`id`) VALUES (?,?,?,?,?)",
		[]driver.Value{up.Endpoints, up.Config, up.Version, sqlmock.AnyArg(), up.ID},
		func() error {
			return cient.createUpstream(db, up)
		},
	)

	// Test deleteUpstream
	runMockExecTest(
		t, mock,
		// TODO(CharlesCheung): delete statement should be optimized, such as remove duplicated fields.
		// Note: should the version be checked?
		"DELETE FROM `upstream` WHERE `upstream`.`id` = ?",
		[]driver.Value{up.ID},
		func() error {
			return cient.deleteUpstream(db, up)
		},
	)

	// Test updateUpstream
	runMockExecTest(
		t, mock,
		"UPDATE `upstream` SET `endpoints`=?,`config`=?,`version`=?,`update_at`=? WHERE id = ? and version = ?",
		[]driver.Value{up.Endpoints, config, up.Version + 1, sqlmock.AnyArg(), up.ID, up.Version},
		func() error {
			return cient.updateUpstream(db, up)
		},
	)

	// Test updateUpstream with nil config
	up.Config = nil
	runMockExecTest(
		t, mock,
		"UPDATE `upstream` SET `endpoints`=?,`version`=?,`update_at`=? WHERE id = ? and version = ?",
		[]driver.Value{up.Endpoints, up.Version + 1, sqlmock.AnyArg(), up.ID, up.Version},
		func() error {
			return cient.updateUpstream(db, up)
		},
	)
}

func TestChangefeedInfoClientExecSQL(t *testing.T) {
	t.Parallel()

	backendDB, db, mock := newMockDB(t)
	defer backendDB.Close()
	client := NewORMClient("test-changefeed-info-client", db)

	info := &ChangefeedInfoDO{
		ChangefeedInfo: metadata.ChangefeedInfo{
			ChangefeedIdent: metadata.ChangefeedIdent{
				UUID:      1,
				Namespace: "namespace",
				ID:        "id",
			},
			UpstreamID: 1,
			SinkURI:    "sinkURI",
			StartTs:    1,
			TargetTs:   1,
			Config:     &config.ReplicaConfig{},
		},
		RemovedAt: nil,
		Version:   1,
	}
	configValue, err := info.Config.Value()
	require.NoError(t, err)

	// Test createChangefeedInfo
	runMockExecTest(
		t, mock,
		"INSERT INTO `changefeed_info` ("+
			"`namespace`,`id`,`upstream_id`,`sink_uri`,"+
			"`start_ts`,`target_ts`,`config`,`removed_at`,"+
			"`version`,`update_at`,`uuid`) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
		[]driver.Value{
			info.Namespace, info.ID, info.UpstreamID, info.SinkURI,
			info.StartTs, info.TargetTs, configValue, info.RemovedAt,
			info.Version, sqlmock.AnyArg(), info.UUID,
		},
		func() error {
			return client.createChangefeedInfo(db, info)
		},
	)

	// Test deleteChangefeedInfo
	runMockExecTest(
		t, mock,
		"DELETE FROM `changefeed_info` WHERE `changefeed_info`.`uuid` = ?",
		[]driver.Value{info.UUID},
		func() error {
			return client.deleteChangefeedInfo(db, info)
		},
	)

	// Test updateChangefeedInfo
	runMockExecTest(
		t, mock,
		"UPDATE `changefeed_info` SET `sink_uri`=?,`start_ts`=?,`target_ts`=?,`config`=?,`version`=?,`update_at`=? WHERE uuid = ? and version = ?",
		[]driver.Value{info.SinkURI, info.StartTs, info.TargetTs, configValue, info.Version + 1, sqlmock.AnyArg(), info.UUID, info.Version},
		func() error {
			return client.updateChangefeedInfo(db, info)
		},
	)

	// Test updateChangefeedInfo with nil config
	info.Config = nil
	runMockExecTest(
		t, mock,
		"UPDATE `changefeed_info` SET `sink_uri`=?,`start_ts`=?,`target_ts`=?,`version`=?,`update_at`=? WHERE uuid = ? and version = ?",
		[]driver.Value{info.SinkURI, info.StartTs, info.TargetTs, info.Version + 1, sqlmock.AnyArg(), info.UUID, info.Version},
		func() error {
			return client.updateChangefeedInfo(db, info)
		},
	)

	// Test markChangefeedRemoved
	runMockExecTest(
		t, mock,
		"UPDATE `changefeed_info` SET `removed_at`=?,`version`=?,`update_at`=? WHERE uuid = ? and version = ?",
		[]driver.Value{sqlmock.AnyArg(), info.Version + 1, sqlmock.AnyArg(), info.UUID, info.Version},
		func() error {
			return client.markChangefeedRemoved(db, info)
		},
	)
}

func TestChangefeedStateClientExecSQL(t *testing.T) {
	t.Parallel()

	backendDB, db, mock := newMockDB(t)
	defer backendDB.Close()
	cient := NewORMClient("test-changefeed-state-client", db)

	state := &ChangefeedStateDO{
		ChangefeedState: metadata.ChangefeedState{
			ChangefeedUUID: 1,
			State:          "state",
			// Note that warning and error could be nil.
			Warning: nil,
			Error: &model.RunningError{
				Time: time.Now(),
				Addr: "addr",
				Code: "code",
			},
		},
		Version: 1,
	}

	errVal, err := state.Error.Value()
	require.NoError(t, err)

	// Test createChangefeedState
	runMockExecTest(
		t, mock,
		"INSERT INTO `changefeed_state` (`state`,`warning`,`error`,`version`,`update_at`,`changefeed_uuid`) VALUES (?,?,?,?,?,?)",
		[]driver.Value{state.State, state.Warning, errVal, state.Version, sqlmock.AnyArg(), state.ChangefeedUUID},
		func() error {
			return cient.createChangefeedState(db, state)
		},
	)

	// Test deleteChangefeedState
	runMockExecTest(
		t, mock,
		"DELETE FROM `changefeed_state` WHERE `changefeed_state`.`changefeed_uuid` = ?",
		[]driver.Value{state.ChangefeedUUID},
		func() error {
			return cient.deleteChangefeedState(db, state)
		},
	)

	// Test updateChangefeedState
	// Note that a nil error or warning will also be updated.
	runMockExecTest(
		t, mock,
		"UPDATE `changefeed_state` SET `state`=?,`warning`=?,`error`=?,`version`=?,`update_at`=? WHERE changefeed_uuid = ? and version = ?",
		[]driver.Value{state.State, state.Warning, errVal, state.Version + 1, sqlmock.AnyArg(), state.ChangefeedUUID, state.Version},
		func() error {
			return cient.updateChangefeedState(db, state)
		},
	)
}

func TestScheduleClientExecSQL(t *testing.T) {
	t.Parallel()

	backendDB, db, mock := newMockDB(t)
	defer backendDB.Close()
	cient := NewORMClient("test-schedule-client", db)

	ownerCapture := "test-owner"
	schedule := &ScheduleDO{
		ScheduledChangefeed: metadata.ScheduledChangefeed{
			ChangefeedUUID: 1,
			Owner:          &ownerCapture,
			OwnerState:     metadata.SchedRemoved,
			Processors:     nil,
			TaskPosition: metadata.ChangefeedProgress{
				CheckpointTs: 1,
			},
		},
		Version: 1,
	}

	// Test createSchedule
	runMockExecTest(
		t, mock,
		"INSERT INTO `schedule` (`owner`,`owner_state`,`processors`,`task_position`,`version`,`update_at`,`changefeed_uuid`) VALUES (?,?,?,?,?,?,?)",
		[]driver.Value{schedule.Owner, schedule.OwnerState, schedule.Processors, schedule.TaskPosition, schedule.Version, sqlmock.AnyArg(), schedule.ChangefeedUUID},
		func() error {
			return cient.createSchedule(db, schedule)
		},
	)

	// Test deleteSchedule
	runMockExecTest(
		t, mock,
		"DELETE FROM `schedule` WHERE `schedule`.`changefeed_uuid` = ?",
		[]driver.Value{schedule.ChangefeedUUID},
		func() error {
			return cient.deleteSchedule(db, schedule)
		},
	)

	// Test updateSchedule with non-empty task position.
	runMockExecTest(
		t, mock,
		"UPDATE `schedule` SET `owner`=?,`owner_state`=?,`task_position`=?,`version`=?,`update_at`=? WHERE changefeed_uuid = ? and version = ?",
		[]driver.Value{schedule.Owner, schedule.OwnerState, schedule.TaskPosition, schedule.Version + 1, sqlmock.AnyArg(), schedule.ChangefeedUUID, schedule.Version},
		func() error {
			return cient.updateSchedule(db, schedule)
		},
	)

	// Test updateSchedule with empty task position.
	schedule.TaskPosition = metadata.ChangefeedProgress{}
	runMockExecTest(
		t, mock,
		"UPDATE `schedule` SET `owner`=?,`owner_state`=?,`version`=?,`update_at`=? WHERE changefeed_uuid = ? and version = ?",
		[]driver.Value{schedule.Owner, schedule.OwnerState, schedule.Version + 1, sqlmock.AnyArg(), schedule.ChangefeedUUID, schedule.Version},
		func() error {
			return cient.updateSchedule(db, schedule)
		},
	)

	// Test updateScheduleOwnerState
	runMockExecTest(
		t, mock,
		"UPDATE `schedule` SET `owner_state`=?,`version`=?,`update_at`=? WHERE changefeed_uuid = ? and version = ?",
		[]driver.Value{schedule.OwnerState, schedule.Version + 1, sqlmock.AnyArg(), schedule.ChangefeedUUID, schedule.Version},
		func() error {
			return cient.updateScheduleOwnerState(db, schedule)
		},
	)

	// Test updateScheduleOwnerStateByOwnerID
	runMockExecTest(
		t, mock,
		"UPDATE `schedule` SET `owner`=?,`owner_state`=?,`processors`=?,`version`=version + ?,`update_at`=? WHERE owner = ?",
		[]driver.Value{nil, metadata.SchedRemoved, nil, 1, sqlmock.AnyArg(), *schedule.Owner},
		func() error {
			return cient.updateScheduleOwnerStateByOwnerID(db, metadata.SchedRemoved, *schedule.Owner)
		},
		true, // skip check ErrMetaRowsAffectedNotMatch since multiple rows would be affected.
	)
}

func TestProgressClientExecSQL(t *testing.T) {
	t.Parallel()

	backendDB, db, mock := newMockDB(t)
	defer backendDB.Close()
	cient := NewORMClient("test-progress-client", db)

	progress := &ProgressDO{
		CaptureID: "test-captureID",
		Progress:  nil,
		Version:   1,
	}

	// Test createProgress
	runMockExecTest(
		t, mock,
		"INSERT INTO `progress` (`capture_id`,`progress`,`version`,`update_at`) VALUES (?,?,?,?)",
		[]driver.Value{progress.CaptureID, progress.Progress, progress.Version, sqlmock.AnyArg()},
		func() error {
			return cient.createProgress(db, progress)
		},
	)

	// Test deleteProgress
	runMockExecTest(
		t, mock,
		"DELETE FROM `progress` WHERE `progress`.`capture_id` = ?",
		[]driver.Value{progress.CaptureID},
		func() error {
			return cient.deleteProgress(db, progress)
		},
	)

	// Test updateProgress
	progress.Progress = &metadata.CaptureProgress{}
	runMockExecTest(
		t, mock,
		"UPDATE `progress` SET `progress`=?,`version`=?,`update_at`=? WHERE capture_id = ? and version = ?",
		[]driver.Value{progress.Progress, progress.Version + 1, sqlmock.AnyArg(), progress.CaptureID, progress.Version},
		func() error {
			return cient.updateProgress(db, progress)
		},
	)
}

// ================================ Test Query =================================

type queryType int32

const (
	queryTypePoint queryType = iota
	queryTypeRange
	queryTypeFullTable
)

func runMockQueryTest(
	_ *testing.T, mock sqlmock.Sqlmock,
	expectedSQL string, args []driver.Value,
	columns []string, rows []interface{},
	getValue func(interface{}) []driver.Value,
	runQuery func(expectedRowsCnt int, expectedError error),
	queryTpye queryType,
) {
	// Test normal execution
	returnRows := sqlmock.NewRows(columns)
	for _, row := range rows {
		returnRows.AddRow(getValue(row)...)
	}
	mock.ExpectQuery(expectedSQL).WithArgs(args...).WillReturnRows(returnRows)
	runQuery(len(rows), nil)

	// Test return empty rows
	mock.ExpectQuery(expectedSQL).WithArgs(args...).WillReturnRows(sqlmock.NewRows(columns))
	if queryTpye == queryTypePoint {
		runQuery(0, errors.ErrMetaRowsAffectedNotMatch)
	} else {
		runQuery(0, nil)
	}

	// Test return error
	testErr := errors.New("test error")
	mock.ExpectQuery(expectedSQL).WithArgs(args...).WillReturnError(testErr)
	runQuery(0, testErr)
}

func TestUpstreamClientQuerySQL(t *testing.T) {
	t.Parallel()

	backendDB, db, mock := newMockDB(t)
	defer backendDB.Close()
	client := NewORMClient("test-upstream-client-query", db)

	rows := []*UpstreamDO{
		{
			ID:        1,
			Endpoints: strings.Join([]string{"endpoint1", "endpoint2"}, ","),
			Config:    nil, /* test nil */
			Version:   1,
			UpdateAt:  time.Now(),
		},
		{
			ID:        2,
			Endpoints: strings.Join([]string{"endpoint3", "endpoint4"}, ","),
			Config:    &security.Credential{}, /* test empty */
			Version:   2,
			UpdateAt:  time.Now(),
		},
	}

	// Test queryUpstreams
	expectedQueryUpstreams := rows
	queryUpstreamsRows := []interface{}{expectedQueryUpstreams[0], expectedQueryUpstreams[1]}
	runMockQueryTest(t, mock,
		"SELECT * FROM `upstream`", nil,
		[]string{"id", "endpoints", "config", "version", "update_at"},
		queryUpstreamsRows,
		func(r interface{}) []driver.Value {
			row, ok := r.(*UpstreamDO)
			require.True(t, ok)
			return []driver.Value{row.ID, row.Endpoints, row.Config, row.Version, row.UpdateAt}
		},
		func(expectedRowsCnt int, expectedError error) {
			upstreams, err := client.queryUpstreams(db)
			require.ErrorIs(t, err, expectedError)
			require.Len(t, upstreams, expectedRowsCnt)
			if expectedRowsCnt != 0 {
				require.Equal(t, expectedQueryUpstreams, upstreams)
			}
		},
		queryTypeFullTable,
	)

	// Test queryUpstreamsByUpdateAt
	expectedQueryUpstreamsByUpdateAt := rows
	queryUpstreamsByUpdateAtRows := []interface{}{expectedQueryUpstreamsByUpdateAt[0], expectedQueryUpstreamsByUpdateAt[1]}
	queryAt := time.Now()
	runMockQueryTest(t, mock,
		"SELECT * FROM `upstream` WHERE update_at > ?", []driver.Value{queryAt},
		[]string{"id", "endpoints", "config", "version", "update_at"},
		queryUpstreamsByUpdateAtRows,
		func(r interface{}) []driver.Value {
			row, ok := r.(*UpstreamDO)
			require.True(t, ok)
			return []driver.Value{row.ID, row.Endpoints, row.Config, row.Version, row.UpdateAt}
		},
		func(expectedRowsCnt int, expectedError error) {
			upstreams, err := client.queryUpstreamsByUpdateAt(db, queryAt)
			require.ErrorIs(t, err, expectedError)
			require.Len(t, upstreams, expectedRowsCnt)
			if expectedRowsCnt != 0 {
				require.Equal(t, expectedQueryUpstreamsByUpdateAt, upstreams)
			}
		},
		queryTypeRange,
	)

	// Test queryUpstreamByID
	for _, row := range rows {
		expectedQueryUpstreamByID := row
		queryUpstreamByIDRows := []interface{}{row}
		runMockQueryTest(t, mock,
			"SELECT * FROM `upstream` WHERE id = ? LIMIT 1",
			[]driver.Value{expectedQueryUpstreamByID.ID},
			[]string{"id", "endpoints", "config", "version", "update_at"},
			queryUpstreamByIDRows,
			func(r interface{}) []driver.Value {
				row, ok := r.(*UpstreamDO)
				require.True(t, ok)
				return []driver.Value{row.ID, row.Endpoints, row.Config, row.Version, row.UpdateAt}
			},
			func(expectedRowsCnt int, expectedError error) {
				upstream, err := client.queryUpstreamByID(db, expectedQueryUpstreamByID.ID)
				require.ErrorIs(t, err, expectedError)
				if expectedRowsCnt != 0 {
					require.Equal(t, expectedQueryUpstreamByID, upstream)
				} else {
					require.Nil(t, upstream)
				}
			},
			queryTypePoint,
		)
	}
}

func TestChangefeedInfoClientQuerySQL(t *testing.T) {
	t.Parallel()

	backendDB, db, mock := newMockDB(t)
	defer backendDB.Close()
	client := NewORMClient("test-changefeed-info-client-query", db)

	rows := []*ChangefeedInfoDO{
		{
			ChangefeedInfo: metadata.ChangefeedInfo{
				ChangefeedIdent: metadata.ChangefeedIdent{
					UUID:      1,
					Namespace: "namespace",
					ID:        "id",
				},
				UpstreamID: 1,
				SinkURI:    "sinkURI",
				StartTs:    1,
				TargetTs:   1,
				Config:     nil, /* test nil */
			},
			RemovedAt: nil, /* test nil */
			Version:   1,
			UpdateAt:  time.Now(),
		},
		{
			ChangefeedInfo: metadata.ChangefeedInfo{
				ChangefeedIdent: metadata.ChangefeedIdent{
					UUID:      2,
					Namespace: "namespace",
					ID:        "id",
				},
				UpstreamID: 2,
				SinkURI:    "sinkURI",
				StartTs:    2,
				TargetTs:   2,
				Config:     &config.ReplicaConfig{}, /* test empty */
			},
			RemovedAt: &time.Time{}, /* test empty */
			Version:   2,
			UpdateAt:  time.Now(),
		},
	}

	// Test queryChangefeedInfos
	expectedQueryChangefeedInfos := rows
	queryChangefeedInfosRows := []interface{}{expectedQueryChangefeedInfos[0], expectedQueryChangefeedInfos[1]}
	runMockQueryTest(t, mock,
		"SELECT * FROM `changefeed_info`", nil,
		[]string{
			"uuid", "namespace", "id", "upstream_id", "sink_uri",
			"start_ts", "target_ts", "config", "removed_at",
			"version", "update_at",
		},
		queryChangefeedInfosRows,
		func(r interface{}) []driver.Value {
			row, ok := r.(*ChangefeedInfoDO)
			require.True(t, ok)
			return []driver.Value{
				row.UUID, row.Namespace, row.ID, row.UpstreamID, row.SinkURI,
				row.StartTs, row.TargetTs, row.Config, row.RemovedAt,
				row.Version, row.UpdateAt,
			}
		},
		func(expectedRowsCnt int, expectedError error) {
			changefeedInfos, err := client.queryChangefeedInfos(db)
			require.ErrorIs(t, err, expectedError)
			require.Len(t, changefeedInfos, expectedRowsCnt)
			if expectedRowsCnt != 0 {
				require.Equal(t, expectedQueryChangefeedInfos, changefeedInfos)
			}
		},
		queryTypeFullTable,
	)

	// Test queryChangefeedInfosByUpdateAt
	expectedQueryChangefeedInfosByUpdateAt := rows
	queryChangefeedInfosByUpdateAtRows := []interface{}{
		expectedQueryChangefeedInfosByUpdateAt[0],
		expectedQueryChangefeedInfosByUpdateAt[1],
	}
	queryAt := time.Now()
	runMockQueryTest(t, mock,
		"SELECT * FROM `changefeed_info` WHERE update_at > ?", []driver.Value{queryAt},
		[]string{
			"uuid", "namespace", "id", "upstream_id", "sink_uri",
			"start_ts", "target_ts", "config", "removed_at",
			"version", "update_at",
		},
		queryChangefeedInfosByUpdateAtRows,
		func(r interface{}) []driver.Value {
			row, ok := r.(*ChangefeedInfoDO)
			require.True(t, ok)
			return []driver.Value{
				row.UUID, row.Namespace, row.ID, row.UpstreamID, row.SinkURI,
				row.StartTs, row.TargetTs, row.Config, row.RemovedAt,
				row.Version, row.UpdateAt,
			}
		},
		func(expectedRowsCnt int, expectedError error) {
			changefeedInfos, err := client.queryChangefeedInfosByUpdateAt(db, queryAt)
			require.ErrorIs(t, err, expectedError)
			require.Len(t, changefeedInfos, expectedRowsCnt)
			if expectedRowsCnt != 0 {
				require.Equal(t, expectedQueryChangefeedInfosByUpdateAt, changefeedInfos)
			}
		},
		queryTypeRange,
	)

	// Test queryChangefeedInfosByUUIDs
	expectedQueryChangefeedInfosByUUIDs := rows
	queryChangefeedInfosByUUIDsRows := []interface{}{
		expectedQueryChangefeedInfosByUUIDs[0],
		expectedQueryChangefeedInfosByUUIDs[1],
	}
	runMockQueryTest(t, mock,
		"SELECT * FROM `changefeed_info` WHERE uuid IN (?,?)", []driver.Value{1, 2},
		[]string{
			"uuid", "namespace", "id", "upstream_id", "sink_uri",
			"start_ts", "target_ts", "config", "removed_at",
			"version", "update_at",
		},
		queryChangefeedInfosByUUIDsRows,
		func(r interface{}) []driver.Value {
			row, ok := r.(*ChangefeedInfoDO)
			require.True(t, ok)
			return []driver.Value{
				row.UUID, row.Namespace, row.ID, row.UpstreamID, row.SinkURI,
				row.StartTs, row.TargetTs, row.Config, row.RemovedAt,
				row.Version, row.UpdateAt,
			}
		},
		func(expectedRowsCnt int, expectedError error) {
			changefeedInfos, err := client.queryChangefeedInfosByUUIDs(db, 1, 2)
			require.ErrorIs(t, err, expectedError)
			require.Len(t, changefeedInfos, expectedRowsCnt)
			if expectedRowsCnt != 0 {
				require.Equal(t, expectedQueryChangefeedInfosByUUIDs, changefeedInfos)
			}
		},
		queryTypePoint,
	)

	// Test queryChangefeedInfoByUUID
	for _, row := range rows {
		expectedQueryChangefeedInfoByUUID := row
		queryChangefeedInfoByUUIDRows := []interface{}{row}
		runMockQueryTest(t, mock,
			"SELECT * FROM `changefeed_info` WHERE uuid = ? LIMIT 1",
			[]driver.Value{expectedQueryChangefeedInfoByUUID.UUID},
			[]string{
				"uuid", "namespace", "id", "upstream_id", "sink_uri",
				"start_ts", "target_ts", "config", "removed_at",
				"version", "update_at",
			},
			queryChangefeedInfoByUUIDRows,
			func(r interface{}) []driver.Value {
				row, ok := r.(*ChangefeedInfoDO)
				require.True(t, ok)
				return []driver.Value{
					row.UUID, row.Namespace, row.ID, row.UpstreamID, row.SinkURI,
					row.StartTs, row.TargetTs, row.Config, row.RemovedAt,
					row.Version, row.UpdateAt,
				}
			},
			func(expectedRowsCnt int, expectedError error) {
				changefeedInfo, err := client.queryChangefeedInfoByUUID(db, expectedQueryChangefeedInfoByUUID.UUID)
				require.ErrorIs(t, err, expectedError)
				if expectedRowsCnt != 0 {
					require.Equal(t, expectedQueryChangefeedInfoByUUID, changefeedInfo)
				} else {
					require.Nil(t, changefeedInfo)
				}
			},
			queryTypePoint,
		)
	}
}

func TestChangefeedStateClientQuerySQL(t *testing.T) {
	t.Parallel()

	backendDB, db, mock := newMockDB(t)
	defer backendDB.Close()
	client := NewORMClient("test-changefeed-state-client-query", db)

	rows := []*ChangefeedStateDO{
		{
			ChangefeedState: metadata.ChangefeedState{
				ChangefeedUUID: 1,
				State:          "state",
				// Note that warning and error could be nil.
				Warning: nil,                   /* test nil */
				Error:   &model.RunningError{}, /* test empty*/
			},
			Version:  1,
			UpdateAt: time.Now(),
		},
		{
			ChangefeedState: metadata.ChangefeedState{
				ChangefeedUUID: 2,
				State:          "state",
				Warning: &model.RunningError{
					// ref: TestRunningErrorScan
					// Time: time.Now(),
					Addr: "addr",
					Code: "warn",
				},
				Error: &model.RunningError{
					// Time: time.Now(),
					Addr: "addr",
					Code: "error",
				},
			},
			Version:  2,
			UpdateAt: time.Now(),
		},
	}

	// Test queryChangefeedStates
	expectedQueryChangefeedStates := rows
	queryChangefeedStatesRows := []interface{}{expectedQueryChangefeedStates[0], expectedQueryChangefeedStates[1]}
	runMockQueryTest(t, mock,
		"SELECT * FROM `changefeed_state`", nil,
		[]string{"changefeed_uuid", "state", "warning", "error", "version", "update_at"},
		queryChangefeedStatesRows,
		func(r interface{}) []driver.Value {
			row, ok := r.(*ChangefeedStateDO)
			require.True(t, ok)
			return []driver.Value{row.ChangefeedUUID, row.State, row.Warning, row.Error, row.Version, row.UpdateAt}
		},
		func(expectedRowsCnt int, expectedError error) {
			changefeedStates, err := client.queryChangefeedStates(db)
			require.ErrorIs(t, err, expectedError)
			require.Len(t, changefeedStates, expectedRowsCnt)
			if expectedRowsCnt != 0 {
				require.Equal(t, expectedQueryChangefeedStates, changefeedStates)
			}
		},
		queryTypeFullTable,
	)

	// Test queryChangefeedStatesByUpdateAt
	expectedQueryChangefeedStatesByUpdateAt := rows
	queryChangefeedStatesByUpdateAtRows := []interface{}{
		expectedQueryChangefeedStatesByUpdateAt[0],
		expectedQueryChangefeedStatesByUpdateAt[1],
	}
	queryAt := time.Now()
	runMockQueryTest(t, mock,
		"SELECT * FROM `changefeed_state` WHERE update_at > ?", []driver.Value{queryAt},
		[]string{"changefeed_uuid", "state", "warning", "error", "version", "update_at"},
		queryChangefeedStatesByUpdateAtRows,
		func(r interface{}) []driver.Value {
			row, ok := r.(*ChangefeedStateDO)
			require.True(t, ok)
			return []driver.Value{row.ChangefeedUUID, row.State, row.Warning, row.Error, row.Version, row.UpdateAt}
		},
		func(expectedRowsCnt int, expectedError error) {
			changefeedStates, err := client.queryChangefeedStatesByUpdateAt(db, queryAt)
			require.ErrorIs(t, err, expectedError)
			require.Len(t, changefeedStates, expectedRowsCnt)
			if expectedRowsCnt != 0 {
				require.Equal(t, expectedQueryChangefeedStatesByUpdateAt, changefeedStates)
			}
		},
		queryTypeRange,
	)

	// Test queryChangefeedStateByUUID
	for _, row := range rows {
		expectedQueryChangefeedStateByUUID := row
		queryChangefeedStateByUUIDRows := []interface{}{row}
		runMockQueryTest(t, mock,
			"SELECT * FROM `changefeed_state` WHERE changefeed_uuid = ? LIMIT 1",
			[]driver.Value{expectedQueryChangefeedStateByUUID.ChangefeedUUID},
			[]string{"changefeed_uuid", "state", "warning", "error", "version", "update_at"},
			queryChangefeedStateByUUIDRows,
			func(r interface{}) []driver.Value {
				row, ok := r.(*ChangefeedStateDO)
				require.True(t, ok)
				return []driver.Value{row.ChangefeedUUID, row.State, row.Warning, row.Error, row.Version, row.UpdateAt}
			},
			func(expectedRowsCnt int, expectedError error) {
				changefeedState, err := client.queryChangefeedStateByUUID(db, expectedQueryChangefeedStateByUUID.ChangefeedUUID)
				require.ErrorIs(t, err, expectedError)
				if expectedRowsCnt != 0 {
					require.Equal(t, expectedQueryChangefeedStateByUUID, changefeedState)
				} else {
					require.Nil(t, changefeedState)
				}
			},
			queryTypePoint,
		)
	}

	// Test queryChangefeedStateByUUIDWithLock
	for _, row := range rows {
		expectedQueryChangefeedStateByUUIDWithLock := row
		queryChangefeedStateByUUIDWithLockRows := []interface{}{row}
		runMockQueryTest(t, mock,
			"SELECT * FROM `changefeed_state` WHERE changefeed_uuid = ? LIMIT 1 LOCK IN SHARE MODE",
			[]driver.Value{expectedQueryChangefeedStateByUUIDWithLock.ChangefeedUUID},
			[]string{"changefeed_uuid", "state", "warning", "error", "version", "update_at"},
			queryChangefeedStateByUUIDWithLockRows,
			func(r interface{}) []driver.Value {
				row, ok := r.(*ChangefeedStateDO)
				require.True(t, ok)
				return []driver.Value{row.ChangefeedUUID, row.State, row.Warning, row.Error, row.Version, row.UpdateAt}
			},
			func(expectedRowsCnt int, expectedError error) {
				changefeedState, err := client.queryChangefeedStateByUUIDWithLock(db, expectedQueryChangefeedStateByUUIDWithLock.ChangefeedUUID)
				require.ErrorIs(t, err, expectedError)
				if expectedRowsCnt != 0 {
					require.Equal(t, expectedQueryChangefeedStateByUUIDWithLock, changefeedState)
				} else {
					require.Nil(t, changefeedState)
				}
			},
			queryTypePoint,
		)
	}
}

func TestScheduleClientQuerySQL(t *testing.T) {
	t.Parallel()

	backendDB, db, mock := newMockDB(t)
	defer backendDB.Close()
	client := NewORMClient("test-schedule-client-query", db)

	ownerCapture := "test-schedule-client-query"
	rows := []*ScheduleDO{
		{
			ScheduledChangefeed: metadata.ScheduledChangefeed{
				ChangefeedUUID: 1,
				Owner:          nil, /* test nil */
				OwnerState:     metadata.SchedRemoved,
				Processors:     nil, /* test nil */
				TaskPosition: metadata.ChangefeedProgress{
					CheckpointTs: 1,
				},
			},
			Version:  1,
			UpdateAt: time.Now(),
		},
		{
			ScheduledChangefeed: metadata.ScheduledChangefeed{
				ChangefeedUUID: 2,
				Owner:          &ownerCapture,
				OwnerState:     metadata.SchedRemoved,
				Processors:     &ownerCapture,
				TaskPosition: metadata.ChangefeedProgress{
					CheckpointTs: 2,
				},
			},
			Version:  2,
			UpdateAt: time.Now(),
		},
	}

	// Test querySchedules
	expectedQuerySchedules := rows
	querySchedulesRows := []interface{}{expectedQuerySchedules[0], expectedQuerySchedules[1]}
	runMockQueryTest(t, mock,
		"SELECT * FROM `schedule`", nil,
		[]string{
			"changefeed_uuid", "owner", "owner_state", "processors", "task_position",
			"version", "update_at",
		},
		querySchedulesRows,
		func(r interface{}) []driver.Value {
			row, ok := r.(*ScheduleDO)
			require.True(t, ok)
			return []driver.Value{
				row.ChangefeedUUID, row.Owner, row.OwnerState, row.Processors, row.TaskPosition,
				row.Version, row.UpdateAt,
			}
		},
		func(expectedRowsCnt int, expectedError error) {
			schedules, err := client.querySchedules(db)
			require.ErrorIs(t, err, expectedError)
			require.Len(t, schedules, expectedRowsCnt)
			if expectedRowsCnt != 0 {
				require.Equal(t, expectedQuerySchedules, schedules)
			}
		},
		queryTypeFullTable,
	)

	// Test querySchedulesByUpdateAt
	expectedQuerySchedulesByUpdateAt := rows
	querySchedulesByUpdateAtRows := []interface{}{
		expectedQuerySchedulesByUpdateAt[0],
		expectedQuerySchedulesByUpdateAt[1],
	}
	queryAt := time.Now()
	runMockQueryTest(t, mock,
		"SELECT * FROM `schedule` WHERE update_at > ?", []driver.Value{queryAt},
		[]string{
			"changefeed_uuid", "owner", "owner_state", "processors", "task_position",
			"version", "update_at",
		},
		querySchedulesByUpdateAtRows,
		func(r interface{}) []driver.Value {
			row, ok := r.(*ScheduleDO)
			require.True(t, ok)
			return []driver.Value{
				row.ChangefeedUUID, row.Owner, row.OwnerState, row.Processors, row.TaskPosition,
				row.Version, row.UpdateAt,
			}
		},
		func(expectedRowsCnt int, expectedError error) {
			schedules, err := client.querySchedulesByUpdateAt(db, queryAt)
			require.ErrorIs(t, err, expectedError)
			require.Len(t, schedules, expectedRowsCnt)
			if expectedRowsCnt != 0 {
				require.Equal(t, expectedQuerySchedulesByUpdateAt, schedules)
			}
		},
		queryTypeRange,
	)

	// Test querySchedulesByOwnerIDAndUpdateAt
	expectedQuerySchedulesByOwnerIDAndUpdateAt := rows
	querySchedulesByOwnerIDAndUpdateAtRows := []interface{}{
		expectedQuerySchedulesByOwnerIDAndUpdateAt[0],
		expectedQuerySchedulesByOwnerIDAndUpdateAt[1],
	}
	queryAt = time.Now()
	runMockQueryTest(t, mock,
		"SELECT * FROM `schedule` WHERE owner = ? and update_at > ?", []driver.Value{ownerCapture, queryAt},
		[]string{
			"changefeed_uuid", "owner", "owner_state", "processors", "task_position",
			"version", "update_at",
		},
		querySchedulesByOwnerIDAndUpdateAtRows,
		func(r interface{}) []driver.Value {
			row, ok := r.(*ScheduleDO)
			require.True(t, ok)
			return []driver.Value{
				row.ChangefeedUUID, row.Owner, row.OwnerState, row.Processors, row.TaskPosition,
				row.Version, row.UpdateAt,
			}
		},
		func(expectedRowsCnt int, expectedError error) {
			schedules, err := client.querySchedulesByOwnerIDAndUpdateAt(db, ownerCapture, queryAt)
			require.ErrorIs(t, err, expectedError)
			require.Len(t, schedules, expectedRowsCnt)
			if expectedRowsCnt != 0 {
				require.Equal(t, expectedQuerySchedulesByOwnerIDAndUpdateAt, schedules)
			}
		},
		queryTypeRange,
	)

	// Test queryScheduleByUUID
	for _, row := range rows {
		expectedQueryScheduleByUUID := row
		queryScheduleByUUIDRows := []interface{}{row}
		runMockQueryTest(t, mock,
			"SELECT * FROM `schedule` WHERE changefeed_uuid = ? LIMIT 1",
			[]driver.Value{expectedQueryScheduleByUUID.ChangefeedUUID},
			[]string{
				"changefeed_uuid", "owner", "owner_state", "processors", "task_position",
				"version", "update_at",
			},
			queryScheduleByUUIDRows,
			func(r interface{}) []driver.Value {
				row, ok := r.(*ScheduleDO)
				require.True(t, ok)
				return []driver.Value{
					row.ChangefeedUUID, row.Owner, row.OwnerState, row.Processors, row.TaskPosition,
					row.Version, row.UpdateAt,
				}
			},
			func(expectedRowsCnt int, expectedError error) {
				schedule, err := client.queryScheduleByUUID(db, expectedQueryScheduleByUUID.ChangefeedUUID)
				require.ErrorIs(t, err, expectedError)
				if expectedRowsCnt != 0 {
					require.Equal(t, expectedQueryScheduleByUUID, schedule)
				} else {
					require.Nil(t, schedule)
				}
			},
			queryTypePoint,
		)
	}

	// Test querySchedulesUinqueOwnerIDs
	expectedQuerySchedulesUinqueOwnerIDs := []string{"owner1", "owner2"}
	querySchedulesUinqueOwnerIDsRows := []interface{}{
		expectedQuerySchedulesUinqueOwnerIDs[0],
		expectedQuerySchedulesUinqueOwnerIDs[1],
	}
	runMockQueryTest(t, mock,
		"SELECT DISTINCT `owner` FROM `schedule` WHERE owner IS NOT NULL", nil,
		[]string{"owner"},
		querySchedulesUinqueOwnerIDsRows,
		func(r interface{}) []driver.Value {
			row, ok := r.(string)
			require.True(t, ok)
			return []driver.Value{row}
		},
		func(expectedRowsCnt int, expectedError error) {
			ownerIDs, err := client.querySchedulesUinqueOwnerIDs(db)
			require.ErrorIs(t, err, expectedError)
			require.Len(t, ownerIDs, expectedRowsCnt)
			if expectedRowsCnt != 0 {
				require.Equal(t, expectedQuerySchedulesUinqueOwnerIDs, ownerIDs)
			}
		},
		queryTypeFullTable,
	)
}

func TestProgressClientQuerySQL(t *testing.T) {
	t.Parallel()

	backendDB, db, mock := newMockDB(t)
	defer backendDB.Close()
	client := NewORMClient("test-progress-client-query", db)

	rows := []*ProgressDO{
		{
			CaptureID: "captureID-1",
			Progress: &metadata.CaptureProgress{
				1: {
					CheckpointTs:      1,
					MinTableBarrierTs: 1,
				},
				2: {
					CheckpointTs:      2,
					MinTableBarrierTs: 2,
				},
			},
			Version:  1,
			UpdateAt: time.Now(),
		},
		{
			CaptureID: "captureID-2",
			Progress:  &metadata.CaptureProgress{},
			Version:   2,
			UpdateAt:  time.Now(),
		},
	}

	// Test queryProgresses
	expectedqueryProgresses := rows
	queryProgressesRows := []interface{}{expectedqueryProgresses[0], expectedqueryProgresses[1]}
	runMockQueryTest(t, mock,
		"SELECT * FROM `progress`", nil,
		[]string{"capture_id", "progress", "version", "update_at"},
		queryProgressesRows,
		func(r interface{}) []driver.Value {
			row, ok := r.(*ProgressDO)
			require.True(t, ok)
			return []driver.Value{row.CaptureID, row.Progress, row.Version, row.UpdateAt}
		},
		func(expectedRowsCnt int, expectedError error) {
			progresses, err := client.queryProgresses(db)
			require.ErrorIs(t, err, expectedError)
			require.Len(t, progresses, expectedRowsCnt)
			if expectedRowsCnt != 0 {
				require.Equal(t, expectedqueryProgresses, progresses)
			}
		},
		queryTypeFullTable,
	)

	// Test queryProgressesByUpdateAt
	expectedqueryProgressesByUpdateAt := rows
	queryProgressesByUpdateAtRows := []interface{}{
		expectedqueryProgressesByUpdateAt[0],
		expectedqueryProgressesByUpdateAt[1],
	}
	queryAt := time.Now()
	runMockQueryTest(t, mock,
		"SELECT * FROM `progress` WHERE update_at > ?", []driver.Value{queryAt},
		[]string{"capture_id", "progress", "version", "update_at"},
		queryProgressesByUpdateAtRows,
		func(r interface{}) []driver.Value {
			row, ok := r.(*ProgressDO)
			require.True(t, ok)
			return []driver.Value{row.CaptureID, row.Progress, row.Version, row.UpdateAt}
		},
		func(expectedRowsCnt int, expectedError error) {
			progresses, err := client.queryProgressesByUpdateAt(db, queryAt)
			require.ErrorIs(t, err, expectedError)
			require.Len(t, progresses, expectedRowsCnt)
			if expectedRowsCnt != 0 {
				require.Equal(t, expectedqueryProgressesByUpdateAt, progresses)
			}
		},
		queryTypeRange,
	)

	// Test queryProgressByCaptureID
	for _, row := range rows {
		expectedqueryProgressByCaptureID := row
		queryProgressByCaptureIDRows := []interface{}{row}
		runMockQueryTest(t, mock,
			"SELECT * FROM `progress` WHERE capture_id = ? LIMIT 1",
			[]driver.Value{expectedqueryProgressByCaptureID.CaptureID},
			[]string{"capture_id", "progress", "version", "update_at"},
			queryProgressByCaptureIDRows,
			func(r interface{}) []driver.Value {
				row, ok := r.(*ProgressDO)
				require.True(t, ok)
				return []driver.Value{row.CaptureID, row.Progress, row.Version, row.UpdateAt}
			},
			func(expectedRowsCnt int, expectedError error) {
				progress, err := client.queryProgressByCaptureID(db, expectedqueryProgressByCaptureID.CaptureID)
				require.ErrorIs(t, err, expectedError)
				if expectedRowsCnt != 0 {
					require.Equal(t, expectedqueryProgressByCaptureID, progress)
				} else {
					require.Nil(t, progress)
				}
			},
			queryTypePoint,
		)
	}

	// Test queryProgressByCaptureIDsWithLock
	expectedqueryProgressByCaptureIDsWithLock := rows
	queryProgressByCaptureIDsWithLockRows := []interface{}{rows[0], rows[1]}
	captureIDs := []string{expectedqueryProgressByCaptureIDsWithLock[0].CaptureID, expectedqueryProgressByCaptureIDsWithLock[1].CaptureID}
	runMockQueryTest(t, mock,
		"SELECT * FROM `progress` WHERE capture_id in (?,?) LOCK IN SHARE MODE",
		[]driver.Value{expectedqueryProgressByCaptureIDsWithLock[0].CaptureID, expectedqueryProgressByCaptureIDsWithLock[1].CaptureID},
		[]string{"capture_id", "progress", "version", "update_at"},
		queryProgressByCaptureIDsWithLockRows,
		func(r interface{}) []driver.Value {
			row, ok := r.(*ProgressDO)
			require.True(t, ok)
			return []driver.Value{row.CaptureID, row.Progress, row.Version, row.UpdateAt}
		},
		func(expectedRowsCnt int, expectedError error) {
			progress, err := client.queryProgressByCaptureIDsWithLock(db, captureIDs)
			require.ErrorIs(t, err, expectedError)
			require.Len(t, progress, expectedRowsCnt)
			if expectedRowsCnt != 0 {
				require.Equal(t, expectedqueryProgressByCaptureIDsWithLock, progress)
			}
		},
		queryTypeRange,
	)
}
