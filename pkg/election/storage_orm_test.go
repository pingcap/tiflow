// Copyright 2022 PingCAP, Inc.
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

package election

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	ormUtil "github.com/pingcap/tiflow/engine/pkg/orm"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func newORMStorageAndMock(t *testing.T) (*ORMStorage, sqlmock.Sqlmock) {
	backendDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)

	mock.ExpectQuery("SELECT VERSION()").
		WillReturnRows(sqlmock.NewRows([]string{"VERSION()"}).AddRow("5.7.35-log"))
	db, err := ormUtil.NewGormDB(backendDB, "mysql")
	require.NoError(t, err)

	mock.ExpectQuery("SELECT SCHEMA_NAME from Information_schema.SCHEMATA " +
		"where SCHEMA_NAME LIKE ? ORDER BY SCHEMA_NAME=? DESC limit 1").WillReturnRows(
		sqlmock.NewRows([]string{"SCHEMA_NAME"}))
	mock.ExpectExec("CREATE TABLE `test` (`id` int(10) unsigned,`leader_id` text NOT NULL," +
		"`record` text,`version` bigint(20) unsigned NOT NULL,PRIMARY KEY (`id`))").
		WillReturnResult(sqlmock.NewResult(0, 0))

	s, err := NewORMStorage(db, "test")
	require.NoError(t, err)

	return s, mock
}

func TestORMStorageGetEmptyRecord(t *testing.T) {
	s, mock := newORMStorageAndMock(t)

	mock.ExpectQuery("SELECT * FROM `test` WHERE id = ? LIMIT 1").
		WithArgs(1).WillReturnRows(sqlmock.NewRows([]string{"id", "leader_id", "record", "version"}))
	record, err := s.Get(context.Background())
	require.NoError(t, err)
	require.Equal(t, &Record{}, record)
}

func TestORMStorageGetExistingRecord(t *testing.T) {
	s, mock := newORMStorageAndMock(t)

	expectedRecord := &Record{
		LeaderID: "id1",
		Members: []*Member{
			{
				ID:   "id1",
				Name: "name1",
			},
			{
				ID:   "id2",
				Name: "name2",
			},
		},
		Version: 1,
	}
	recordBytes, err := json.Marshal(expectedRecord)
	require.NoError(t, err)

	mock.ExpectQuery("SELECT * FROM `test` WHERE id = ? LIMIT 1").
		WithArgs(1).WillReturnRows(sqlmock.NewRows([]string{"id", "leader_id", "record", "version"}).
		AddRow(1, "id1", recordBytes, 1))
	record, err := s.Get(context.Background())
	require.NoError(t, err)
	require.Equal(t, expectedRecord, record)
}

func TestORMStorageInsertRecord(t *testing.T) {
	s, mock := newORMStorageAndMock(t)

	record := &Record{
		LeaderID: "id1",
		Members: []*Member{
			{
				ID:   "id1",
				Name: "name1",
			},
		},
		Version: 0, // 0 means record not created before.
	}
	recordBytes, err := json.Marshal(record)
	require.NoError(t, err)

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO `test` (`leader_id`,`record`,`version`,`id`) VALUES (?,?,?,?),(?,?,?,?)").
		WithArgs("id1", recordBytes, 1, recordRowID, "id1", nil, 1, leaderRowID).
		WillReturnResult(sqlmock.NewResult(1, 2))
	mock.ExpectCommit()

	err = s.Update(context.Background(), record, true)
	require.NoError(t, err)
}

func TestORMStorageUpdateMember(t *testing.T) {
	leaderNotChanged := false
	s, mock := newORMStorageAndMock(t)

	record := &Record{
		LeaderID: "id1",
		Members: []*Member{
			{
				ID:   "id1",
				Name: "name1",
			},
		},
		Version: 1,
	}
	recordBytes, err := json.Marshal(record)
	require.NoError(t, err)

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE `test` SET `leader_id`=?,`record`=?,`version`=? WHERE id = ? AND version = ?").
		WithArgs("id1", recordBytes, 2, recordRowID, 1).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectRollback()
	err = s.Update(context.Background(), record, leaderNotChanged)
	require.ErrorIs(t, err, errors.ErrElectionRecordConflict)

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE `test` SET `leader_id`=?,`record`=?,`version`=? WHERE id = ? AND version = ?").
		WithArgs("id1", recordBytes, 2, recordRowID, 1).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	err = s.Update(context.Background(), record, leaderNotChanged)
	require.NoError(t, err)
}

func TestORMStorageUpdateLeader(t *testing.T) {
	leaderChanged := true
	s, mock := newORMStorageAndMock(t)

	record := &Record{
		LeaderID: "id1",
		Members: []*Member{
			{
				ID:   "id1",
				Name: "name1",
			},
		},
		Version: 1,
	}
	recordBytes, err := json.Marshal(record)
	require.NoError(t, err)

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE `test` SET `leader_id`=?,`record`=?,`version`=? WHERE id = ? AND version = ?").
		WithArgs("id1", recordBytes, 2, recordRowID, 1).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectRollback()
	err = s.Update(context.Background(), record, leaderChanged)
	require.ErrorIs(t, err, errors.ErrElectionRecordConflict)

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE `test` SET `leader_id`=?,`record`=?,`version`=? WHERE id = ? AND version = ?").
		WithArgs("id1", recordBytes, 2, recordRowID, 1).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("UPDATE `test` SET `leader_id`=?,`version`=? WHERE id = ?").
		WithArgs("id1", 2, leaderRowID).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectRollback()
	err = s.Update(context.Background(), record, leaderChanged)
	require.ErrorIs(t, err, errors.ErrElectionRecordConflict)

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE `test` SET `leader_id`=?,`record`=?,`version`=? WHERE id = ? AND version = ?").
		WithArgs("id1", recordBytes, 2, recordRowID, 1).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("UPDATE `test` SET `leader_id`=?,`version`=? WHERE id = ?").
		WithArgs("id1", 2, leaderRowID).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	err = s.Update(context.Background(), record, leaderChanged)
	require.NoError(t, err)
}

func TestORMStorageTxnWithLeaderCheck(t *testing.T) {
	s, mock := newORMStorageAndMock(t)

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT `leader_id` FROM `test` WHERE id = ? and leader_id = ? LIMIT 1 LOCK IN SHARE MODE").
		WithArgs(leaderRowID, "leader1").WillReturnRows(sqlmock.NewRows([]string{"leader_id"}))
	mock.ExpectRollback()
	doNothing := func(*gorm.DB) error {
		return nil
	}
	err := s.TxnWithLeaderLock(context.Background(), "leader1", doNothing)
	require.ErrorIs(t, err, errors.ErrElectorNotLeader)

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT `leader_id` FROM `test` WHERE id = ? and leader_id = ? LIMIT 1 LOCK IN SHARE MODE").
		WithArgs(leaderRowID, "leader1").
		WillReturnRows(sqlmock.NewRows([]string{"leader_id"}).AddRow("leader1"))
	mock.ExpectQuery("SELECT * FROM `test` WHERE id = ? LIMIT 1").
		WithArgs(1).WillReturnRows(sqlmock.NewRows([]string{"id", "leader_id", "record", "version"}))
	mock.ExpectCommit()
	doTxn := func(tx *gorm.DB) error {
		_, err := s.Get(context.Background())
		require.NoError(t, err)
		return nil
	}
	err = s.TxnWithLeaderLock(context.Background(), "leader1", doTxn)
	require.NoError(t, err)
}
