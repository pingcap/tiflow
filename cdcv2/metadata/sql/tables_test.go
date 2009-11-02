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
	"database/sql"
	"testing"
	"time"

	"github.com/pingcap/tiflow/cdcv2/metadata/sql/internal"
	"github.com/pingcap/tiflow/engine/pkg/dbutil"
	ormUtil "github.com/pingcap/tiflow/engine/pkg/orm"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// only for test
const TableNameElection = internal.TableNameElection

type Election = internal.Election

func initAndCleanupTablesForTest(t *testing.T, dsn string) (*sql.DB, *gorm.DB) {
	backendDB, err := dbutil.NewSQLDB("mysql", dsn, nil)
	require.NoError(t, err)

	db, err := ormUtil.NewGormDB(backendDB, "mysql")
	require.NoError(t, err)

	db.AutoMigrate(&Election{})
	db.Exec("TRUNCATE " + TableNameElection)

	AutoMigrate(db)
	db.Exec("TRUNCATE " + TableNameUpstream)
	db.Exec("TRUNCATE " + TableNameChangefeedInfo)
	db.Exec("TRUNCATE " + TableNameChangefeedState)
	db.Exec("TRUNCATE " + TableNameSchedule)
	db.Exec("TRUNCATE " + TableNameProgress)

	return backendDB, db
}

func genMockData(t *testing.T, db *gorm.DB, captureNum int) {
	election := internal.NewTestElectionRow(captureNum)
	ret := db.Create(&election)
	require.NoError(t, ret.Error)
	require.Equal(t, int64(1), ret.RowsAffected)

	ret = db.Create(&UpstreamDO{
		ID:        1,
		Endpoints: "localhost:2379",
		Version:   1,
		// UpdateAt:  time.Now(),
	})
	require.NoError(t, ret.Error)
	require.Equal(t, int64(1), ret.RowsAffected)

	c := ReplicaConfig(*config.GetDefaultReplicaConfig())
	ret = db.Create(&ChangefeedInfoDO{
		UUID:       1,
		ID:         "changefeed-1",
		Namespace:  "default",
		UpstreamID: 1,
		Config:     c,
		Version:    1,
		UpdateAt:   time.Now(),
	})
	require.NoError(t, ret.Error)
	require.Equal(t, int64(1), ret.RowsAffected)

	ret = db.Create(&ChangefeedStateDO{
		ChangefeedUUID: 1,
		Version:        1,
		UpdateAt:       time.Now(),
	})
	require.NoError(t, ret.Error)
	require.Equal(t, int64(1), ret.RowsAffected)

	o := "capture-0"
	ret = db.Create(&ScheduleDO{
		ChangefeedUUID: 1,
		Owner:          &o,
		OwnerState:     "removed",
		Processors:     &o,
		TaskPosition:   &ChangefeedProgress{},
		Version:        1,
		UpdateAt:       time.Now(),
	})
	require.NoError(t, ret.Error)
	require.Equal(t, int64(1), ret.RowsAffected)

	ret = db.Create(&ProgressDO{
		CaptureID: "capture-0",
		Version:   1,
		UpdateAt:  time.Now(),
	})
	require.NoError(t, ret.Error)
	require.Equal(t, int64(1), ret.RowsAffected)
}

func TestMigrateTableBasicInsert(t *testing.T) {
	t.Parallel()

	dsn := "root:123456@tcp(10.2.6.9:3306)/test_local?charset=utf8mb4&parseTime=true&loc=Local&multiStatements=true"
	backendDB, db := initAndCleanupTablesForTest(t, dsn)
	defer backendDB.Close()

	genMockData(t, db, 3)

	ret := db.Model(&UpstreamDO{}).Where("id = ?", 1).Update("version", 2)
	require.NoError(t, ret.Error)
}

func TestXxx(t *testing.T) {
	t.Parallel()

	dsn := "root:123456@tcp(10.2.6.9:3306)/test_local?charset=utf8mb4&parseTime=true&loc=Local&multiStatements=true"
	db, err := dbutil.NewSQLDB("mysql", dsn, nil)
	require.NoError(t, err)
	defer db.Close()

	ormDB, err := ormUtil.NewGormDB(db, "mysql")
	if err != nil {
		panic("failed to connect database")
	}

	_, err = ormDB.DB()
	require.NoError(t, err)

	ormDB.AutoMigrate(&Election{}, &ScheduleDO{})
	ormDB.Exec("TRUNCATE " + TableNameElection)
	ormDB.Exec("TRUNCATE " + TableNameSchedule)

	ormDB.Create(&Election{ID: 1, Version: 1, LeaderID: "leader1"})
	o := "owner1"
	ormDB.Create(&ScheduleDO{
		ChangefeedUUID: 1,
		Version:        1,
		Owner:          &o,
		OwnerState:     "",
		Processors:     &o,
		TaskPosition:   &ChangefeedProgress{},
	})

	// ret, err := db.Exec("BEGIN;UPDATE schedule SET owner='owner11', owner_state='launched', processors='owner11' WHERE changefeed_uuid=1;COMMIT;")
	// require.NoError(t, err)
	// aft, err := ret.RowsAffected()
	// require.NoError(t, err)
	// require.Equal(t, int64(1), aft)

	// var leaderID string
	// ret := ormDB.Raw("SELECT leader_id FROM election WHERE leader_id = 'leader1' LOCK IN SHARE MODE")
	// ret.Scan(&leaderID)
	// require.Equal(t, "leader1", leaderID)

	// seSTR := `BEGIN;
	// SELECT 1 FROM election LOCK IN SHARE MODE;
	// UPDATE schedule SET owner=?, owner_state='launched', processors=?
	// 	WHERE changefeed_uuid=? AND EXISTS (
	// 	  SELECT 1 FROM election WHERE election.leader_id = ?
	// );
	// COMMIT;`
	// seSQL := ormDB.ToSQL(func(tx *gorm.DB) *gorm.DB {
	// 	return tx.Raw(seSTR, "owner222", "owner232", 1, "leader1")
	// })

	// t.Error(seSQL)
	// res := ormDB.Exec(seSQL)
	// require.NoError(t, res.Error)
	// require.Equal(t, int64(1), res.RowsAffected)

	err = ormDB.Transaction(func(tx *gorm.DB) error {
		var leaderID string
		ret := tx.Raw("SELECT leader_id FROM election WHERE leader_id = 'leader1' LOCK IN SHARE MODE").Scan(&leaderID)
		if ret.Error != nil {
			return ret.Error
		} else if ret.RowsAffected == 0 {
			return errors.ErrNotController
		}
		if leaderID != "leader1" {
			return errors.ErrNotController
		}
		time.Sleep(time.Hour)

		/* orm */
		tx.Table(TableNameSchedule).Where("changefeed_uuid = ?", 1).Updates(map[string]interface{}{
			"owner":       "owner2",
			"owner_state": "launched",
			"processors":  "owner2",
		})

		// 	/* raw sql */
		// 	ret = tx.Exec("UPDATE schedule SET owner=?, owner_state='launched', processors=? WHERE changefeed_uuid=?", "owner3", "owner3", 1)
		require.Equal(t, int64(1), ret.RowsAffected)
		return ret.Error
	})
	require.NoError(t, err)

	// res, err := db.Exec(seSTR, "owner2", "owner2", 1, "leader1")
	// require.NoError(t, err)

	// aft, err := res.RowsAffected()
	// require.NoError(t, err)
	// require.Equal(t, int64(1), aft)
}
