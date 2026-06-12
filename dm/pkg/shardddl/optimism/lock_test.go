// Copyright 2020 PingCAP, Inc.
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

package optimism

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/schemacmp"
	"github.com/pingcap/tiflow/dm/config/dbconfig"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	"github.com/pingcap/tiflow/dm/pkg/cputil"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type testLock struct {
	suite.Suite
}

func TestLock(t *testing.T) {
	suite.Run(t, new(testLock))
}

func (t *testLock) SetupSuite() {
	t.Require().NoError(log.InitLogger(&log.Config{}))
}

func (t *testLock) TearDownSuite() {
	clearTestInfoOperation(t.T())
}

func (t *testLock) TestLockTrySyncNormal() {
	var (
		ID               = "test_lock_try_sync_normal-`foo`.`bar`"
		task             = "test_lock_try_sync_normal"
		sources          = []string{"mysql-replica-1", "mysql-replica-2"}
		downSchema       = "db"
		downTable        = "bar"
		dbs              = []string{"db1", "db2"}
		tbls             = []string{"bar1", "bar2"}
		tableCount       = len(sources) * len(dbs) * len(tbls)
		p                = parser.New()
		se               = mock.NewContext()
		tblID      int64 = 111
		DDLs1            = []string{"ALTER TABLE bar ADD COLUMN c1 INT"}
		DDLs2            = []string{"ALTER TABLE bar ADD COLUMN c2 BIGINT", "ALTER TABLE bar ADD COLUMN c3 TEXT"}
		DDLs3            = []string{"ALTER TABLE bar DROP COLUMN c3"}
		DDLs4            = []string{"ALTER TABLE bar DROP COLUMN c2", "ALTER TABLE bar DROP COLUMN c1"}
		ti0              = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY)`)
		ti1              = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT)`)
		ti2              = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT, c2 BIGINT, c3 TEXT)`)
		ti2_1            = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT, c2 BIGINT)`)
		ti3              = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT, c2 BIGINT)`)
		ti4              = ti0
		ti4_1            = ti1
		tables           = map[string]map[string]struct{}{
			dbs[0]: {tbls[0]: struct{}{}, tbls[1]: struct{}{}},
			dbs[1]: {tbls[0]: struct{}{}, tbls[1]: struct{}{}},
		}
		tts = []TargetTable{
			newTargetTable(task, sources[0], downSchema, downTable, tables),
			newTargetTable(task, sources[1], downSchema, downTable, tables),
		}

		l = NewLock(etcdTestCli, ID, task, downSchema, downTable, schemacmp.Encode(ti0), tts, nil)

		vers = map[string]map[string]map[string]int64{
			sources[0]: {
				dbs[0]: {tbls[0]: 0, tbls[1]: 0},
				dbs[1]: {tbls[0]: 0, tbls[1]: 0},
			},
			sources[1]: {
				dbs[0]: {tbls[0]: 0, tbls[1]: 0},
				dbs[1]: {tbls[0]: 0, tbls[1]: 0},
			},
		}
	)

	// the initial status is synced.
	t.checkLockSynced(l)
	t.checkLockNoDone(l)

	// CASE: all tables execute a single & same DDL (schema become larger).
	syncedCount := 0
	for _, source := range sources {
		if source == sources[len(sources)-1] {
			ready := l.Ready()
			for _, source2 := range sources {
				synced := source != source2 // tables before the last source have synced.
				for _, db2 := range dbs {
					for _, tbl2 := range tbls {
						t.Require().Equal(synced, ready[source2][db2][tbl2])
					}
				}
			}
		}

		for _, db := range dbs {
			for _, tbl := range tbls {
				info := newInfoWithVersion(task, source, db, tbl, downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1}, vers)
				DDLs, cols, err := l.TrySync(info, tts)
				t.Require().NoError(err)
				t.Require().Equal(DDLs1, DDLs)
				t.Require().Equal([]string{}, cols)
				t.Require().Equal(vers, l.versions)

				syncedCount++
				synced, remain := l.IsSynced()
				t.Require().Equal(syncedCount == tableCount, synced)
				t.Require().Equal(tableCount-syncedCount, remain)
				t.Require().Equal(l.synced, synced)
			}
		}
	}
	// synced again after all tables applied the DDL.
	t.checkLockSynced(l)
	t.checkLockNoDone(l)

	// CASE: TrySync again after synced is idempotent.
	info := newInfoWithVersion(task, sources[0], dbs[0], tbls[0], downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1}, vers)
	DDLs, cols, err := l.TrySync(info, tts)
	t.Require().NoError(err)
	t.Require().Equal(DDLs1, DDLs)
	t.Require().Equal([]string{}, cols)
	t.Require().Equal(vers, l.versions)
	t.checkLockSynced(l)
	t.checkLockNoDone(l)

	// CASE: need to add more than one DDL to reach the desired schema (schema become larger).
	// add two columns for one table.
	info = newInfoWithVersion(task, sources[0], dbs[0], tbls[0], downSchema, downTable, DDLs2, ti1, []*model.TableInfo{ti2_1, ti2}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	t.Require().NoError(err)
	t.Require().Equal(DDLs2, DDLs)
	t.Require().Equal([]string{}, cols)
	t.Require().Equal(vers, l.versions)
	ready := l.Ready()
	t.Require().True(ready[sources[0]][dbs[0]][tbls[0]])
	t.Require().False(ready[sources[0]][dbs[0]][tbls[1]])

	// TrySync again is idempotent (more than one DDL).
	info = newInfoWithVersion(task, sources[0], dbs[0], tbls[0], downSchema, downTable, DDLs2, ti1, []*model.TableInfo{ti2_1, ti2}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	t.Require().NoError(err)
	t.Require().Equal(DDLs2, DDLs)
	t.Require().Equal([]string{}, cols)
	t.Require().Equal(vers, l.versions)
	ready = l.Ready()
	t.Require().True(ready[sources[0]][dbs[0]][tbls[0]])
	t.Require().False(ready[sources[0]][dbs[0]][tbls[1]])

	// add only the first column for another table.
	info = newInfoWithVersion(task, sources[0], dbs[0], tbls[1], downSchema, downTable, DDLs2[0:1], ti1, []*model.TableInfo{ti2_1}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	t.Require().NoError(err)
	t.Require().Equal(DDLs2[0:1], DDLs)
	t.Require().Equal([]string{}, cols)
	t.Require().Equal(vers, l.versions)
	ready = l.Ready()
	t.Require().True(ready[sources[0]][dbs[0]][tbls[0]])
	t.Require().False(ready[sources[0]][dbs[0]][tbls[1]])
	synced, remain := l.IsSynced()
	t.Require().False(synced)
	t.Require().Equal(tableCount-1, remain)
	t.Require().Equal(l.synced, synced)
	cmp, err := l.tables[sources[0]][dbs[0]][tbls[0]].Compare(l.tables[sources[0]][dbs[0]][tbls[1]])
	t.Require().NoError(err)
	t.Require().Equal(1, cmp)

	// TrySync again is idempotent
	info = newInfoWithVersion(task, sources[0], dbs[0], tbls[1], downSchema, downTable, DDLs2[0:1], ti1, []*model.TableInfo{ti2_1}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	t.Require().NoError(err)
	t.Require().Equal(DDLs2[0:1], DDLs)
	t.Require().Equal([]string{}, cols)
	t.Require().Equal(vers, l.versions)
	ready = l.Ready()
	t.Require().True(ready[sources[0]][dbs[0]][tbls[0]])
	t.Require().False(ready[sources[0]][dbs[0]][tbls[1]])

	// add the second column for another table.
	info = newInfoWithVersion(task, sources[0], dbs[0], tbls[1], downSchema, downTable, DDLs2[1:2], ti2_1, []*model.TableInfo{ti2}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	t.Require().NoError(err)
	t.Require().Equal(DDLs2[1:2], DDLs)
	t.Require().Equal([]string{}, cols)
	t.Require().Equal(vers, l.versions)
	ready = l.Ready()
	t.Require().True(ready[sources[0]][dbs[0]][tbls[1]]) // ready now.
	synced, remain = l.IsSynced()
	t.Require().False(synced)
	t.Require().Equal(tableCount-2, remain)
	t.Require().Equal(l.synced, synced)
	cmp, err = l.tables[sources[0]][dbs[0]][tbls[0]].Compare(l.tables[sources[0]][dbs[0]][tbls[1]])
	t.Require().NoError(err)
	t.Require().Equal(0, cmp)

	// Try again (for the second DDL).
	info = newInfoWithVersion(task, sources[0], dbs[0], tbls[1], downSchema, downTable, DDLs2[1:2], ti2_1, []*model.TableInfo{ti2}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	t.Require().NoError(err)
	t.Require().Equal(DDLs2[1:2], DDLs)
	t.Require().Equal([]string{}, cols)
	t.Require().Equal(vers, l.versions)

	t.trySyncForAllTablesLarger(l, DDLs2, ti1, []*model.TableInfo{ti2_1, ti2}, tts, vers)
	t.checkLockSynced(l)
	t.checkLockNoDone(l)

	// CASE: all tables execute a single & same DDL (schema become smaller).
	syncedCount = 0
	for _, source := range sources {
		if source == sources[len(sources)-1] {
			ready = l.Ready()
			for _, source2 := range sources {
				synced = source == source2 // tables before the last source have not synced.
				for _, db2 := range dbs {
					for _, tbl2 := range tbls {
						t.Require().Equal(synced, ready[source2][db2][tbl2])
					}
				}
			}
		}

		for _, db := range dbs {
			for _, tbl := range tbls {
				syncedCount++
				info = newInfoWithVersion(task, source, db, tbl, downSchema, downTable, DDLs3, ti2, []*model.TableInfo{ti3}, vers)
				DDLs, cols, err = l.TrySync(info, tts)
				t.Require().NoError(err)
				t.Require().Equal(vers, l.versions)
				t.Require().Equal([]string{"c3"}, cols)
				synced, remain = l.IsSynced()
				t.Require().Equal(l.synced, synced)
				if syncedCount == tableCount {
					t.Require().Equal(DDLs3, DDLs)
					t.Require().True(synced)
					t.Require().Equal(0, remain)
				} else {
					t.Require().Equal([]string{}, DDLs)
					t.Require().False(synced)
					t.Require().Equal(syncedCount, remain)
				}
			}
		}
	}
	t.checkLockSynced(l)
	t.checkLockNoDone(l)

	// CASE: need to drop more than one DDL to reach the desired schema (schema become smaller).
	// drop two columns for one table.
	info = newInfoWithVersion(task, sources[0], dbs[0], tbls[0], downSchema, downTable, DDLs4, ti3, []*model.TableInfo{ti4_1, ti4}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	t.Require().NoError(err)
	t.Require().Equal([]string{}, DDLs)
	t.Require().Equal([]string{"c2", "c1"}, cols)
	t.Require().Equal(vers, l.versions)
	ready = l.Ready()
	t.Require().False(ready[sources[0]][dbs[0]][tbls[0]])
	t.Require().True(ready[sources[0]][dbs[0]][tbls[1]])

	// TrySync again is idempotent.
	info = newInfoWithVersion(task, sources[0], dbs[0], tbls[0], downSchema, downTable, DDLs4, ti3, []*model.TableInfo{ti4_1, ti4}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	t.Require().NoError(err)
	t.Require().Equal([]string{}, DDLs)
	t.Require().Equal([]string{"c2", "c1"}, cols)
	t.Require().Equal(vers, l.versions)
	ready = l.Ready()
	t.Require().False(ready[sources[0]][dbs[0]][tbls[0]])
	t.Require().True(ready[sources[0]][dbs[0]][tbls[1]])

	// drop only the first column for another table.
	info = newInfoWithVersion(task, sources[0], dbs[0], tbls[1], downSchema, downTable, DDLs4[0:1], ti3, []*model.TableInfo{ti4_1}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	t.Require().NoError(err)
	t.Require().Equal([]string{}, DDLs)
	t.Require().Equal([]string{"c2"}, cols)
	t.Require().Equal(vers, l.versions)
	ready = l.Ready()
	t.Require().False(ready[sources[0]][dbs[0]][tbls[0]])
	t.Require().False(ready[sources[0]][dbs[0]][tbls[1]])
	cmp, err = l.tables[sources[0]][dbs[0]][tbls[0]].Compare(l.tables[sources[0]][dbs[0]][tbls[1]])
	t.Require().NoError(err)
	t.Require().Equal(-1, cmp)

	// TrySync again (only the first DDL).
	info = newInfoWithVersion(task, sources[0], dbs[0], tbls[1], downSchema, downTable, DDLs4[0:1], ti3, []*model.TableInfo{ti4_1}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	t.Require().NoError(err)
	t.Require().Equal([]string{}, DDLs)
	t.Require().Equal([]string{"c2"}, cols)
	t.Require().Equal(vers, l.versions)

	// drop the second column for another table.
	info = newInfoWithVersion(task, sources[0], dbs[0], tbls[1], downSchema, downTable, DDLs4[1:2], ti4_1, []*model.TableInfo{ti4}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	t.Require().NoError(err)
	t.Require().Equal([]string{}, DDLs)
	t.Require().Equal([]string{"c1"}, cols)
	t.Require().Equal(vers, l.versions)
	ready = l.Ready()
	t.Require().False(ready[sources[0]][dbs[0]][tbls[0]])
	t.Require().False(ready[sources[0]][dbs[0]][tbls[1]])
	cmp, err = l.tables[sources[0]][dbs[0]][tbls[0]].Compare(l.tables[sources[0]][dbs[0]][tbls[1]])
	t.Require().NoError(err)
	t.Require().Equal(0, cmp)

	// TrySync again (for the second DDL).
	info = newInfoWithVersion(task, sources[0], dbs[0], tbls[1], downSchema, downTable, DDLs4[1:2], ti4_1, []*model.TableInfo{ti4}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	t.Require().NoError(err)
	t.Require().Equal([]string{}, DDLs)
	t.Require().Equal([]string{"c1"}, cols)
	t.Require().Equal(vers, l.versions)

	// try drop columns for other tables to reach the same schema.
	remain = tableCount - 2
	for source, schemaTables := range l.Ready() {
		for schema, tables := range schemaTables {
			for table, synced2 := range tables {
				if synced2 { // do not `TrySync` again for previous two (un-synced now).
					info = newInfoWithVersion(task, source, schema, table, downSchema, downTable, DDLs4, ti3, []*model.TableInfo{ti4_1, ti4}, vers)
					DDLs, cols, err = l.TrySync(info, tts)
					t.Require().NoError(err)
					t.Require().Equal([]string{"c2", "c1"}, cols)
					t.Require().Equal(vers, l.versions)
					remain--
					if remain == 0 {
						t.Require().Equal(DDLs4, DDLs)
					} else {
						t.Require().Equal([]string{}, DDLs)
					}
				}
			}
		}
	}
	t.checkLockSynced(l)
	t.checkLockNoDone(l)
}

func (t *testLock) TestLockTrySyncIndex() {
	// nolint:dupl
	var (
		ID               = "test_lock_try_sync_index-`foo`.`bar`"
		task             = "test_lock_try_sync_index"
		source           = "mysql-replica-1"
		downSchema       = "db"
		downTable        = "bar"
		db               = "db"
		tbls             = []string{"bar1", "bar2"}
		p                = parser.New()
		se               = mock.NewContext()
		tblID      int64 = 111
		DDLs1            = []string{"ALTER TABLE bar DROP INDEX idx_c1"}
		DDLs2            = []string{"ALTER TABLE bar ADD UNIQUE INDEX idx_c1(c1)"}
		ti0              = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT, UNIQUE INDEX idx_c1(c1))`)
		ti1              = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT)`)
		ti2              = ti0
		tables           = map[string]map[string]struct{}{
			db: {tbls[0]: struct{}{}, tbls[1]: struct{}{}},
		}
		tts = []TargetTable{
			newTargetTable(task, source, downSchema, downTable, tables),
		}

		l = NewLock(etcdTestCli, ID, task, downSchema, downTable, schemacmp.Encode(ti0), tts, nil)

		vers = map[string]map[string]map[string]int64{
			source: {
				db: {tbls[0]: 0, tbls[1]: 0},
			},
		}
	)

	// the initial status is synced.
	t.checkLockSynced(l)
	t.checkLockNoDone(l)

	// try sync for one table, `DROP INDEX` returned directly (to make schema become more compatible).
	// `DROP INDEX` is handled like `ADD COLUMN`.
	info := newInfoWithVersion(task, source, db, tbls[0], downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1}, vers)
	DDLs, cols, err := l.TrySync(info, tts)
	t.Require().NoError(err)
	t.Require().Equal(DDLs1, DDLs)
	t.Require().Equal([]string{}, cols)
	t.Require().Equal(vers, l.versions)
	synced, remain := l.IsSynced()
	t.Require().Equal(l.synced, synced)
	t.Require().False(synced)
	t.Require().Equal(1, remain)

	// try sync for another table, also got `DROP INDEX` now.
	info = newInfoWithVersion(task, source, db, tbls[1], downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	t.Require().NoError(err)
	t.Require().Equal(DDLs1, DDLs)
	t.Require().Equal([]string{}, cols)
	t.Require().Equal(vers, l.versions)
	t.checkLockSynced(l)

	// try sync for one table, `ADD INDEX` not returned directly (to keep the schema more compatible).
	// `ADD INDEX` is handled like `DROP COLUMN`.
	info = newInfoWithVersion(task, source, db, tbls[0], downSchema, downTable, DDLs2, ti1, []*model.TableInfo{ti2}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	t.Require().NoError(err)
	t.Require().Equal([]string{}, DDLs) // no DDLs returned
	t.Require().Equal([]string{}, cols)
	t.Require().Equal(vers, l.versions)
	synced, remain = l.IsSynced()
	t.Require().Equal(l.synced, synced)
	t.Require().False(synced)
	t.Require().Equal(1, remain)

	// try sync for another table, got `ADD INDEX` now.
	info = newInfoWithVersion(task, source, db, tbls[1], downSchema, downTable, DDLs2, ti1, []*model.TableInfo{ti2}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	t.Require().NoError(err)
	t.Require().Equal(DDLs2, DDLs)
	t.Require().Equal([]string{}, cols)
	t.Require().Equal(vers, l.versions)
	t.checkLockSynced(l)
}

func (t *testLock) TestLockTrySyncNullNotNull() {
	// nolint:dupl
	var (
		ID               = "test_lock_try_sync_null_not_null-`foo`.`bar`"
		task             = "test_lock_try_sync_null_not_null"
		source           = "mysql-replica-1"
		downSchema       = "db"
		downTable        = "bar"
		db               = "db"
		tbls             = []string{"bar1", "bar2"}
		p                = parser.New()
		se               = mock.NewContext()
		tblID      int64 = 111
		DDLs1            = []string{"ALTER TABLE bar MODIFY COLUMN c1 INT NOT NULL DEFAULT 1234"}
		DDLs2            = []string{"ALTER TABLE bar MODIFY COLUMN c1 INT NULL DEFAULT 1234"}
		ti0              = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT NULL DEFAULT 1234)`)
		ti1              = createTableInfo(t.T(), p, se, tblID,
			`CREATE TABLE bar (id INT PRIMARY KEY, c1 INT NOT NULL DEFAULT 1234)`)
		ti2    = ti0
		tables = map[string]map[string]struct{}{
			db: {tbls[0]: struct{}{}, tbls[1]: struct{}{}},
		}
		tts = []TargetTable{
			newTargetTable(task, source, downSchema, downTable, tables),
		}

		l = NewLock(etcdTestCli, ID, task, downSchema, downTable, schemacmp.Encode(ti0), tts, nil)

		vers = map[string]map[string]map[string]int64{
			source: {
				db: {tbls[0]: 0, tbls[1]: 0},
			},
		}
	)

	// the initial status is synced.
	t.checkLockSynced(l)
	t.checkLockNoDone(l)

	for i := 0; i < 2; i++ { // two round
		// try sync for one table, from `NULL` to `NOT NULL`, no DDLs returned.
		info := newInfoWithVersion(task, source, db, tbls[0], downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1}, vers)
		DDLs, cols, err := l.TrySync(info, tts)
		t.Require().NoError(err)
		t.Require().Equal([]string{}, DDLs)
		t.Require().Equal([]string{}, cols)
		t.Require().Equal(vers, l.versions)

		// try sync for another table, DDLs returned.
		info = newInfoWithVersion(task, source, db, tbls[1], downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1}, vers)
		DDLs, cols, err = l.TrySync(info, tts)
		t.Require().NoError(err)
		t.Require().Equal(DDLs1, DDLs)
		t.Require().Equal([]string{}, cols)
		t.Require().Equal(vers, l.versions)

		// try sync for one table, from `NOT NULL` to `NULL`, DDLs returned.
		info = newInfoWithVersion(task, source, db, tbls[0], downSchema, downTable, DDLs2, ti1, []*model.TableInfo{ti2}, vers)
		DDLs, cols, err = l.TrySync(info, tts)
		t.Require().NoError(err)
		t.Require().Equal(DDLs2, DDLs)
		t.Require().Equal([]string{}, cols)
		t.Require().Equal(vers, l.versions)

		// try sync for another table, from `NOT NULL` to `NULL`, DDLs, returned.
		info = newInfoWithVersion(task, source, db, tbls[1], downSchema, downTable, DDLs2, ti1, []*model.TableInfo{ti2}, vers)
		DDLs, cols, err = l.TrySync(info, tts)
		t.Require().NoError(err)
		t.Require().Equal(DDLs2, DDLs)
		t.Require().Equal([]string{}, cols)
		t.Require().Equal(vers, l.versions)
	}
}

func (t *testLock) TestLockTrySyncIntBigint() {
	var (
		ID               = "test_lock_try_sync_int_bigint-`foo`.`bar`"
		task             = "test_lock_try_sync_int_bigint"
		source           = "mysql-replica-1"
		downSchema       = "db"
		downTable        = "bar"
		db               = "db"
		tbls             = []string{"bar1", "bar2"}
		p                = parser.New()
		se               = mock.NewContext()
		tblID      int64 = 111
		DDLs1            = []string{"ALTER TABLE bar MODIFY COLUMN c1 BIGINT NOT NULL DEFAULT 1234"}
		ti0              = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT NOT NULL DEFAULT 1234)`)
		ti1              = createTableInfo(t.T(), p, se, tblID,
			`CREATE TABLE bar (id INT PRIMARY KEY, c1 BIGINT NOT NULL DEFAULT 1234)`)
		tables = map[string]map[string]struct{}{
			db: {tbls[0]: struct{}{}, tbls[1]: struct{}{}},
		}
		tts = []TargetTable{
			newTargetTable(task, source, downSchema, downTable, tables),
		}

		l = NewLock(etcdTestCli, ID, task, downSchema, downTable, schemacmp.Encode(ti0), tts, nil)

		vers = map[string]map[string]map[string]int64{
			source: {
				db: {tbls[0]: 0, tbls[1]: 0},
			},
		}
	)

	// the initial status is synced.
	t.checkLockSynced(l)
	t.checkLockNoDone(l)

	// try sync for one table, from `INT` to `BIGINT`, DDLs returned.
	info := newInfoWithVersion(task, source, db, tbls[0], downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1}, vers)
	DDLs, cols, err := l.TrySync(info, tts)
	t.Require().NoError(err)
	t.Require().Equal(DDLs1, DDLs)
	t.Require().Equal([]string{}, cols)
	t.Require().Equal(vers, l.versions)

	// try sync for another table, DDLs returned.
	info = newInfoWithVersion(task, source, db, tbls[1], downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	t.Require().NoError(err)
	t.Require().Equal(DDLs1, DDLs)
	t.Require().Equal([]string{}, cols)
	t.Require().Equal(vers, l.versions)
}

func (t *testLock) TestLockTrySyncNoDiff() {
	var (
		ID               = "test_lock_try_sync_no_diff-`foo`.`bar`"
		task             = "test_lock_try_sync_no_diff"
		source           = "mysql-replica-1"
		downSchema       = "db"
		downTable        = "bar"
		db               = "db"
		tbls             = []string{"bar1", "bar2"}
		p                = parser.New()
		se               = mock.NewContext()
		tblID      int64 = 111
		DDLs1            = []string{"ALTER TABLE bar RENAME c1 TO c2"}
		ti0              = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT)`)
		ti1              = createTableInfo(t.T(), p, se, tblID,
			`CREATE TABLE bar (id INT PRIMARY KEY, c2 INT)`) // `c1` dropped, `c2` added
		tables = map[string]map[string]struct{}{
			db: {tbls[0]: struct{}{}, tbls[1]: struct{}{}},
		}
		tts = []TargetTable{
			newTargetTable(task, source, downSchema, downTable, tables),
		}

		l = NewLock(etcdTestCli, ID, task, downSchema, downTable, schemacmp.Encode(ti0), tts, nil)

		vers = map[string]map[string]map[string]int64{
			source: {
				db: {tbls[0]: 0, tbls[1]: 0},
			},
		}
	)

	// the initial status is synced.
	t.checkLockSynced(l)
	t.checkLockNoDone(l)

	// try sync for one table.
	info := newInfoWithVersion(task, source, db, tbls[0], downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1}, vers)
	DDLs, cols, err := l.TrySync(info, tts)
	t.Require().True(terror.ErrShardDDLOptimismNeedSkipAndRedirect.Equal(err))
	t.Require().Equal([]string{}, DDLs)
	t.Require().Equal([]string{}, cols)
	t.Require().Equal(vers, l.versions)
}

func (t *testLock) TestLockTrySyncNewTable() {
	var (
		ID               = "test_lock_try_sync_new_table-`foo`.`bar`"
		task             = "test_lock_try_sync_new_table"
		source1          = "mysql-replica-1"
		source2          = "mysql-replica-2"
		downSchema       = "foo"
		downTable        = "bar"
		db1              = "foo1"
		db2              = "foo2"
		tbl1             = "bar1"
		tbl2             = "bar2"
		p                = parser.New()
		se               = mock.NewContext()
		tblID      int64 = 111
		DDLs1            = []string{"ALTER TABLE bar ADD COLUMN c1 INT"}
		ti0              = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY)`)
		ti1              = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT)`)

		tables = map[string]map[string]struct{}{db1: {tbl1: struct{}{}}}
		tts    = []TargetTable{newTargetTable(task, source1, downSchema, downTable, tables)}
		l      = NewLock(etcdTestCli, ID, task, downSchema, downTable, schemacmp.Encode(ti0), tts, nil)
		vers   = map[string]map[string]map[string]int64{
			source1: {
				db1: {tbl1: 0},
			},
			source2: {
				db2: {tbl2: 0},
			},
		}
	)

	// only one table exists before TrySync.
	t.checkLockSynced(l)
	t.checkLockNoDone(l)

	// TrySync for a new table as the caller.
	info := newInfoWithVersion(task, source2, db2, tbl2, downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1}, vers)
	DDLs, cols, err := l.TrySync(info, tts)
	t.Require().NoError(err)
	t.Require().Equal(DDLs1, DDLs)
	t.Require().Equal([]string{}, cols)
	t.Require().Equal(vers, l.versions)

	ready := l.Ready()
	t.Require().Len(ready, 2)
	t.Require().Len(ready[source1], 1)
	t.Require().Len(ready[source1][db1], 1)
	t.Require().False(ready[source1][db1][tbl1])
	t.Require().Len(ready[source2], 1)
	t.Require().Len(ready[source2][db2], 1)
	t.Require().True(ready[source2][db2][tbl2])

	// TrySync for two new tables as extra sources.
	// newly added work table use tableInfoBefore as table info
	tts = append(tts,
		newTargetTable(task, source1, downSchema, downTable, map[string]map[string]struct{}{db1: {tbl2: struct{}{}}}),
		newTargetTable(task, source2, downTable, downTable, map[string]map[string]struct{}{db2: {tbl1: struct{}{}}}),
	)
	vers[source1][db1][tbl2] = 0
	vers[source2][db2][tbl1] = 0

	info = newInfoWithVersion(task, source1, db1, tbl1, downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	t.Require().NoError(err)
	t.Require().Equal(DDLs1, DDLs)
	t.Require().Equal([]string{}, cols)
	t.Require().Equal(vers, l.versions)

	ready = l.Ready()
	t.Require().Len(ready, 2)
	t.Require().Len(ready[source1], 1)
	t.Require().Len(ready[source1][db1], 2)
	t.Require().True(ready[source1][db1][tbl1])
	t.Require().False(ready[source1][db1][tbl2]) // new table use ti0 as init table
	t.Require().Len(ready[source2], 1)
	t.Require().Len(ready[source2][db2], 2)
	t.Require().False(ready[source2][db2][tbl1])
	t.Require().True(ready[source2][db2][tbl2])

	info = newInfoWithVersion(task, source1, db1, tbl2, downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	t.Require().NoError(err)
	t.Require().Equal(DDLs1, DDLs)
	t.Require().Equal([]string{}, cols)
	t.Require().Equal(vers, l.versions)

	ready = l.Ready()
	t.Require().Len(ready, 2)
	t.Require().Len(ready[source1], 1)
	t.Require().Len(ready[source1][db1], 2)
	t.Require().True(ready[source1][db1][tbl1])
	t.Require().True(ready[source1][db1][tbl2])
	t.Require().Len(ready[source2], 1)
	t.Require().Len(ready[source2][db2], 2)
	t.Require().False(ready[source2][db2][tbl1])
	t.Require().True(ready[source2][db2][tbl2])
}

func (t *testLock) TestLockTrySyncRevert() {
	var (
		ID               = "test_lock_try_sync_revert-`foo`.`bar`"
		task             = "test_lock_try_sync_revert"
		source           = "mysql-replica-1"
		downSchema       = "foo"
		downTable        = "bar"
		db               = "foo"
		tbls             = []string{"bar1", "bar2"}
		p                = parser.New()
		se               = mock.NewContext()
		tblID      int64 = 111

		DDLs1 = []string{"ALTER TABLE bar ADD COLUMN c1 TEXT"}
		DDLs2 = []string{"ALTER TABLE bar DROP COLUMN c1"}
		ti0   = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY)`)
		ti1   = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 TEXT)`)
		ti2   = ti0

		DDLs3 = []string{"ALTER TABLE bar ADD COLUMN c1 TEXT", "ALTER TABLE bar ADD COLUMN c2 INT"}
		DDLs4 = []string{"ALTER TABLE bar DROP COLUMN c2"}
		DDLs5 = []string{"ALTER TABLE bar DROP COLUMN c1"}
		ti3   = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 TEXT, c2 INT)`)
		ti4   = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 TEXT)`)
		ti5   = ti0

		DDLs6 = DDLs3
		DDLs7 = DDLs4
		DDLs8 = []string{"ALTER TABLE bar ADD COLUMN c1 TEXT"}
		ti6   = ti3
		ti7   = ti4
		ti8   = ti4

		tables = map[string]map[string]struct{}{db: {tbls[0]: struct{}{}, tbls[1]: struct{}{}}}
		tts    = []TargetTable{newTargetTable(task, source, downSchema, downTable, tables)}
		l      = NewLock(etcdTestCli, ID, task, downSchema, downTable, schemacmp.Encode(ti0), tts, nil)

		vers = map[string]map[string]map[string]int64{
			source: {
				db: {tbls[0]: 0, tbls[1]: 0},
			},
		}
	)

	// the initial status is synced.
	t.checkLockSynced(l)
	t.checkLockNoDone(l)

	// CASE: revert for single DDL.
	// TrySync for one table.
	info := newInfoWithVersion(task, source, db, tbls[0], downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1}, vers)
	DDLs, cols, err := l.TrySync(info, tts)
	t.Require().NoError(err)
	t.Require().Equal(DDLs1, DDLs)
	t.Require().Equal([]string{}, cols)
	t.Require().Equal(vers, l.versions)
	ready := l.Ready()
	t.Require().True(ready[source][db][tbls[0]])
	t.Require().False(ready[source][db][tbls[1]])

	joined, err := l.Joined()
	t.Require().NoError(err)
	cmp, err := l.tables[source][db][tbls[0]].Compare(joined)
	t.Require().NoError(err)
	t.Require().Equal(0, cmp)
	cmp, err = l.tables[source][db][tbls[1]].Compare(joined)
	t.Require().NoError(err)
	t.Require().Equal(-1, cmp)

	// revert for the table, become synced again.
	info = newInfoWithVersion(task, source, db, tbls[0], downSchema, downTable, DDLs2, ti1, []*model.TableInfo{ti2}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	t.Require().NoError(err)
	t.Require().Equal(DDLs2, DDLs)
	t.Require().Equal([]string{"c1"}, cols)
	t.Require().Equal(vers, l.versions)
	t.checkLockSynced(l)
	t.checkLockNoDone(l)

	// Simulate watch done operation from dm-worker
	op := NewOperation(utils.GenDDLLockID(task, downSchema, downTable), task, source, db, tbls[0], DDLs2, ConflictNone, "", true, []string{"c1"})
	t.Require().NoError(l.DeleteColumnsByOp(op))

	// CASE: revert for multiple DDLs.
	// TrySync for one table.
	info = newInfoWithVersion(task, source, db, tbls[0], downSchema, downTable, DDLs3, ti0, []*model.TableInfo{ti4, ti3}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	t.Require().NoError(err)
	t.Require().Equal(DDLs3, DDLs)
	t.Require().Equal([]string{}, cols)
	t.Require().Equal(vers, l.versions)
	ready = l.Ready()
	t.Require().True(ready[source][db][tbls[0]])
	t.Require().False(ready[source][db][tbls[1]])
	joined, err = l.Joined()
	t.Require().NoError(err)
	cmp, err = l.tables[source][db][tbls[0]].Compare(joined)
	t.Require().NoError(err)
	t.Require().Equal(0, cmp)
	cmp, err = l.tables[source][db][tbls[1]].Compare(joined)
	t.Require().NoError(err)
	t.Require().Equal(-1, cmp)

	// revert part of the DDLs.
	info = newInfoWithVersion(task, source, db, tbls[0], downSchema, downTable, DDLs4, ti3, []*model.TableInfo{ti4}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	t.Require().NoError(err)
	t.Require().Equal(DDLs4, DDLs)
	t.Require().Equal([]string{"c2"}, cols)
	t.Require().Equal(vers, l.versions)
	ready = l.Ready()
	t.Require().False(ready[source][db][tbls[1]])
	joined, err = l.Joined()
	t.Require().NoError(err)
	cmp, err = l.tables[source][db][tbls[0]].Compare(joined)
	t.Require().NoError(err)
	t.Require().Equal(0, cmp)
	cmp, err = l.tables[source][db][tbls[1]].Compare(joined)
	t.Require().NoError(err)
	t.Require().Equal(-1, cmp)

	// revert the reset part of the DDLs.
	info = newInfoWithVersion(task, source, db, tbls[0], downSchema, downTable, DDLs5, ti4, []*model.TableInfo{ti5}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	t.Require().NoError(err)
	t.Require().Equal(DDLs5, DDLs)
	t.Require().Equal([]string{"c1"}, cols)
	t.Require().Equal(vers, l.versions)
	t.checkLockSynced(l)
	t.checkLockNoDone(l)

	// Simulate watch done operation from dm-worker
	op = NewOperation(utils.GenDDLLockID(task, downSchema, downTable), task, source, db, tbls[0], DDLs4, ConflictNone, "", true, []string{"c2"})
	t.Require().NoError(l.DeleteColumnsByOp(op))
	op = NewOperation(utils.GenDDLLockID(task, downSchema, downTable), task, source, db, tbls[0], DDLs5, ConflictNone, "", true, []string{"c1"})
	t.Require().NoError(l.DeleteColumnsByOp(op))

	// CASE: revert part of multiple DDLs.
	// TrySync for one table.
	info = newInfoWithVersion(task, source, db, tbls[0], downSchema, downTable, DDLs6, ti0, []*model.TableInfo{ti7, ti6}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	t.Require().NoError(err)
	t.Require().Equal(DDLs6, DDLs)
	t.Require().Equal([]string{}, cols)
	t.Require().Equal(vers, l.versions)
	ready = l.Ready()
	t.Require().False(ready[source][db][tbls[1]])

	joined, err = l.Joined()
	t.Require().NoError(err)
	cmp, err = l.tables[source][db][tbls[0]].Compare(joined)
	t.Require().NoError(err)
	t.Require().Equal(0, cmp)
	cmp, err = l.tables[source][db][tbls[1]].Compare(joined)
	t.Require().NoError(err)
	t.Require().Equal(-1, cmp)

	// revert part of the DDLs.
	info = newInfoWithVersion(task, source, db, tbls[0], downSchema, downTable, DDLs7, ti3, []*model.TableInfo{ti7}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	t.Require().NoError(err)
	t.Require().Equal(DDLs7, DDLs)
	t.Require().Equal([]string{"c2"}, cols)
	t.Require().Equal(vers, l.versions)
	ready = l.Ready()
	t.Require().False(ready[source][db][tbls[1]])

	joined, err = l.Joined()
	t.Require().NoError(err)
	cmp, err = l.tables[source][db][tbls[0]].Compare(joined)
	t.Require().NoError(err)
	t.Require().Equal(0, cmp)
	cmp, err = l.tables[source][db][tbls[1]].Compare(joined)
	t.Require().NoError(err)
	t.Require().Equal(-1, cmp)

	// TrySync for another table.
	info = newInfoWithVersion(task, source, db, tbls[1], downSchema, downTable, DDLs8, ti0, []*model.TableInfo{ti8}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	t.Require().NoError(err)
	t.Require().Equal(DDLs8, DDLs)
	t.Require().Equal([]string{}, cols)
	t.Require().Equal(vers, l.versions)
	t.checkLockSynced(l)
	t.checkLockNoDone(l)
}

func (t *testLock) TestLockTrySyncConflictNonIntrusive() {
	var (
		ID               = "test_lock_try_sync_conflict_non_intrusive-`foo`.`bar`"
		task             = "test_lock_try_sync_conflict_non_intrusive"
		source           = "mysql-replica-1"
		downSchema       = "foo"
		downTable        = "bar"
		db               = "foo"
		tbls             = []string{"bar1", "bar2"}
		p                = parser.New()
		se               = mock.NewContext()
		tblID      int64 = 111
		DDLs1            = []string{"ALTER TABLE bar ADD COLUMN c1 TEXT"}
		DDLs2            = []string{"ALTER TABLE bar ADD COLUMN c1 DATETIME", "ALTER TABLE bar ADD COLUMN c2 INT"}
		DDLs3            = []string{"ALTER TABLE bar DROP COLUMN c1"}
		DDLs4            = DDLs2
		ti0              = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY)`)
		ti1              = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 TEXT)`)
		ti2              = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 DATETIME, c2 INT)`)
		ti2_1            = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 DATETIME)`)
		ti3              = ti0
		ti4              = ti2
		ti4_1            = ti2_1

		tables = map[string]map[string]struct{}{db: {tbls[0]: struct{}{}, tbls[1]: struct{}{}}}
		tts    = []TargetTable{newTargetTable(task, source, downSchema, downTable, tables)}
		l      = NewLock(etcdTestCli, ID, task, downSchema, downTable, schemacmp.Encode(ti0), tts, nil)

		vers = map[string]map[string]map[string]int64{
			source: {
				db: {tbls[0]: 0, tbls[1]: 0},
			},
		}
	)

	// the initial status is synced.
	t.checkLockSynced(l)
	t.checkLockNoDone(l)

	// TrySync for the first table, construct the joined schema.
	info := newInfoWithVersion(task, source, db, tbls[0], downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1}, vers)
	DDLs, cols, err := l.TrySync(info, tts)
	t.Require().NoError(err)
	t.Require().Equal(DDLs1, DDLs)
	t.Require().Equal([]string{}, cols)
	t.Require().Equal(vers, l.versions)
	ready := l.Ready()
	t.Require().True(ready[source][db][tbls[0]])
	t.Require().False(ready[source][db][tbls[1]])
	joined, err := l.Joined()
	t.Require().NoError(err)
	cmp, err := l.tables[source][db][tbls[0]].Compare(joined)
	t.Require().NoError(err)
	t.Require().Equal(0, cmp)
	cmp, err = l.tables[source][db][tbls[1]].Compare(joined)
	t.Require().NoError(err)
	t.Require().Equal(-1, cmp)

	// TrySync for the second table with another schema (add two columns, one of them will cause conflict).
	info = newInfoWithVersion(task, source, db, tbls[1], downSchema, downTable, DDLs2, ti0, []*model.TableInfo{ti2_1, ti2}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	t.Require().True(terror.ErrShardDDLOptimismTrySyncFail.Equal(err))
	t.Require().Equal([]string{}, DDLs)
	t.Require().Equal([]string{}, cols)
	t.Require().Equal(vers, l.versions)
	joined, err = l.Joined()
	t.Require().NoError(err)
	cmp, err = l.tables[source][db][tbls[1]].Compare(joined)
	// join table isn't updated
	t.Require().NoError(err)
	t.Require().Equal(-1, cmp)
	ready = l.Ready()
	t.Require().False(ready[source][db][tbls[1]])

	// TrySync for the first table to resolve the conflict.
	info = newInfoWithVersion(task, source, db, tbls[0], downSchema, downTable, DDLs3, ti1, []*model.TableInfo{ti3}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	t.Require().NoError(err)
	t.Require().Equal(DDLs3, DDLs)
	t.Require().Equal([]string{"c1"}, cols)
	t.Require().Equal(vers, l.versions)
	ready = l.Ready() // all table ready
	t.Require().True(ready[source][db][tbls[0]])
	t.Require().True(ready[source][db][tbls[1]])
	joined, err = l.Joined()
	t.Require().NoError(err)
	cmp, err = l.tables[source][db][tbls[0]].Compare(joined)
	t.Require().NoError(err)
	t.Require().Equal(0, cmp)
	cmp, err = l.tables[source][db][tbls[1]].Compare(joined)
	t.Require().NoError(err)
	t.Require().Equal(0, cmp)

	// TrySync for the second table, succeed now
	info = newInfoWithVersion(task, source, db, tbls[1], downSchema, downTable, DDLs2, ti0, []*model.TableInfo{ti2_1, ti2}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	t.Require().NoError(err)
	t.Require().Equal(DDLs2, DDLs)
	t.Require().Equal([]string{}, cols)
	t.Require().Equal(vers, l.versions)
	joined, err = l.Joined()
	t.Require().NoError(err)
	cmp, err = l.tables[source][db][tbls[1]].Compare(joined)
	t.Require().NoError(err)
	t.Require().Equal(0, cmp)
	ready = l.Ready()
	t.Require().True(ready[source][db][tbls[1]])

	// Simulate watch done operation from dm-worker
	op := NewOperation(utils.GenDDLLockID(task, downSchema, downTable), task, source, db, tbls[0], DDLs3, ConflictNone, "", true, []string{"c1"})
	t.Require().NoError(l.DeleteColumnsByOp(op))

	// TrySync for the first table.
	info = newInfoWithVersion(task, source, db, tbls[0], downSchema, downTable, DDLs4, ti0, []*model.TableInfo{ti4_1, ti4}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	t.Require().NoError(err)
	t.Require().Equal(DDLs4, DDLs)
	t.Require().Equal([]string{}, cols)
	t.Require().Equal(vers, l.versions)
	t.checkLockSynced(l)
	t.checkLockNoDone(l)
}

func (t *testLock) TestLockTrySyncConflictIntrusive() {
	var (
		ID               = "test_lock_try_sync_conflict_intrusive-`foo`.`bar`"
		task             = "test_lock_try_sync_conflict_intrusive"
		source           = "mysql-replica-1"
		downSchema       = "foo"
		downTable        = "bar"
		db               = "foo"
		tbls             = []string{"bar1", "bar2"}
		p                = parser.New()
		se               = mock.NewContext()
		tblID      int64 = 111
		DDLs1            = []string{"ALTER TABLE bar ADD COLUMN c1 TEXT"}
		DDLs2            = []string{"ALTER TABLE bar ADD COLUMN c1 DATETIME", "ALTER TABLE bar ADD COLUMN c2 INT"}
		DDLs3            = []string{"ALTER TABLE bar ADD COLUMN c1 DATETIME"}
		ti0              = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY)`)
		ti1              = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 TEXT)`)
		ti2              = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 DATETIME, c2 INT)`)
		ti3              = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 DATETIME)`)

		DDLs5   = []string{"ALTER TABLE bar ADD COLUMN c2 TEXT"}
		DDLs6   = []string{"ALTER TABLE bar ADD COLUMN c2 DATETIME", "ALTER TABLE bar ADD COLUMN c3 INT"}
		DDLs7   = []string{"ALTER TABLE bar ADD COLUMN c3 INT"}
		DDLs8_1 = DDLs7
		DDLs8_2 = DDLs5
		ti5     = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 TEXT, c2 TEXT)`)
		ti6     = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 TEXT, c2 DATETIME, c3 INT)`)
		ti6_1   = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 TEXT, c2 DATETIME)`)
		ti7     = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 TEXT, c3 INT)`)
		ti8     = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 TEXT, c2 TEXT, c3 INT)`)

		tables = map[string]map[string]struct{}{db: {tbls[0]: struct{}{}, tbls[1]: struct{}{}}}
		tts    = []TargetTable{newTargetTable(task, source, downSchema, downTable, tables)}
		l      = NewLock(etcdTestCli, ID, task, downSchema, downTable, schemacmp.Encode(ti0), tts, nil)

		vers = map[string]map[string]map[string]int64{
			source: {
				db: {tbls[0]: 0, tbls[1]: 0},
			},
		}
	)

	// the initial status is synced.
	t.checkLockSynced(l)
	t.checkLockNoDone(l)

	// CASE: conflict happen, revert all changes to resolve the conflict.
	// TrySync for the first table, construct the joined schema.
	info := newInfoWithVersion(task, source, db, tbls[0], downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1}, vers)
	DDLs, cols, err := l.TrySync(info, tts)
	t.Require().NoError(err)
	t.Require().Equal(DDLs1, DDLs)
	t.Require().Equal([]string{}, cols)
	t.Require().Equal(vers, l.versions)
	ready := l.Ready()
	t.Require().True(ready[source][db][tbls[0]])
	t.Require().False(ready[source][db][tbls[1]])
	joined, err := l.Joined()
	t.Require().NoError(err)
	cmp, err := l.tables[source][db][tbls[0]].Compare(joined)
	t.Require().NoError(err)
	t.Require().Equal(0, cmp)
	cmp, err = l.tables[source][db][tbls[1]].Compare(joined)
	t.Require().NoError(err)
	t.Require().Equal(-1, cmp)

	// TrySync for the second table with another schema (add two columns, one of them will cause conflict).
	info = newInfoWithVersion(task, source, db, tbls[1], downSchema, downTable, DDLs2, ti0, []*model.TableInfo{ti3, ti2}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	t.Require().True(terror.ErrShardDDLOptimismTrySyncFail.Equal(err))
	t.Require().Equal([]string{}, DDLs)
	t.Require().Equal([]string{}, cols)
	t.Require().Equal(vers, l.versions)
	joined, err = l.Joined()
	t.Require().NoError(err)
	cmp, err = l.tables[source][db][tbls[1]].Compare(joined)
	// join table isn't updated
	t.Require().NoError(err)
	t.Require().Equal(-1, cmp)
	ready = l.Ready()
	t.Require().False(ready[source][db][tbls[1]])

	// TrySync again.
	info = newInfoWithVersion(task, source, db, tbls[1], downSchema, downTable, DDLs2, ti0, []*model.TableInfo{ti3, ti2}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	t.Require().True(terror.ErrShardDDLOptimismTrySyncFail.Equal(err))
	t.Require().Equal([]string{}, DDLs)
	t.Require().Equal([]string{}, cols)
	t.Require().Equal(vers, l.versions)
	joined, err = l.Joined()
	t.Require().NoError(err)
	cmp, err = l.tables[source][db][tbls[1]].Compare(joined)
	t.Require().NoError(err)
	t.Require().Equal(-1, cmp)

	// TrySync for the second table to replace a new ddl without non-conflict column, the conflict should still exist.
	info = newInfoWithVersion(task, source, db, tbls[1], downSchema, downTable, DDLs3, ti0, []*model.TableInfo{ti3}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	t.Require().True(terror.ErrShardDDLOptimismTrySyncFail.Equal(err))
	t.Require().Equal([]string{}, DDLs)
	t.Require().Equal([]string{}, cols)
	t.Require().Equal(vers, l.versions)
	joined, err = l.Joined()
	t.Require().NoError(err)
	cmp, err = l.tables[source][db][tbls[1]].Compare(joined)
	t.Require().NoError(err)
	t.Require().Equal(-1, cmp)
	ready = l.Ready()
	t.Require().False(ready[source][db][tbls[1]])

	// TrySync for the second table as we did for the first table, the lock should be synced.
	info = newInfoWithVersion(task, source, db, tbls[1], downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	t.Require().NoError(err)
	t.Require().Equal(DDLs, DDLs)
	t.Require().Equal([]string{}, cols)
	t.Require().Equal(vers, l.versions)
	joined, err = l.Joined()
	t.Require().NoError(err)
	cmp, err = l.tables[source][db][tbls[1]].Compare(joined)
	t.Require().NoError(err)
	t.Require().Equal(0, cmp)
	t.checkLockSynced(l)
	t.checkLockNoDone(l)

	// CASE: conflict happen, revert part of changes to resolve the conflict.
	// TrySync for the first table, construct the joined schema.
	info = newInfoWithVersion(task, source, db, tbls[0], downSchema, downTable, DDLs5, ti1, []*model.TableInfo{ti5}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	t.Require().NoError(err)
	t.Require().Equal(DDLs5, DDLs)
	t.Require().Equal([]string{}, cols)
	t.Require().Equal(vers, l.versions)
	ready = l.Ready()
	t.Require().True(ready[source][db][tbls[0]])
	t.Require().False(ready[source][db][tbls[1]])
	joined, err = l.Joined()
	t.Require().NoError(err)
	cmp, err = l.tables[source][db][tbls[0]].Compare(joined)
	t.Require().NoError(err)
	t.Require().Equal(0, cmp)
	cmp, err = l.tables[source][db][tbls[1]].Compare(joined)
	t.Require().NoError(err)
	t.Require().Equal(-1, cmp)

	// TrySync for the second table with another schema (add two columns, one of them will cause conflict).
	info = newInfoWithVersion(task, source, db, tbls[1], downSchema, downTable, DDLs6, ti1, []*model.TableInfo{ti6_1, ti6}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	t.Require().True(terror.ErrShardDDLOptimismTrySyncFail.Equal(err))
	t.Require().Equal([]string{}, DDLs)
	t.Require().Equal([]string{}, cols)
	joined, err = l.Joined()
	t.Require().NoError(err)
	cmp, err = l.tables[source][db][tbls[1]].Compare(joined)
	t.Require().NoError(err)
	t.Require().Equal(-1, cmp)
	t.Require().Equal(vers, l.versions)
	ready = l.Ready()
	t.Require().False(ready[source][db][tbls[1]])

	// TrySync for the second table to replace a new ddl without conflict column, the conflict should be resolved.
	// but both of tables are not synced now.
	info = newInfoWithVersion(task, source, db, tbls[1], downSchema, downTable, DDLs7, ti1, []*model.TableInfo{ti7}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	t.Require().NoError(err)
	t.Require().Equal(DDLs7, DDLs)
	t.Require().Equal([]string{}, cols)
	t.Require().Equal(vers, l.versions)
	ready = l.Ready()
	t.Require().False(ready[source][db][tbls[0]])
	t.Require().False(ready[source][db][tbls[1]])
	joined, err = l.Joined()
	t.Require().NoError(err)
	cmp, err = l.tables[source][db][tbls[0]].Compare(joined)
	t.Require().NoError(err)
	t.Require().Equal(-1, cmp)
	cmp, err = l.tables[source][db][tbls[1]].Compare(joined)
	t.Require().NoError(err)
	t.Require().Equal(-1, cmp)

	// TrySync for the first table to become synced.
	info = newInfoWithVersion(task, source, db, tbls[0], downSchema, downTable, DDLs8_1, ti5, []*model.TableInfo{ti8}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	t.Require().NoError(err)
	t.Require().Equal(DDLs8_1, DDLs)
	t.Require().Equal([]string{}, cols)
	ready = l.Ready()
	t.Require().True(ready[source][db][tbls[0]])

	// TrySync for the second table to become synced.
	info = newInfoWithVersion(task, source, db, tbls[1], downSchema, downTable, DDLs8_2, ti7, []*model.TableInfo{ti8}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	t.Require().NoError(err)
	t.Require().Equal(DDLs8_2, DDLs)
	t.Require().Equal([]string{}, cols)
	ready = l.Ready()
	t.Require().True(ready[source][db][tbls[1]])

	// all tables synced now.
	t.checkLockSynced(l)
	t.checkLockNoDone(l)
}

func (t *testLock) TestLockTrySyncMultipleChangeDDL() {
	var (
		ID               = "test_lock_try_sync_normal-`foo`.`bar`"
		task             = "test_lock_try_sync_normal"
		sources          = []string{"mysql-replica-1", "mysql-replica-2"}
		downSchema       = "db"
		downTable        = "bar"
		dbs              = []string{"db1", "db2"}
		tbls             = []string{"bar1", "bar2"}
		tableCount       = len(sources) * len(dbs) * len(tbls)
		p                = parser.New()
		se               = mock.NewContext()
		tblID      int64 = 111
		DDLs1            = []string{"ALTER TABLE bar ADD COLUMN c2 INT", "ALTER TABLE bar DROP COLUMN c1"}
		DDLs2            = []string{"ALTER TABLE bar DROP COLUMN c2", "ALTER TABLE bar ADD COLUMN c3 TEXT"}
		//		DDLs3            = []string{"ALTER TABLE bar DROP COLUMN c3"}
		//		DDLs4            = []string{"ALTER TABLE bar DROP COLUMN c2", "ALTER TABLE bar DROP COLUMN c1"}
		ti0   = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT)`)
		ti1_1 = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT, c2 INT)`)
		ti1   = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c2 INT)`)
		ti2   = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c3 TEXT)`)
		ti2_1 = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY)`)
		//		ti3              = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT, c2 BIGINT)`)
		//		ti4              = ti0
		//		ti4_1            = ti1
		tables = map[string]map[string]struct{}{
			dbs[0]: {tbls[0]: struct{}{}, tbls[1]: struct{}{}},
			dbs[1]: {tbls[0]: struct{}{}, tbls[1]: struct{}{}},
		}
		tts = []TargetTable{
			newTargetTable(task, sources[0], downSchema, downTable, tables),
			newTargetTable(task, sources[1], downSchema, downTable, tables),
		}

		l = NewLock(etcdTestCli, ID, task, downSchema, downTable, schemacmp.Encode(ti0), tts, nil)

		vers = map[string]map[string]map[string]int64{
			sources[0]: {
				dbs[0]: {tbls[0]: 0, tbls[1]: 0},
				dbs[1]: {tbls[0]: 0, tbls[1]: 0},
			},
			sources[1]: {
				dbs[0]: {tbls[0]: 0, tbls[1]: 0},
				dbs[1]: {tbls[0]: 0, tbls[1]: 0},
			},
		}
	)

	// the initial status is synced.
	t.checkLockSynced(l)
	t.checkLockNoDone(l)

	// inconsistent ddls and table infos
	info := newInfoWithVersion(task, sources[0], dbs[0], tbls[0], downSchema, downTable, DDLs1[:1], ti0, []*model.TableInfo{ti1_1, ti1}, vers)
	DDLs, cols, err := l.TrySync(info, tts)
	t.Require().Equal([]string{}, DDLs)
	t.Require().Equal([]string{}, cols)
	t.Require().True(terror.ErrMasterInconsistentOptimisticDDLsAndInfo.Equal(err))

	info = newInfoWithVersion(task, sources[0], dbs[0], tbls[0], downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	t.Require().Equal([]string{}, DDLs)
	t.Require().Equal([]string{}, cols)
	t.Require().True(terror.ErrMasterInconsistentOptimisticDDLsAndInfo.Equal(err))

	t.checkLockSynced(l)
	t.checkLockNoDone(l)

	// CASE: all tables execute a same multiple change DDLs1
	syncedCount := 0
	resultDDLs1 := map[string]map[string]map[string][]string{
		sources[0]: {
			dbs[0]: {tbls[0]: DDLs1[:1], tbls[1]: DDLs1[:1]},
			dbs[1]: {tbls[0]: DDLs1[:1], tbls[1]: DDLs1[:1]},
		},
		sources[1]: {
			dbs[0]: {tbls[0]: DDLs1[:1], tbls[1]: DDLs1[:1]},
			dbs[1]: {tbls[0]: DDLs1[:1], tbls[1]: DDLs1}, // only last table sync DROP COLUMN
		},
	}
	for _, source := range sources {
		for _, db := range dbs {
			for _, tbl := range tbls {
				info = newInfoWithVersion(task, source, db, tbl, downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1_1, ti1}, vers)
				DDLs, cols, err = l.TrySync(info, tts)
				t.Require().NoError(err)
				t.Require().Equal(resultDDLs1[source][db][tbl], DDLs)
				t.Require().Equal([]string{"c1"}, cols)
				t.Require().Equal(vers, l.versions)

				syncedCount++
				synced, _ := l.IsSynced()
				t.Require().Equal(syncedCount == tableCount, synced)
				t.Require().Equal(l.synced, synced)
			}
		}
	}
	// synced again after all tables applied the DDL.
	t.checkLockSynced(l)
	t.checkLockNoDone(l)

	// CASE: TrySync again after synced is idempotent.
	// both ddl will sync again
	info = newInfoWithVersion(task, sources[0], dbs[0], tbls[0], downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1_1, ti1}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	t.Require().NoError(err)
	t.Require().Equal(DDLs1, DDLs)
	t.Require().Equal([]string{"c1"}, cols)
	t.Require().Equal(vers, l.versions)
	t.checkLockSynced(l)
	t.checkLockNoDone(l)

	// CASE: all tables execute a same multiple change DDLs2
	syncedCount = 0
	resultDDLs2 := map[string]map[string]map[string][]string{
		sources[0]: {
			dbs[0]: {tbls[0]: DDLs2[1:], tbls[1]: DDLs2[1:]},
			dbs[1]: {tbls[0]: DDLs2[1:], tbls[1]: DDLs2[1:]},
		},
		sources[1]: {
			dbs[0]: {tbls[0]: DDLs2[1:], tbls[1]: DDLs2[1:]},
			dbs[1]: {tbls[0]: DDLs2[1:], tbls[1]: DDLs2}, // only last table sync DROP COLUMN
		},
	}
	for _, source := range sources {
		for _, db := range dbs {
			for _, tbl := range tbls {
				info = newInfoWithVersion(task, source, db, tbl, downSchema, downTable, DDLs2, ti1, []*model.TableInfo{ti2_1, ti2}, vers)
				DDLs, cols, err = l.TrySync(info, tts)
				t.Require().NoError(err)
				t.Require().Equal(resultDDLs2[source][db][tbl], DDLs)
				t.Require().Equal([]string{"c2"}, cols)
				t.Require().Equal(vers, l.versions)

				syncedCount++
				synced, _ := l.IsSynced()
				t.Require().Equal(syncedCount == tableCount, synced)
				t.Require().Equal(l.synced, synced)
			}
		}
	}
	// synced again after all tables applied the DDL.
	t.checkLockSynced(l)
	t.checkLockNoDone(l)

	// CASE: TrySync again after synced is idempotent.
	info = newInfoWithVersion(task, sources[0], dbs[0], tbls[0], downSchema, downTable, DDLs2, ti1, []*model.TableInfo{ti2_1, ti2}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	t.Require().NoError(err)
	t.Require().Equal(DDLs2, DDLs)
	t.Require().Equal([]string{"c2"}, cols)
	t.Require().Equal(vers, l.versions)
	t.checkLockSynced(l)
	t.checkLockNoDone(l)
}

func (t *testLock) TestTryRemoveTable() {
	var (
		ID               = "test_lock_try_remove_table-`foo`.`bar`"
		task             = "test_lock_try_remove_table"
		source           = "mysql-replica-1"
		downSchema       = "foo"
		downTable        = "bar"
		db               = "foo"
		tbl1             = "bar1"
		tbl2             = "bar2"
		p                = parser.New()
		se               = mock.NewContext()
		tblID      int64 = 111
		DDLs1            = []string{"ALTER TABLE bar ADD COLUMN c1 INT"}
		DDLs2            = []string{"ALTER TABLE bar ADD COLUMN c2 INT"}
		ti0              = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY)`)
		ti1              = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT)`)
		ti2              = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT, c2 INT)`)

		tables = map[string]map[string]struct{}{db: {tbl1: struct{}{}, tbl2: struct{}{}}}
		tts    = []TargetTable{newTargetTable(task, source, downSchema, downTable, tables)}
		l      = NewLock(etcdTestCli, ID, task, downSchema, downTable, schemacmp.Encode(ti0), tts, nil)

		vers = map[string]map[string]map[string]int64{
			source: {
				db: {tbl1: 0, tbl2: 0},
			},
		}
	)

	// only one table exists before TrySync.
	t.checkLockSynced(l)
	t.checkLockNoDone(l)

	// CASE: remove a table as normal.
	// TrySync for the first table.
	info := newInfoWithVersion(task, source, db, tbl1, downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1}, vers)
	DDLs, cols, err := l.TrySync(info, tts)
	t.Require().NoError(err)
	t.Require().Equal(DDLs1, DDLs)
	t.Require().Equal([]string{}, cols)
	t.Require().Equal(vers, l.versions)
	ready := l.Ready()
	t.Require().Len(ready, 1)
	t.Require().Len(ready[source], 1)
	t.Require().Len(ready[source][db], 2)
	t.Require().True(ready[source][db][tbl1])
	t.Require().False(ready[source][db][tbl2])

	// TryRemoveTable for the second table.
	l.columns = map[string]map[string]map[string]map[string]DropColumnStage{
		"col": {
			source: {db: {tbl2: DropNotDone}},
		},
	}
	col := l.TryRemoveTable(source, db, tbl2)
	t.Require().Equal([]string{"col"}, col)
	delete(vers[source][db], tbl2)
	ready = l.Ready()
	t.Require().Len(ready, 1)
	t.Require().Len(ready[source], 1)
	t.Require().Len(ready[source][db], 1)
	t.Require().True(ready[source][db][tbl1])
	t.Require().Equal(vers, l.versions)

	// CASE: remove a table will rebuild joined schema now.
	// TrySync to add the second back.
	vers[source][db][tbl2] = 0
	info = newInfoWithVersion(task, source, db, tbl2, downSchema, downTable, DDLs2, ti1, []*model.TableInfo{ti2}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	t.Require().NoError(err)
	t.Require().Equal(DDLs2, DDLs)
	t.Require().Equal([]string{}, cols)
	t.Require().Equal(vers, l.versions)
	ready = l.Ready()
	t.Require().Len(ready, 1)
	t.Require().Len(ready[source], 1)
	t.Require().Len(ready[source][db], 2)
	t.Require().False(ready[source][db][tbl1])
	t.Require().True(ready[source][db][tbl2])

	// TryRemoveTable for the second table.
	t.Require().Len(l.TryRemoveTable(source, db, tbl2), 0)
	delete(vers[source][db], tbl2)
	ready = l.Ready()
	t.Require().Len(ready, 1)
	t.Require().Len(ready[source], 1)
	t.Require().Len(ready[source][db], 1)
	t.Require().True(ready[source][db][tbl1]) // the joined schema is rebuild.
	t.Require().Equal(vers, l.versions)

	// CASE: try to remove for not-exists table.
	t.Require().Len(l.TryRemoveTable(source, db, "not-exist"), 0)
	t.Require().Len(l.TryRemoveTable(source, "not-exist", tbl1), 0)
	t.Require().Len(l.TryRemoveTable("not-exist", db, tbl1), 0)
}

func (t *testLock) TestTryRemoveTableWithSources() {
	var (
		ID               = "test_lock_try_remove_table-`foo`.`bar`"
		task             = "test_lock_try_remove_table"
		source1          = "mysql-replica-1"
		source2          = "mysql-replica-2"
		downSchema       = "foo"
		downTable        = "bar"
		db               = "foo"
		tbl1             = "bar1"
		tbl2             = "bar2"
		p                = parser.New()
		se               = mock.NewContext()
		tblID      int64 = 111
		DDLs1            = []string{"ALTER TABLE bar DROP COLUMN c1"}
		ti0              = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT)`)
		ti1              = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY)`)

		tables = map[string]map[string]struct{}{db: {tbl1: struct{}{}, tbl2: struct{}{}}}
		tts    = []TargetTable{newTargetTable(task, source1, downSchema, downTable, tables), newTargetTable(task, source2, downSchema, downTable, tables)}
		l      = NewLock(etcdTestCli, ID, task, downSchema, downTable, schemacmp.Encode(ti0), tts, nil)

		vers = map[string]map[string]map[string]int64{
			source1: {
				db: {tbl1: 0, tbl2: 0},
			},
			source2: {
				db: {tbl1: 0, tbl2: 0},
			},
		}
	)

	// only one table exists before TrySync.
	t.checkLockSynced(l)
	t.checkLockNoDone(l)

	// TrySync for the first table.
	info := newInfoWithVersion(task, source1, db, tbl1, downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1}, vers)
	DDLs, cols, err := l.TrySync(info, tts)
	t.Require().NoError(err)
	t.Require().Equal([]string{}, DDLs)
	t.Require().Equal([]string{"c1"}, cols)
	t.Require().Equal(vers, l.versions)
	ready := l.Ready()
	t.Require().Len(ready, 2)
	t.Require().Len(ready[source1], 1)
	t.Require().Len(ready[source1][db], 2)
	t.Require().False(ready[source1][db][tbl1])
	t.Require().True(ready[source1][db][tbl2])
	t.Require().Len(ready[source2], 1)
	t.Require().Len(ready[source2][db], 2)
	t.Require().True(ready[source2][db][tbl1])
	t.Require().True(ready[source2][db][tbl2])

	// TryRemoveTableBySources with nil
	t.Require().Equal(0, len(l.TryRemoveTableBySources(nil)))
	ready = l.Ready()
	t.Require().Len(ready, 2)

	// TryRemoveTableBySources with wrong source
	tts = tts[:1]
	t.Require().Equal(0, len(l.TryRemoveTableBySources([]string{"hahaha"})))
	ready = l.Ready()
	t.Require().Len(ready, 2)

	// TryRemoveTableBySources with source2
	t.Require().Equal(0, len(l.TryRemoveTableBySources([]string{source2})))
	ready = l.Ready()
	t.Require().Len(ready, 1)
	t.Require().Len(ready[source1], 1)
	t.Require().Len(ready[source1][db], 2)
	t.Require().False(ready[source1][db][tbl1])
	t.Require().True(ready[source1][db][tbl2])
	delete(vers, source2)
	t.Require().Equal(vers, l.versions)
	t.Require().True(l.HasTables())

	// TrySync with second table
	info = newInfoWithVersion(task, source1, db, tbl2, downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	t.Require().NoError(err)
	t.Require().Equal(DDLs1, DDLs)
	t.Require().Equal([]string{"c1"}, cols)
	t.Require().Equal(vers, l.versions)
	ready = l.Ready()
	t.Require().Len(ready, 1)
	t.Require().Len(ready[source1], 1)
	t.Require().Len(ready[source1][db], 2)
	t.Require().True(ready[source1][db][tbl1])
	t.Require().True(ready[source1][db][tbl2])

	// TryRemoveTableBySources with source1,source2
	cols = l.TryRemoveTableBySources([]string{source1})
	t.Require().Equal([]string{"c1"}, cols)
	t.Require().False(l.HasTables())
}

func (t *testLock) TestLockTryMarkDone() {
	var (
		ID               = "test_lock_try_mark_done-`foo`.`bar`"
		task             = "test_lock_try_mark_done"
		source           = "mysql-replica-1"
		downSchema       = "foo"
		downTable        = "bar"
		db               = "foo"
		tbls             = []string{"bar1", "bar2"}
		p                = parser.New()
		se               = mock.NewContext()
		tblID      int64 = 111
		DDLs1            = []string{"ALTER TABLE bar ADD COLUMN c1 INT"}
		DDLs2            = []string{"ALTER TABLE bar ADD COLUMN c1 INT", "ALTER TABLE bar ADD COLUMN c2 INT"}
		DDLs3            = []string{"ALTER TABLE bar ADD COLUMN c2 INT"}
		ti0              = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY)`)
		ti1              = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT)`)
		ti2              = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT, c2 INT)`)
		ti3              = ti2

		tables = map[string]map[string]struct{}{db: {tbls[0]: struct{}{}, tbls[1]: struct{}{}}}
		tts    = []TargetTable{newTargetTable(task, source, downSchema, downTable, tables)}
		l      = NewLock(etcdTestCli, ID, task, downSchema, downTable, schemacmp.Encode(ti0), tts, nil)

		vers = map[string]map[string]map[string]int64{
			source: {
				db: {tbls[0]: 0, tbls[1]: 0},
			},
		}
	)

	// the initial status is synced but not resolved.
	t.checkLockSynced(l)
	t.checkLockNoDone(l)
	t.Require().False(l.IsResolved())

	// TrySync for the first table, no table has done the DDLs operation.
	info := newInfoWithVersion(task, source, db, tbls[0], downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1}, vers)
	DDLs, cols, err := l.TrySync(info, tts)
	t.Require().NoError(err)
	t.Require().Equal(DDLs1, DDLs)
	t.Require().Equal([]string{}, cols)
	t.Require().Equal(vers, l.versions)
	t.checkLockNoDone(l)
	t.Require().False(l.IsResolved())

	// mark done for the synced table, the lock is un-resolved.
	t.Require().True(l.TryMarkDone(source, db, tbls[0]))
	t.Require().True(l.IsDone(source, db, tbls[0]))
	t.Require().False(l.IsDone(source, db, tbls[1]))
	t.Require().False(l.IsResolved())

	// TrySync for the second table, the joined schema become larger.
	info = newInfoWithVersion(task, source, db, tbls[1], downSchema, downTable, DDLs2, ti0, []*model.TableInfo{ti1, ti2}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	t.Require().NoError(err)
	t.Require().Equal(DDLs2, DDLs)
	t.Require().Equal([]string{}, cols)
	t.Require().Equal(vers, l.versions)

	// the first table is still keep `done` (for the previous DDLs operation)
	t.Require().True(l.IsDone(source, db, tbls[0]))
	t.Require().False(l.IsDone(source, db, tbls[1]))
	t.Require().False(l.IsResolved())

	// mark done for the second table, both of them are done (for different DDLs operations)
	t.Require().True(l.TryMarkDone(source, db, tbls[1]))
	t.Require().True(l.IsDone(source, db, tbls[0]))
	t.Require().True(l.IsDone(source, db, tbls[1]))
	// but the lock is still not resolved because tables have different schemas.
	t.Require().False(l.IsResolved())

	// TrySync for the first table, all tables become synced.
	info = newInfoWithVersion(task, source, db, tbls[0], downSchema, downTable, DDLs3, ti1, []*model.TableInfo{ti3}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	t.Require().NoError(err)
	t.Require().Equal(DDLs3, DDLs)
	t.Require().Equal([]string{}, cols)
	t.Require().Equal(vers, l.versions)

	// the first table become not-done, and the lock is un-resolved.
	t.Require().False(l.IsDone(source, db, tbls[0]))
	t.Require().True(l.IsDone(source, db, tbls[1]))
	t.Require().False(l.IsResolved())

	// mark done for the first table.
	t.Require().True(l.TryMarkDone(source, db, tbls[0]))
	t.Require().True(l.IsDone(source, db, tbls[0]))
	t.Require().True(l.IsDone(source, db, tbls[1]))

	// the lock become resolved now.
	t.Require().True(l.IsResolved())

	// TryMarkDone for not-existing table take no effect.
	t.Require().False(l.TryMarkDone(source, db, "not-exist"))
	t.Require().False(l.TryMarkDone(source, "not-exist", tbls[0]))
	t.Require().False(l.TryMarkDone("not-exist", db, tbls[0]))

	// check IsDone for not-existing table take no effect.
	t.Require().False(l.IsDone(source, db, "not-exist"))
	t.Require().False(l.IsDone(source, "not-exist", tbls[0]))
	t.Require().False(l.IsDone("not-exist", db, tbls[0]))
}

func (t *testLock) TestAddDifferentFieldLenColumns() {
	var (
		ID         = "test_lock_add_diff_flen_cols-`foo`.`bar`"
		task       = "test_lock_add_diff_flen_cols"
		source     = "mysql-replica-1"
		downSchema = "foo"
		downTable  = "bar"
		db         = "foo"
		tbls       = []string{"bar1", "bar2"}
		p          = parser.New()
		se         = mock.NewContext()

		tblID int64 = 111
		ti0         = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY)`)
		ti1         = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 VARCHAR(4))`)
		ti2         = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 VARCHAR(5))`)

		DDLs1 = []string{"ALTER TABLE bar ADD COLUMN c1 VARCHAR(4)"}
		DDLs2 = []string{"ALTER TABLE bar ADD COLUMN c1 VARCHAR(5)"}

		table1 = schemacmp.Encode(ti0)
		table2 = schemacmp.Encode(ti1)
		table3 = schemacmp.Encode(ti2)

		tables = map[string]map[string]struct{}{db: {tbls[0]: struct{}{}, tbls[1]: struct{}{}}}
		tts    = []TargetTable{newTargetTable(task, source, downSchema, downTable, tables)}
		l      = NewLock(etcdTestCli, ID, task, downSchema, downTable, schemacmp.Encode(ti0), tts, nil)

		vers = map[string]map[string]map[string]int64{
			source: {
				db: {tbls[0]: 0, tbls[1]: 0},
			},
		}
	)
	col, err := AddDifferentFieldLenColumns(ID, DDLs1[0], table1, table2)
	t.Require().Equal("c1", col)
	t.Require().NoError(err)
	col, err = AddDifferentFieldLenColumns(ID, DDLs2[0], table2, table3)
	t.Require().Equal("c1", col)
	t.Require().Error(err)
	t.Require().Regexp(".*add columns with different field lengths.*", err.Error())
	col, err = AddDifferentFieldLenColumns(ID, DDLs1[0], table3, table2)
	t.Require().Equal("c1", col)
	t.Require().Error(err)
	t.Require().Regexp(".*add columns with different field lengths.*", err.Error())

	// the initial status is synced but not resolved.
	t.checkLockSynced(l)
	t.checkLockNoDone(l)
	t.Require().False(l.IsResolved())

	// TrySync for the first table, no table has done the DDLs operation.
	info := newInfoWithVersion(task, source, db, tbls[0], downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1}, vers)
	DDLs, cols, err := l.TrySync(info, tts)
	t.Require().NoError(err)
	t.Require().Equal(DDLs1, DDLs)
	t.Require().Equal([]string{}, cols)
	t.Require().Equal(vers, l.versions)
	t.checkLockNoDone(l)
	t.Require().False(l.IsResolved())

	// TrySync for the second table, add a table with a larger field length
	info = newInfoWithVersion(task, source, db, tbls[1], downSchema, downTable, DDLs2, ti0, []*model.TableInfo{ti2}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	t.Require().Error(err)
	t.Require().Regexp(".*add columns with different field lengths.*", err.Error())
	t.Require().Equal([]string{}, DDLs)
	t.Require().Equal([]string{}, cols)
	t.Require().Equal(vers, l.versions)

	// case 2: add a column with a smaller field length
	l = NewLock(etcdTestCli, ID, task, downSchema, downTable, schemacmp.Encode(ti0), tts, nil)

	// TrySync for the first table, no table has done the DDLs operation.
	vers[source][db][tbls[0]]--
	info = NewInfo(task, source, db, tbls[1], downSchema, downTable, DDLs2, ti0, []*model.TableInfo{ti2})
	info.Version = vers[source][db][tbls[1]]
	DDLs, cols, err = l.TrySync(info, tts)
	t.Require().NoError(err)
	t.Require().Equal(DDLs2, DDLs)
	t.Require().Equal([]string{}, cols)
	t.Require().Equal(vers, l.versions)
	t.checkLockNoDone(l)
	t.Require().False(l.IsResolved())

	// TrySync for the second table, add a table with a smaller field length
	info = newInfoWithVersion(task, source, db, tbls[0], downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	t.Require().Error(err)
	t.Require().Regexp(".*add columns with different field lengths.*", err.Error())
	t.Require().Equal([]string{}, DDLs)
	t.Require().Equal([]string{}, cols)
	t.Require().Equal(vers, l.versions)
}

func (t *testLock) TestAddNotFullyDroppedColumns() {
	var (
		ID         = "test_lock_add_not_fully_dropped_cols-`foo`.`bar`"
		task       = "test_lock_add_not_fully_dropped_cols"
		source     = "mysql-replica-1"
		downSchema = "foo"
		downTable  = "bar"
		db         = "foo"
		tbls       = []string{"bar1", "bar2"}
		p          = parser.New()
		se         = mock.NewContext()

		tblID int64 = 111
		ti0         = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, b int, c int)`)
		ti1         = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, b int)`)
		ti2         = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY)`)
		ti3         = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c int)`)

		DDLs1 = []string{"ALTER TABLE bar DROP COLUMN c"}
		DDLs2 = []string{"ALTER TABLE bar DROP COLUMN b"}
		DDLs3 = []string{"ALTER TABLE bar ADD COLUMN b INT"}
		DDLs4 = []string{"ALTER TABLE bar ADD COLUMN c INT"}
		DDLs5 = []string{"ALTER TABLE bar DROP COLUMN c", "ALTER TABLE bar ADD COLUMN c INT"}

		tables = map[string]map[string]struct{}{db: {tbls[0]: struct{}{}, tbls[1]: struct{}{}}}
		tts    = []TargetTable{newTargetTable(task, source, downSchema, downTable, tables)}
		l      = NewLock(etcdTestCli, ID, task, downSchema, downTable, schemacmp.Encode(ti0), tts, nil)

		vers = map[string]map[string]map[string]int64{
			source: {
				db: {tbls[0]: 0, tbls[1]: 0},
			},
		}

		colm1 = map[string]map[string]map[string]map[string]map[string]DropColumnStage{
			ID: {
				"b": {source: {db: {tbls[0]: DropNotDone}}},
				"c": {source: {db: {tbls[0]: DropNotDone}}},
			},
		}
		colm2 = map[string]map[string]map[string]map[string]map[string]DropColumnStage{
			ID: {
				"b": {source: {db: {tbls[0]: DropNotDone, tbls[1]: DropDone}}},
				"c": {source: {db: {tbls[0]: DropNotDone}}},
			},
		}
		colm3 = map[string]map[string]map[string]map[string]map[string]DropColumnStage{
			ID: {
				"c": {source: {db: {tbls[0]: DropNotDone}}},
			},
		}
	)
	col, err := GetColumnName(ID, DDLs1[0], ast.AlterTableDropColumn)
	t.Require().Equal("c", col)
	t.Require().NoError(err)
	col, err = GetColumnName(ID, DDLs2[0], ast.AlterTableDropColumn)
	t.Require().Equal("b", col)
	t.Require().NoError(err)

	// the initial status is synced but not resolved.
	t.checkLockSynced(l)
	t.checkLockNoDone(l)
	t.Require().False(l.IsResolved())

	// TrySync for the first table, drop column c
	DDLs, cols, err := l.TrySync(newInfoWithVersion(task, source, db, tbls[0], downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1}, vers), tts)
	t.Require().NoError(err)
	t.Require().Equal([]string{}, DDLs)
	t.Require().Equal([]string{"c"}, cols)
	t.Require().Equal(vers, l.versions)
	t.Require().False(l.IsResolved())

	// TrySync for the first table, drop column b
	DDLs, cols, err = l.TrySync(newInfoWithVersion(task, source, db, tbls[0], downSchema, downTable, DDLs2, ti1, []*model.TableInfo{ti2}, vers), tts)
	t.Require().NoError(err)
	t.Require().Equal([]string{}, DDLs)
	t.Require().Equal([]string{"b"}, cols)
	t.Require().Equal(vers, l.versions)
	t.Require().False(l.IsResolved())

	colm, _, err := GetAllDroppedColumns(etcdTestCli)
	t.Require().NoError(err)
	t.Require().Equal(colm1, colm)

	// TrySync for the second table, drop column b, this column should be fully dropped
	DDLs, cols, err = l.TrySync(newInfoWithVersion(task, source, db, tbls[1], downSchema, downTable, DDLs2, ti0, []*model.TableInfo{ti3}, vers), tts)
	t.Require().NoError(err)
	t.Require().Equal(DDLs2, DDLs)
	t.Require().Equal([]string{"b"}, cols)
	t.Require().Equal(vers, l.versions)
	t.Require().False(l.IsResolved())
	// Simulate watch done operation from dm-worker
	op := NewOperation(utils.GenDDLLockID(task, downSchema, downTable), task, source, db, tbls[1], DDLs2, ConflictNone, "", true, []string{"b"})
	t.Require().NoError(l.DeleteColumnsByOp(op))

	colm, _, err = GetAllDroppedColumns(etcdTestCli)
	t.Require().NoError(err)
	t.Require().Equal(colm2, colm)

	op = NewOperation(utils.GenDDLLockID(task, downSchema, downTable), task, source, db, tbls[0], []string{}, ConflictNone, "", true, []string{"b"})
	t.Require().NoError(l.DeleteColumnsByOp(op))

	colm, _, err = GetAllDroppedColumns(etcdTestCli)
	t.Require().NoError(err)
	t.Require().Equal(colm3, colm)

	// TrySync for the first table, add column b, should succeed, because this column is fully dropped in the downstream
	DDLs, cols, err = l.TrySync(newInfoWithVersion(task, source, db, tbls[0], downSchema, downTable, DDLs3, ti2, []*model.TableInfo{ti1}, vers), tts)
	t.Require().NoError(err)
	t.Require().Equal(DDLs3, DDLs)
	t.Require().Equal([]string{}, cols)
	t.Require().Equal(vers, l.versions)
	t.Require().False(l.IsResolved())

	// TrySync for the first table, add column c, should fail, because this column isn't fully dropped in the downstream
	_, _, err = l.TrySync(newInfoWithVersion(task, source, db, tbls[0], downSchema, downTable, DDLs4, ti1, []*model.TableInfo{ti0}, vers), tts)
	t.Require().Error(err)
	t.Require().Regexp(".*add column c that wasn't fully dropped in downstream.*", err.Error())
	t.Require().False(l.IsResolved())

	// TrySync for the second table, drop column c, this column should be fully dropped
	DDLs, cols, err = l.TrySync(newInfoWithVersion(task, source, db, tbls[1], downSchema, downTable, DDLs1, ti3, []*model.TableInfo{ti2}, vers), tts)
	t.Require().NoError(err)
	t.Require().Equal(DDLs1, DDLs)
	t.Require().Equal([]string{"c"}, cols)
	t.Require().Equal(vers, l.versions)
	t.Require().False(l.IsResolved())
	// Simulate watch done operation from dm-worker
	op = NewOperation(utils.GenDDLLockID(task, downSchema, downTable), task, source, db, tbls[1], DDLs1, ConflictNone, "", true, []string{"c"})
	t.Require().NoError(l.DeleteColumnsByOp(op))

	// TrySync for the second table, add column c, should fail, because this column isn't fully dropped in the downstream
	_, _, err = l.TrySync(newInfoWithVersion(task, source, db, tbls[1], downSchema, downTable, DDLs4, ti1, []*model.TableInfo{ti0}, vers), tts)
	t.Require().Error(err)
	t.Require().Regexp(".*add column c that wasn't fully dropped in downstream.*", err.Error())
	t.Require().False(l.IsResolved())

	// Simulate watch done operation from dm-worker
	op = NewOperation(utils.GenDDLLockID(task, downSchema, downTable), task, source, db, tbls[0], []string{}, ConflictNone, "", true, []string{"c"})
	t.Require().NoError(l.DeleteColumnsByOp(op))

	// TrySync for the first table, add column c, should succeed, because this column is fully dropped in the downstream
	DDLs, cols, err = l.TrySync(newInfoWithVersion(task, source, db, tbls[0], downSchema, downTable, DDLs4, ti1, []*model.TableInfo{ti0}, vers), tts)
	t.Require().NoError(err)
	t.Require().Equal(DDLs4, DDLs)
	t.Require().Equal([]string{}, cols)
	t.Require().Equal(vers, l.versions)
	t.Require().False(l.IsResolved())

	_, _, err = l.TrySync(newInfoWithVersion(task, source, db, tbls[0], downSchema, downTable, DDLs5, ti0, []*model.TableInfo{ti1, ti0}, vers), tts)
	t.Require().Error(err)
	t.Require().Regexp(".*add column c that wasn't fully dropped in downstream.*", err.Error())
}

func (t *testLock) trySyncForAllTablesLarger(l *Lock,
	ddls []string, tableInfoBefore *model.TableInfo, tis []*model.TableInfo, tts []TargetTable, vers map[string]map[string]map[string]int64,
) {
	for source, schemaTables := range l.Ready() {
		for schema, tables := range schemaTables {
			for table := range tables {
				info := newInfoWithVersion(l.Task, source, schema, table, l.DownSchema, l.DownTable, ddls, tableInfoBefore, tis, vers)
				DDLs2, cols, err := l.TrySync(info, tts)
				t.Require().NoError(err)
				t.Require().Equal([]string{}, cols)
				t.Require().Equal(ddls, DDLs2)
			}
		}
	}
}

func (t *testLock) checkLockSynced(l *Lock) {
	synced, remain := l.IsSynced()
	t.Require().Equal(l.synced, synced)
	t.Require().True(synced)
	t.Require().Equal(0, remain)

	ready := l.Ready()
	for _, schemaTables := range ready {
		for _, tables := range schemaTables {
			for _, synced := range tables {
				t.Require().True(synced)
			}
		}
	}
}

func (t *testLock) checkLockNoDone(l *Lock) {
	t.Require().False(l.IsResolved())
	for source, schemaTables := range l.Ready() {
		for schema, tables := range schemaTables {
			for table := range tables {
				t.Require().False(l.IsDone(source, schema, table))
			}
		}
	}
}

func newInfoWithVersion(task, source, upSchema, upTable, downSchema, downTable string, ddls []string, tableInfoBefore *model.TableInfo,
	tableInfosAfter []*model.TableInfo, vers map[string]map[string]map[string]int64,
) Info {
	info := NewInfo(task, source, upSchema, upTable, downSchema, downTable, ddls, tableInfoBefore, tableInfosAfter)
	vers[source][upSchema][upTable]++
	info.Version = vers[source][upSchema][upTable]
	return info
}

func (t *testLock) TestLockTrySyncDifferentIndex() {
	var (
		ID               = "test_lock_try_sync_index-`foo`.`bar`"
		task             = "test_lock_try_sync_index"
		source           = "mysql-replica-1"
		downSchema       = "db"
		downTable        = "bar"
		db               = "db"
		tbls             = []string{"bar1", "bar2"}
		p                = parser.New()
		se               = mock.NewContext()
		tblID      int64 = 111
		DDLs1            = []string{"ALTER TABLE bar DROP INDEX idx_c1"}
		DDLs2            = []string{"ALTER TABLE bar ADD INDEX new_idx(c1)"}
		ti0              = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT, UNIQUE INDEX idx_c1(c1))`)
		ti1              = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT)`)
		ti2              = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT, INDEX new_idx(c1))`)
		tables           = map[string]map[string]struct{}{
			db: {tbls[0]: struct{}{}, tbls[1]: struct{}{}},
		}
		tts = []TargetTable{
			newTargetTable(task, source, downSchema, downTable, tables),
		}

		l = NewLock(etcdTestCli, ID, task, downSchema, downTable, schemacmp.Encode(ti0), tts, nil)

		vers = map[string]map[string]map[string]int64{
			source: {
				db: {tbls[0]: 0, tbls[1]: 0},
			},
		}
	)

	// the initial status is synced.
	t.checkLockSynced(l)
	t.checkLockNoDone(l)

	// try sync for one table, `DROP INDEX` returned directly (to make schema become more compatible).
	// `DROP INDEX` is handled like `ADD COLUMN`.
	info := newInfoWithVersion(task, source, db, tbls[0], downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1}, vers)
	DDLs, cols, err := l.TrySync(info, tts)
	t.Require().NoError(err)
	t.Require().Equal(DDLs1, DDLs)
	t.Require().Equal([]string{}, cols)
	t.Require().Equal(vers, l.versions)
	synced, remain := l.IsSynced()
	t.Require().Equal(l.synced, synced)
	t.Require().False(synced)
	t.Require().Equal(1, remain)

	cmp, err := l.tables[source][db][tbls[1]].Compare(schemacmp.Encode(ti0))
	t.Require().NoError(err)
	t.Require().Equal(0, cmp)

	// try sync ADD another INDEX for another table
	// `ADD INDEX` is handled like `DROP COLUMN`.
	info = newInfoWithVersion(task, source, db, tbls[1], downSchema, downTable, DDLs2, ti1, []*model.TableInfo{ti2}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	t.Require().NoError(err)
	t.Require().Equal([]string{}, DDLs) // no DDLs returned
	t.Require().Equal([]string{}, cols)
	t.Require().Equal(vers, l.versions)
	synced, remain = l.IsSynced()
	t.Require().Equal(l.synced, synced)
	t.Require().False(synced)
	t.Require().Equal(1, remain)

	joined, err := l.joinFinalTables()
	t.Require().NoError(err)
	cmp, err = l.tables[source][db][tbls[0]].Compare(joined)
	t.Require().NoError(err)
	t.Require().Equal(0, cmp)

	// try sync ADD INDEX for first table
	info = newInfoWithVersion(task, source, db, tbls[0], downSchema, downTable, DDLs2, ti1, []*model.TableInfo{ti2}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	t.Require().NoError(err)
	t.Require().Equal(DDLs2, DDLs)
	t.Require().Equal([]string{}, cols)
	t.Require().Equal(vers, l.versions)
	t.checkLockSynced(l)
}

func (t *testLock) TestFetchTableInfo() {
	var (
		meta             = "meta"
		ID               = "test_lock_try_sync_index-`foo`.`bar`"
		task             = "test_lock_try_sync_index"
		source           = "mysql-replica-1"
		downSchema       = "db"
		downTable        = "bar"
		schema           = "db"
		tbls             = []string{"bar1", "bar2"}
		p                = parser.New()
		se               = mock.NewContext()
		tblID      int64 = 111
		ti0              = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT, UNIQUE INDEX idx_c1(c1))`)
		tables           = map[string]map[string]struct{}{
			schema: {tbls[0]: struct{}{}, tbls[1]: struct{}{}},
		}
		tts = []TargetTable{
			newTargetTable(task, source, downSchema, downTable, tables),
		}
		query = fmt.Sprintf("SELECT table_info FROM `%s`.`%s` WHERE id = \\? AND cp_schema = \\? AND cp_table = \\?", meta, cputil.SyncerCheckpoint(task))
	)

	// nil downstream meta
	l := NewLock(etcdTestCli, ID, task, downSchema, downTable, schemacmp.Encode(ti0), tts, nil)
	ti, err := l.FetchTableInfos(task, source, schema, tbls[0])
	t.Require().True(terror.ErrMasterOptimisticDownstreamMetaNotFound.Equal(err))
	t.Require().Nil(ti)

	// table info not exist
	l = NewLock(etcdTestCli, ID, task, downSchema, downTable, schemacmp.Encode(ti0), tts, &DownstreamMeta{dbConfig: &dbconfig.DBConfig{}, meta: meta})
	conn.DefaultDBProvider = &conn.DefaultDBProviderImpl{}
	mock := conn.InitMockDB(t.T())
	mock.ExpectQuery(query).WithArgs(source, schema, tbls[0]).WillReturnRows(sqlmock.NewRows([]string{"table_info"}))
	ti, err = l.FetchTableInfos(task, source, schema, tbls[0])
	t.Require().True(terror.ErrDBExecuteFailed.Equal(err))
	t.Require().Nil(ti)

	// null table info
	l = NewLock(etcdTestCli, ID, task, downSchema, downTable, schemacmp.Encode(ti0), tts, &DownstreamMeta{dbConfig: &dbconfig.DBConfig{}, meta: meta})
	conn.DefaultDBProvider = &conn.DefaultDBProviderImpl{}
	mock = conn.InitMockDB(t.T())
	mock.ExpectQuery(query).WithArgs(source, schema, tbls[0]).WillReturnRows(sqlmock.NewRows([]string{"table_info"}).AddRow("null"))
	ti, err = l.FetchTableInfos(task, source, schema, tbls[0])
	t.Require().True(terror.ErrMasterOptimisticDownstreamMetaNotFound.Equal(err))
	t.Require().Nil(ti)

	// succeed
	tiBytes, err := json.Marshal(ti0)
	t.Require().NoError(err)
	conn.DefaultDBProvider = &conn.DefaultDBProviderImpl{}
	mock = conn.InitMockDB(t.T())
	mock.ExpectQuery(query).WithArgs(source, schema, tbls[0]).WillReturnRows(sqlmock.NewRows([]string{"table_info"}).AddRow(tiBytes))
	ti, err = l.FetchTableInfos(task, source, schema, tbls[0])
	t.Require().NoError(err)
	t.Require().NoError(mock.ExpectationsWereMet())
	t.Require().Equal(ti0, ti)
}

func (t *testLock) TestCheckAddDropColumns() {
	var (
		ID               = "test-`foo`.`bar`"
		task             = "test"
		source           = "mysql-replica-1"
		downSchema       = "db"
		downTable        = "bar"
		db               = "db"
		tbls             = []string{"bar1", "bar2"}
		p                = parser.New()
		se               = mock.NewContext()
		tblID      int64 = 111
		DDLs1            = "ALTER TABLE bar ADD COLUMN a VARCHAR(1)"
		DDLs2            = "ALTER TABLE bar ADD COLUMN a VARCHAR(2)"
		DDLs3            = "ALTER TABLE bar DROP COLUMN col"
		DDLs4            = "ALTER TABLE bar ADD COLUMN col int"
		ti0              = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, col int)`)
		ti1              = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, col int, a VARCHAR(1))`)
		ti2              = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, col int, a VARCHAR(2))`)
		ti3              = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, a VARCHAR(2))`)
		tables           = map[string]map[string]struct{}{
			db: {tbls[0]: struct{}{}, tbls[1]: struct{}{}},
		}
		tts = []TargetTable{
			newTargetTable(task, source, downSchema, downTable, tables),
		}

		l = NewLock(etcdTestCli, ID, task, downSchema, downTable, schemacmp.Encode(ti0), tts, nil)
	)

	l.tables[source][db][tbls[0]] = schemacmp.Encode(ti0)
	l.tables[source][db][tbls[1]] = schemacmp.Encode(ti1)

	col, err := l.checkAddDropColumn(source, db, tbls[0], DDLs1, schemacmp.Encode(ti0), schemacmp.Encode(ti1), nil)

	t.Require().NoError(err)
	t.Require().Equal(0, len(col))

	l.tables[source][db][tbls[0]] = schemacmp.Encode(ti1)
	col, err = l.checkAddDropColumn(source, db, tbls[1], DDLs2, schemacmp.Encode(ti0), schemacmp.Encode(ti2), nil)
	t.Require().Error(err)
	t.Require().Error(err)
	t.Require().Regexp(".*add columns with different field lengths.*", err.Error())
	t.Require().Equal(0, len(col))

	col, err = l.checkAddDropColumn(source, db, tbls[0], DDLs3, schemacmp.Encode(ti2), schemacmp.Encode(ti3), nil)
	t.Require().NoError(err)
	t.Require().Equal("col", col)

	l.columns = map[string]map[string]map[string]map[string]DropColumnStage{
		"col": {
			source: {
				db: {tbls[0]: DropNotDone},
			},
		},
	}

	col, err = l.checkAddDropColumn(source, db, tbls[0], DDLs4, schemacmp.Encode(ti3), schemacmp.Encode(ti2), nil)
	t.Require().Error(err)
	t.Require().Error(err)
	t.Require().Regexp(".*add column .* that wasn't fully dropped in downstream.*", err.Error())
	t.Require().Equal(0, len(col))
}

func (t *testLock) TestJoinTables() {
	var (
		source       = "mysql-replica-1"
		db           = "db"
		tbls         = []string{"bar1", "bar2"}
		p            = parser.New()
		se           = mock.NewContext()
		tblID  int64 = 111
		ti0          = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, col int)`)
		ti1          = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, col int, a VARCHAR(1))`)
		ti2          = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, col varchar(4))`)
		t0           = schemacmp.Encode(ti0)
		t1           = schemacmp.Encode(ti1)
		t2           = schemacmp.Encode(ti2)
	)

	l := &Lock{
		tables: map[string]map[string]map[string]schemacmp.Table{
			source: {
				db: {tbls[0]: t0, tbls[1]: t0},
			},
		},
		finalTables: map[string]map[string]map[string]schemacmp.Table{
			source: {
				db: {tbls[0]: t0, tbls[1]: t0},
			},
		},
	}

	joined, err := l.joinNormalTables()
	t.Require().NoError(err)
	cmp, err := joined.Compare(t0)
	t.Require().NoError(err)
	t.Require().Equal(0, cmp)
	joined, err = l.joinFinalTables()
	t.Require().NoError(err)
	cmp, err = joined.Compare(t0)
	t.Require().NoError(err)
	t.Require().Equal(0, cmp)
	_, err = l.joinConflictTables()
	t.Require().NoError(err)

	l.tables[source][db][tbls[0]] = t1
	l.finalTables[source][db][tbls[0]] = t1

	joined, err = l.joinNormalTables()
	t.Require().NoError(err)
	cmp, err = joined.Compare(t1)
	t.Require().NoError(err)
	t.Require().Equal(0, cmp)
	joined, err = l.joinFinalTables()
	t.Require().NoError(err)
	cmp, err = joined.Compare(t1)
	t.Require().NoError(err)
	t.Require().Equal(0, cmp)
	_, err = l.joinConflictTables()
	t.Require().NoError(err)

	l.tables[source][db][tbls[1]] = t1
	l.finalTables[source][db][tbls[1]] = t1
	l.conflictTables = map[string]map[string]map[string]schemacmp.Table{
		source: {
			db: {tbls[0]: t2},
		},
	}

	joined, err = l.joinNormalTables()
	t.Require().NoError(err)
	cmp, err = joined.Compare(t1)
	t.Require().NoError(err)
	t.Require().Equal(0, cmp)
	joined, err = l.joinFinalTables()
	t.Require().NoError(err)
	cmp, err = joined.Compare(t1)
	t.Require().NoError(err)
	t.Require().Equal(0, cmp)
	joined, err = l.joinConflictTables()
	t.Require().NoError(err)
	cmp, err = joined.Compare(t2)
	t.Require().NoError(err)
	t.Require().Equal(0, cmp)

	l.tables[source][db][tbls[1]] = t2
	_, err = l.joinNormalTables()
	t.Require().Error(err)
	t.Require().Error(err)
	t.Require().Regexp(".*incompatible mysql type.*", err.Error())

	l.resolveTables()
	t.Require().Len(l.conflictTables, 0)
	t.Require().Equal(l.finalTables, l.tables)
}

func (t *testLock) TestAddRemoveConflictTable() {
	var (
		source       = "source"
		schema       = "schema"
		table1       = "table1"
		table2       = "table2"
		table3       = "table3"
		p            = parser.New()
		se           = mock.NewContext()
		tblID  int64 = 111
		ti0          = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, col int)`)
		t0           = schemacmp.Encode(ti0)
	)
	l := &Lock{
		conflictTables: make(map[string]map[string]map[string]schemacmp.Table),
	}
	t.Require().Len(l.conflictTables, 0)

	l.addConflictTable(source, schema, table1, t0)
	t.Require().Len(l.conflictTables, 1)
	t.Require().Len(l.conflictTables[source], 1)
	t.Require().Len(l.conflictTables[source][schema], 1)
	tb := l.conflictTables[source][schema][table1]
	cmp, err := tb.Compare(t0)
	t.Require().NoError(err)
	t.Require().Equal(0, cmp)

	l.addConflictTable(source, schema, table1, t0)
	t.Require().Len(l.conflictTables, 1)
	t.Require().Len(l.conflictTables[source], 1)
	t.Require().Len(l.conflictTables[source][schema], 1)

	l.addConflictTable(source, schema, table2, t0)
	t.Require().Len(l.conflictTables, 1)
	t.Require().Len(l.conflictTables[source], 1)
	t.Require().Len(l.conflictTables[source][schema], 2)
	tb = l.conflictTables[source][schema][table2]
	cmp, err = tb.Compare(t0)
	t.Require().NoError(err)
	t.Require().Equal(0, cmp)

	l.removeConflictTable(source, schema, table3)
	t.Require().Len(l.conflictTables[source][schema], 2)

	l.removeConflictTable(source, schema, table1)
	t.Require().Len(l.conflictTables[source][schema], 1)
	tb = l.conflictTables[source][schema][table2]
	cmp, err = tb.Compare(t0)
	t.Require().NoError(err)
	t.Require().Equal(0, cmp)

	l.removeConflictTable(source, schema, table2)
	t.Require().Len(l.conflictTables, 0)
}

func (t *testLock) TestAllTableSmallerLarger() {
	var (
		source       = "source"
		schema       = "schema"
		table1       = "table1"
		table2       = "table2"
		p            = parser.New()
		se           = mock.NewContext()
		tblID  int64 = 111
		ti0          = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, col int)`)
		ti1          = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, new_col int)`)
		ti2          = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, new_col varchar(4))`)
		ti3          = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (a INT PRIMARY KEY, new_col varchar(4))`)
		ti4          = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, new_col2 varchar(4))`)
		ti5          = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id varchar(4) PRIMARY KEY, new_col int)`)
		ti6          = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, col int, new_col int)`)
		ti7          = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, col int, new_col varchar(4))`)
		ti8          = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, col int, col2 int not null)`)
		ti9          = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (a INT PRIMARY KEY, new_col int)`)
		t0           = schemacmp.Encode(ti0)
		t1           = schemacmp.Encode(ti1)
		t2           = schemacmp.Encode(ti2)
		t3           = schemacmp.Encode(ti3)
		t4           = schemacmp.Encode(ti4)
		t5           = schemacmp.Encode(ti5)
		t6           = schemacmp.Encode(ti6)
		t7           = schemacmp.Encode(ti7)
		t8           = schemacmp.Encode(ti8)
		t9           = schemacmp.Encode(ti9)
	)
	l := &Lock{
		tables: map[string]map[string]map[string]schemacmp.Table{
			source: {
				schema: {table1: t0, table2: t0},
			},
		},
		finalTables: map[string]map[string]map[string]schemacmp.Table{
			source: {
				schema: {table1: t0, table2: t0},
			},
		},
		conflictTables: make(map[string]map[string]map[string]schemacmp.Table),
	}
	t.Require().True(l.allFinalTableSmaller())

	// rename table
	l.addConflictTable(source, schema, table1, t1)
	l.finalTables[source][schema][table1] = t1
	t.Require().True(l.allConflictTableSmaller())
	t.Require().True(l.allConflictTableLarger())
	t.Require().False(l.allFinalTableSmaller())
	t.Require().False(l.allFinalTableLarger())
	l.addConflictTable(source, schema, table2, t1)
	l.finalTables[source][schema][table2] = t1
	t.Require().True(l.allConflictTableSmaller())
	t.Require().True(l.allConflictTableLarger())
	t.Require().True(l.allFinalTableSmaller())
	t.Require().True(l.allFinalTableLarger())
	// reset
	l.resolveTables()
	t.Require().Len(l.conflictTables, 0)
	t.Require().Equal(l.finalTables, l.tables)
	t.Require().Len(l.tables[source][schema], 2)

	// modify column
	l.addConflictTable(source, schema, table1, t2)
	l.finalTables[source][schema][table1] = t2
	t.Require().True(l.allConflictTableSmaller())
	t.Require().True(l.allConflictTableLarger())
	t.Require().False(l.allFinalTableSmaller())
	t.Require().False(l.allFinalTableLarger())
	l.addConflictTable(source, schema, table2, t2)
	l.finalTables[source][schema][table2] = t2
	t.Require().True(l.allConflictTableSmaller())
	t.Require().True(l.allConflictTableLarger())
	t.Require().True(l.allFinalTableSmaller())
	t.Require().True(l.allFinalTableLarger())
	// reset
	l.resolveTables()
	t.Require().Len(l.conflictTables, 0)
	t.Require().Equal(l.finalTables, l.tables)
	t.Require().Len(l.tables[source][schema], 2)
	t.Require().Equal(t2, l.tables[source][schema][table1])
	t.Require().Equal(t2, l.tables[source][schema][table2])

	// different rename
	l.addConflictTable(source, schema, table1, t3)
	l.finalTables[source][schema][table1] = t3
	t.Require().True(l.allConflictTableSmaller())
	t.Require().True(l.allConflictTableLarger())
	t.Require().False(l.allFinalTableSmaller())
	t.Require().False(l.allFinalTableLarger())
	l.addConflictTable(source, schema, table2, t4)
	l.finalTables[source][schema][table2] = t4
	t.Require().False(l.allConflictTableSmaller())
	t.Require().False(l.allConflictTableLarger())
	t.Require().False(l.allFinalTableSmaller())
	t.Require().False(l.allFinalTableLarger())
	// reset
	l.finalTables[source][schema][table1] = t1
	l.finalTables[source][schema][table2] = t1
	l.resolveTables()
	t.Require().Len(l.conflictTables, 0)
	t.Require().Equal(l.finalTables, l.tables)
	t.Require().Len(l.tables[source][schema], 2)
	t.Require().Equal(t1, l.tables[source][schema][table1])
	t.Require().Equal(t1, l.tables[source][schema][table2])

	// different modify
	l.addConflictTable(source, schema, table1, t2)
	l.finalTables[source][schema][table1] = t2
	t.Require().True(l.allConflictTableSmaller())
	t.Require().True(l.allConflictTableLarger())
	t.Require().False(l.allFinalTableSmaller())
	t.Require().False(l.allFinalTableLarger())
	l.addConflictTable(source, schema, table2, t5)
	l.finalTables[source][schema][table2] = t5
	t.Require().False(l.allConflictTableSmaller())
	t.Require().False(l.allConflictTableLarger())
	t.Require().False(l.allFinalTableSmaller())
	t.Require().False(l.allFinalTableLarger())
	// reset
	l.finalTables[source][schema][table1] = t1
	l.finalTables[source][schema][table2] = t1
	l.resolveTables()
	t.Require().Len(l.conflictTables, 0)
	t.Require().Equal(l.finalTables, l.tables)
	t.Require().Len(l.tables[source][schema], 2)
	t.Require().Equal(t1, l.tables[source][schema][table1])
	t.Require().Equal(t1, l.tables[source][schema][table2])

	// one table rename, one table modify
	l.addConflictTable(source, schema, table1, t4)
	l.finalTables[source][schema][table1] = t4
	t.Require().True(l.allConflictTableSmaller())
	t.Require().True(l.allConflictTableLarger())
	t.Require().False(l.allFinalTableSmaller())
	t.Require().False(l.allFinalTableLarger())
	l.addConflictTable(source, schema, table2, t5)
	l.finalTables[source][schema][table2] = t5
	t.Require().False(l.allConflictTableSmaller())
	t.Require().False(l.allConflictTableLarger())
	t.Require().False(l.allFinalTableSmaller())
	t.Require().False(l.allFinalTableLarger())
	// reset
	l.finalTables[source][schema][table1] = t0
	l.finalTables[source][schema][table2] = t0
	l.resolveTables()
	t.Require().Len(l.conflictTables, 0)
	t.Require().Equal(l.finalTables, l.tables)
	t.Require().Len(l.tables[source][schema], 2)
	t.Require().Equal(t0, l.tables[source][schema][table1])
	t.Require().Equal(t0, l.tables[source][schema][table2])

	// one table rename, one table add and drop
	l.addConflictTable(source, schema, table1, t1)
	l.finalTables[source][schema][table1] = t1
	t.Require().True(l.allConflictTableSmaller())
	t.Require().True(l.allConflictTableLarger())
	t.Require().False(l.allFinalTableSmaller())
	t.Require().False(l.allFinalTableLarger())
	l.finalTables[source][schema][table2] = t6
	t.Require().True(l.allConflictTableSmaller())
	t.Require().True(l.allConflictTableLarger())
	t.Require().False(l.allFinalTableSmaller())
	t.Require().True(l.allFinalTableLarger())
	l.finalTables[source][schema][table2] = t1
	t.Require().True(l.allConflictTableSmaller())
	t.Require().True(l.allConflictTableLarger())
	t.Require().True(l.allFinalTableSmaller())
	t.Require().True(l.allFinalTableLarger())
	// reset
	l.finalTables[source][schema][table1] = t0
	l.finalTables[source][schema][table2] = t0
	l.resolveTables()
	t.Require().Len(l.conflictTables, 0)
	t.Require().Equal(l.finalTables, l.tables)
	t.Require().Len(l.tables[source][schema], 2)
	t.Require().Equal(t0, l.tables[source][schema][table1])
	t.Require().Equal(t0, l.tables[source][schema][table2])

	// one table modify, one table add and drop
	l.addConflictTable(source, schema, table1, t2)
	l.finalTables[source][schema][table1] = t2
	t.Require().True(l.allConflictTableSmaller())
	t.Require().True(l.allConflictTableLarger())
	t.Require().False(l.allFinalTableSmaller())
	t.Require().False(l.allFinalTableLarger())
	l.finalTables[source][schema][table2] = t7
	t.Require().True(l.allConflictTableSmaller())
	t.Require().True(l.allConflictTableLarger())
	t.Require().False(l.allFinalTableSmaller())
	t.Require().True(l.allFinalTableLarger())
	l.finalTables[source][schema][table2] = t2
	t.Require().True(l.allConflictTableSmaller())
	t.Require().True(l.allConflictTableLarger())
	t.Require().True(l.allFinalTableSmaller())
	t.Require().True(l.allFinalTableLarger())
	// reset
	l.finalTables[source][schema][table1] = t0
	l.finalTables[source][schema][table2] = t0
	l.resolveTables()
	t.Require().Len(l.conflictTables, 0)
	t.Require().Equal(l.finalTables, l.tables)
	t.Require().Len(l.tables[source][schema], 2)
	t.Require().Equal(t0, l.tables[source][schema][table1])
	t.Require().Equal(t0, l.tables[source][schema][table2])

	// not null no default
	l.addConflictTable(source, schema, table1, t8)
	l.finalTables[source][schema][table1] = t8
	t.Require().True(l.allConflictTableSmaller())
	t.Require().True(l.allConflictTableLarger())
	t.Require().False(l.allFinalTableSmaller())
	t.Require().False(l.allFinalTableLarger())
	l.addConflictTable(source, schema, table2, t8)
	l.finalTables[source][schema][table2] = t8
	t.Require().True(l.allConflictTableSmaller())
	t.Require().True(l.allConflictTableLarger())
	t.Require().True(l.allFinalTableSmaller())
	t.Require().True(l.allFinalTableLarger())
	// reset
	l.finalTables[source][schema][table1] = t0
	l.finalTables[source][schema][table2] = t0
	l.resolveTables()
	t.Require().Len(l.conflictTables, 0)
	t.Require().Equal(l.finalTables, l.tables)
	t.Require().Len(l.tables[source][schema], 2)
	t.Require().Equal(t0, l.tables[source][schema][table1])
	t.Require().Equal(t0, l.tables[source][schema][table2])

	// multiple rename
	// tb1: rename col to new_col
	l.addConflictTable(source, schema, table1, t1)
	l.finalTables[source][schema][table1] = t1
	t.Require().True(l.allConflictTableSmaller())
	t.Require().True(l.allConflictTableLarger())
	t.Require().False(l.allFinalTableSmaller())
	t.Require().False(l.allFinalTableLarger())
	// tb2: rename col to new_col
	l.addConflictTable(source, schema, table2, t1)
	l.finalTables[source][schema][table2] = t1
	t.Require().True(l.allConflictTableSmaller())
	t.Require().True(l.allConflictTableLarger())
	t.Require().True(l.allFinalTableSmaller())
	t.Require().True(l.allFinalTableLarger())
	l.resolveTables()
	// tb1: rename id to a
	l.addConflictTable(source, schema, table1, t9)
	l.finalTables[source][schema][table1] = t9
	t.Require().False(l.noConflictWithOneNormalTable(source, schema, table1, t1, t9))
	t.Require().True(l.allConflictTableSmaller())
	t.Require().True(l.allConflictTableLarger())
	t.Require().False(l.allFinalTableSmaller())
	t.Require().False(l.allFinalTableLarger())
	// tb2: rename col to new_col (idempotent)
	l.tables[source][schema][table2] = t0
	l.addConflictTable(source, schema, table2, t1)
	l.finalTables[source][schema][table2] = t1
	t.Require().True(l.noConflictWithOneNormalTable(source, schema, table2, t0, t1))
	l.removeConflictTable(source, schema, table2)
	l.tables[source][schema][table2] = t1
	// tb2: rename id to a
	l.addConflictTable(source, schema, table2, t9)
	l.finalTables[source][schema][table2] = t9
	t.Require().False(l.noConflictWithOneNormalTable(source, schema, table2, t1, t9))
	t.Require().True(l.allConflictTableSmaller())
	t.Require().True(l.allConflictTableLarger())
	t.Require().True(l.allFinalTableSmaller())
	t.Require().True(l.allFinalTableLarger())
	// reset
	l.finalTables[source][schema][table1] = t0
	l.finalTables[source][schema][table2] = t0
	l.resolveTables()
	t.Require().Len(l.conflictTables, 0)
	t.Require().Equal(l.finalTables, l.tables)
	t.Require().Len(l.tables[source][schema], 2)
	t.Require().Equal(t0, l.tables[source][schema][table1])
	t.Require().Equal(t0, l.tables[source][schema][table2])
}

func (t *testLock) TestNoConflictWithOneNormalTable() {
	var (
		source       = "source"
		schema       = "schema"
		table1       = "table1"
		table2       = "table2"
		p            = parser.New()
		se           = mock.NewContext()
		tblID  int64 = 111
		ti0          = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, a int, col int)`)
		ti1          = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, a int, new_col int)`)
		ti2          = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, a int, col varchar(4))`)
		ti3          = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, a int, new_col2 int)`)
		ti4          = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, b int, new_col int)`)
		t0           = schemacmp.Encode(ti0)
		t1           = schemacmp.Encode(ti1)
		t2           = schemacmp.Encode(ti2)
		t3           = schemacmp.Encode(ti3)
		t4           = schemacmp.Encode(ti4)
	)
	l := &Lock{
		tables: map[string]map[string]map[string]schemacmp.Table{
			source: {
				schema: {table1: t0, table2: t0},
			},
		},
	}

	// table1 nothing happened.
	// table2 rename column
	t.Require().False(l.noConflictWithOneNormalTable(source, schema, table2, t0, t1))

	// mock table1 rename column already
	l.tables[source][schema][table1] = t1
	// table2 rename column
	t.Require().True(l.noConflictWithOneNormalTable(source, schema, table2, t0, t1))
	// table2 modify column
	t.Require().False(l.noConflictWithOneNormalTable(source, schema, table2, t0, t2))
	// table2 different rename
	t.Require().False(l.noConflictWithOneNormalTable(source, schema, table2, t0, t3))

	// mock table1 rename another column already
	l.tables[source][schema][table1] = t4
	// same results
	// table2 rename column
	t.Require().True(l.noConflictWithOneNormalTable(source, schema, table2, t0, t1))
	// table2 modify column
	t.Require().False(l.noConflictWithOneNormalTable(source, schema, table2, t0, t2))
	// table2 different rename
	t.Require().False(l.noConflictWithOneNormalTable(source, schema, table2, t0, t3))
}

func checkRedirectOp(t *testing.T, task, source, schema, table string) bool {
	ops, _, err := GetAllOperations(etcdTestCli)
	require.NoError(t, err)
	if _, ok := ops[task]; !ok {
		return false
	}
	if _, ok := ops[task][source]; !ok {
		return false
	}
	if _, ok := ops[task][source][schema]; !ok {
		return false
	}
	op, ok := ops[task][source][schema][table]
	if !ok {
		return false
	}
	return op.ConflictStage == ConflictResolved
}

func (t *testLock) TestTrySyncForOneDDL() {
	var (
		ID               = "test-`foo`.`bar`"
		task             = "test"
		source           = "source"
		schema           = "schema"
		downSchema       = "downSchema"
		downTable        = "downTable"
		table1           = "table1"
		table2           = "table2"
		p                = parser.New()
		se               = mock.NewContext()
		tblID      int64 = 111
		ti0              = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, col1 int)`)
		ti1              = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, col1 int, col2 int)`)
		ti2              = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY)`)
		ti3              = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, col1 int, col3 int)`)
		ti4              = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, col2 int)`)
		ti5              = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, col3 int)`)
		ti6              = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, col3 varchar(4))`)
		ti7              = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, col3 int)`)
		ti8              = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, col3 varchar(4), col4 int not null)`)
		ti9              = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, col2 varchar(4), col4 int not null)`)
		ti10             = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, col3 int, col4 int not null)`)
		t0               = schemacmp.Encode(ti0)
		t1               = schemacmp.Encode(ti1)
		t2               = schemacmp.Encode(ti2)
		t3               = schemacmp.Encode(ti3)
		t4               = schemacmp.Encode(ti4)
		t5               = schemacmp.Encode(ti5)
		t6               = schemacmp.Encode(ti6)
		t7               = schemacmp.Encode(ti7)
		t8               = schemacmp.Encode(ti8)
		t9               = schemacmp.Encode(ti9)
		t10              = schemacmp.Encode(ti10)
		tables           = map[string]map[string]struct{}{
			schema: {table1: struct{}{}, table2: struct{}{}},
		}
		tts = []TargetTable{
			newTargetTable(task, source, downSchema, downTable, tables),
		}
		l = NewLock(etcdTestCli, ID, task, downSchema, downTable, t0, tts, nil)
	)

	// check create table statement
	schemaChanged, conflictStage := l.trySyncForOneDDL(source, schema, table1, t0, t0)
	t.Require().True(schemaChanged)
	t.Require().Equal(ConflictNone, conflictStage)

	// check alter table add column
	schemaChanged, conflictStage = l.trySyncForOneDDL(source, schema, table1, t0, t1)
	t.Require().True(schemaChanged)
	t.Require().Equal(ConflictNone, conflictStage)

	// check create partition, no changed since https://github.com/pingcap/tidb/blob/42075e6ad60ba5d1f860ac845908f0c34ac28253/pkg/util/schemacmp/table.go#L256
	schemaChanged, conflictStage = l.trySyncForOneDDL(source, schema, table1, t0, t1)
	t.Require().True(schemaChanged)
	t.Require().Equal(ConflictNone, conflictStage)

	// check alter table drop column
	schemaChanged, conflictStage = l.trySyncForOneDDL(source, schema, table2, t0, t2)
	t.Require().False(schemaChanged)
	t.Require().Equal(ConflictNone, conflictStage)

	// check table rename column
	schemaChanged, conflictStage = l.trySyncForOneDDL(source, schema, table1, t1, t3)
	t.Require().False(schemaChanged)
	t.Require().Equal(ConflictSkipWaitRedirect, conflictStage)

	// check other table add column
	schemaChanged, conflictStage = l.trySyncForOneDDL(source, schema, table2, t2, t4)
	t.Require().True(schemaChanged)
	t.Require().Equal(ConflictNone, conflictStage)

	// check all table rename column
	schemaChanged, conflictStage = l.trySyncForOneDDL(source, schema, table2, t4, t5)
	t.Require().True(schemaChanged)
	t.Require().Equal(ConflictNone, conflictStage)
	// table1 redirect
	t.Require().True(checkRedirectOp(t.T(), task, source, schema, table1))

	// check one table modify column
	schemaChanged, conflictStage = l.trySyncForOneDDL(source, schema, table2, t5, t6)
	t.Require().False(schemaChanged)
	t.Require().Equal(ConflictSkipWaitRedirect, conflictStage)

	// check other table drop column
	schemaChanged, conflictStage = l.trySyncForOneDDL(source, schema, table1, t3, t7)
	t.Require().True(schemaChanged)
	t.Require().Equal(ConflictNone, conflictStage)

	// check all table modify column
	schemaChanged, conflictStage = l.trySyncForOneDDL(source, schema, table1, t7, t6)
	t.Require().True(schemaChanged)
	t.Require().Equal(ConflictNone, conflictStage)
	// table2 redirect
	t.Require().True(checkRedirectOp(t.T(), task, source, schema, table2))

	// check add column not null no default
	schemaChanged, conflictStage = l.trySyncForOneDDL(source, schema, table1, t6, t8)
	t.Require().False(schemaChanged)
	t.Require().Equal(ConflictSkipWaitRedirect, conflictStage)
	// check idempotent.
	schemaChanged, conflictStage = l.trySyncForOneDDL(source, schema, table1, t6, t8)
	t.Require().False(schemaChanged)
	t.Require().Equal(ConflictSkipWaitRedirect, conflictStage)

	schemaChanged, conflictStage = l.trySyncForOneDDL(source, schema, table2, t6, t8)
	t.Require().True(schemaChanged)
	t.Require().Equal(ConflictNone, conflictStage)
	// table1 redirect
	t.Require().True(checkRedirectOp(t.T(), task, source, schema, table2))
	// check idempotent.
	schemaChanged, conflictStage = l.trySyncForOneDDL(source, schema, table2, t6, t8)
	t.Require().True(schemaChanged)
	t.Require().Equal(ConflictNone, conflictStage)

	// check multiple conflict DDL
	// tb1 rename column
	schemaChanged, conflictStage = l.trySyncForOneDDL(source, schema, table1, t8, t9)
	t.Require().False(schemaChanged)
	t.Require().Equal(ConflictSkipWaitRedirect, conflictStage)
	// tb2 modify column
	schemaChanged, conflictStage = l.trySyncForOneDDL(source, schema, table2, t8, t10)
	t.Require().False(schemaChanged)
	t.Require().Equal(ConflictDetected, conflictStage)
}
