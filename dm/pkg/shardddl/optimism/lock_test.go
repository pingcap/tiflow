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
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/schemacmp"
	"github.com/pingcap/tiflow/dm/config/dbconfig"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	"github.com/pingcap/tiflow/dm/pkg/cputil"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"go.etcd.io/etcd/tests/v3/integration"
)

type testLock struct{}

var _ = Suite(&testLock{})

func TestLock(t *testing.T) {
	integration.BeforeTestExternal(t)
	mockCluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer mockCluster.Terminate(t)

	etcdTestCli = mockCluster.RandClient()

	TestingT(t)
}

func (t *testLock) SetUpSuite(c *C) {
	c.Assert(log.InitLogger(&log.Config{}), IsNil)
}

func (t *testLock) TearDownSuite(c *C) {
	clearTestInfoOperation(c)
}

func (t *testLock) TestLockTrySyncNormal(c *C) {
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
		ti0              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY)`)
		ti1              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT)`)
		ti2              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT, c2 BIGINT, c3 TEXT)`)
		ti2_1            = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT, c2 BIGINT)`)
		ti3              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT, c2 BIGINT)`)
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
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)

	// CASE: all tables execute a single & same DDL (schema become larger).
	syncedCount := 0
	for _, source := range sources {
		if source == sources[len(sources)-1] {
			ready := l.Ready()
			for _, source2 := range sources {
				synced := source != source2 // tables before the last source have synced.
				for _, db2 := range dbs {
					for _, tbl2 := range tbls {
						c.Assert(ready[source2][db2][tbl2], Equals, synced)
					}
				}
			}
		}

		for _, db := range dbs {
			for _, tbl := range tbls {
				info := newInfoWithVersion(task, source, db, tbl, downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1}, vers)
				DDLs, cols, err := l.TrySync(info, tts)
				c.Assert(err, IsNil)
				c.Assert(DDLs, DeepEquals, DDLs1)
				c.Assert(cols, DeepEquals, []string{})
				c.Assert(l.versions, DeepEquals, vers)

				syncedCount++
				synced, remain := l.IsSynced()
				c.Assert(synced, Equals, syncedCount == tableCount)
				c.Assert(remain, Equals, tableCount-syncedCount)
				c.Assert(synced, Equals, l.synced)
			}
		}
	}
	// synced again after all tables applied the DDL.
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)

	// CASE: TrySync again after synced is idempotent.
	info := newInfoWithVersion(task, sources[0], dbs[0], tbls[0], downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1}, vers)
	DDLs, cols, err := l.TrySync(info, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs1)
	c.Assert(cols, DeepEquals, []string{})
	c.Assert(l.versions, DeepEquals, vers)
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)

	// CASE: need to add more than one DDL to reach the desired schema (schema become larger).
	// add two columns for one table.
	info = newInfoWithVersion(task, sources[0], dbs[0], tbls[0], downSchema, downTable, DDLs2, ti1, []*model.TableInfo{ti2_1, ti2}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs2)
	c.Assert(cols, DeepEquals, []string{})
	c.Assert(l.versions, DeepEquals, vers)
	ready := l.Ready()
	c.Assert(ready[sources[0]][dbs[0]][tbls[0]], IsTrue)
	c.Assert(ready[sources[0]][dbs[0]][tbls[1]], IsFalse)

	// TrySync again is idempotent (more than one DDL).
	info = newInfoWithVersion(task, sources[0], dbs[0], tbls[0], downSchema, downTable, DDLs2, ti1, []*model.TableInfo{ti2_1, ti2}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs2)
	c.Assert(cols, DeepEquals, []string{})
	c.Assert(l.versions, DeepEquals, vers)
	ready = l.Ready()
	c.Assert(ready[sources[0]][dbs[0]][tbls[0]], IsTrue)
	c.Assert(ready[sources[0]][dbs[0]][tbls[1]], IsFalse)

	// add only the first column for another table.
	info = newInfoWithVersion(task, sources[0], dbs[0], tbls[1], downSchema, downTable, DDLs2[0:1], ti1, []*model.TableInfo{ti2_1}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs2[0:1])
	c.Assert(cols, DeepEquals, []string{})
	c.Assert(l.versions, DeepEquals, vers)
	ready = l.Ready()
	c.Assert(ready[sources[0]][dbs[0]][tbls[0]], IsTrue)
	c.Assert(ready[sources[0]][dbs[0]][tbls[1]], IsFalse)
	synced, remain := l.IsSynced()
	c.Assert(synced, IsFalse)
	c.Assert(remain, Equals, tableCount-1)
	c.Assert(synced, Equals, l.synced)
	cmp, err := l.tables[sources[0]][dbs[0]][tbls[0]].Compare(l.tables[sources[0]][dbs[0]][tbls[1]])
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 1)

	// TrySync again is idempotent
	info = newInfoWithVersion(task, sources[0], dbs[0], tbls[1], downSchema, downTable, DDLs2[0:1], ti1, []*model.TableInfo{ti2_1}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs2[0:1])
	c.Assert(cols, DeepEquals, []string{})
	c.Assert(l.versions, DeepEquals, vers)
	ready = l.Ready()
	c.Assert(ready[sources[0]][dbs[0]][tbls[0]], IsTrue)
	c.Assert(ready[sources[0]][dbs[0]][tbls[1]], IsFalse)

	// add the second column for another table.
	info = newInfoWithVersion(task, sources[0], dbs[0], tbls[1], downSchema, downTable, DDLs2[1:2], ti2_1, []*model.TableInfo{ti2}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs2[1:2])
	c.Assert(cols, DeepEquals, []string{})
	c.Assert(l.versions, DeepEquals, vers)
	ready = l.Ready()
	c.Assert(ready[sources[0]][dbs[0]][tbls[1]], IsTrue) // ready now.
	synced, remain = l.IsSynced()
	c.Assert(synced, IsFalse)
	c.Assert(remain, Equals, tableCount-2)
	c.Assert(synced, Equals, l.synced)
	cmp, err = l.tables[sources[0]][dbs[0]][tbls[0]].Compare(l.tables[sources[0]][dbs[0]][tbls[1]])
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)

	// Try again (for the second DDL).
	info = newInfoWithVersion(task, sources[0], dbs[0], tbls[1], downSchema, downTable, DDLs2[1:2], ti2_1, []*model.TableInfo{ti2}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs2[1:2])
	c.Assert(cols, DeepEquals, []string{})
	c.Assert(l.versions, DeepEquals, vers)

	t.trySyncForAllTablesLarger(c, l, DDLs2, ti1, []*model.TableInfo{ti2_1, ti2}, tts, vers)
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)

	// CASE: all tables execute a single & same DDL (schema become smaller).
	syncedCount = 0
	for _, source := range sources {
		if source == sources[len(sources)-1] {
			ready = l.Ready()
			for _, source2 := range sources {
				synced = source == source2 // tables before the last source have not synced.
				for _, db2 := range dbs {
					for _, tbl2 := range tbls {
						c.Assert(ready[source2][db2][tbl2], Equals, synced)
					}
				}
			}
		}

		for _, db := range dbs {
			for _, tbl := range tbls {
				syncedCount++
				info = newInfoWithVersion(task, source, db, tbl, downSchema, downTable, DDLs3, ti2, []*model.TableInfo{ti3}, vers)
				DDLs, cols, err = l.TrySync(info, tts)
				c.Assert(err, IsNil)
				c.Assert(l.versions, DeepEquals, vers)
				c.Assert(cols, DeepEquals, []string{"c3"})
				synced, remain = l.IsSynced()
				c.Assert(synced, Equals, l.synced)
				if syncedCount == tableCount {
					c.Assert(DDLs, DeepEquals, DDLs3)
					c.Assert(synced, IsTrue)
					c.Assert(remain, Equals, 0)
				} else {
					c.Assert(DDLs, DeepEquals, []string{})
					c.Assert(synced, IsFalse)
					c.Assert(remain, Equals, syncedCount)
				}
			}
		}
	}
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)

	// CASE: need to drop more than one DDL to reach the desired schema (schema become smaller).
	// drop two columns for one table.
	info = newInfoWithVersion(task, sources[0], dbs[0], tbls[0], downSchema, downTable, DDLs4, ti3, []*model.TableInfo{ti4_1, ti4}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, []string{})
	c.Assert(cols, DeepEquals, []string{"c2", "c1"})
	c.Assert(l.versions, DeepEquals, vers)
	ready = l.Ready()
	c.Assert(ready[sources[0]][dbs[0]][tbls[0]], IsFalse)
	c.Assert(ready[sources[0]][dbs[0]][tbls[1]], IsTrue)

	// TrySync again is idempotent.
	info = newInfoWithVersion(task, sources[0], dbs[0], tbls[0], downSchema, downTable, DDLs4, ti3, []*model.TableInfo{ti4_1, ti4}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, []string{})
	c.Assert(cols, DeepEquals, []string{"c2", "c1"})
	c.Assert(l.versions, DeepEquals, vers)
	ready = l.Ready()
	c.Assert(ready[sources[0]][dbs[0]][tbls[0]], IsFalse)
	c.Assert(ready[sources[0]][dbs[0]][tbls[1]], IsTrue)

	// drop only the first column for another table.
	info = newInfoWithVersion(task, sources[0], dbs[0], tbls[1], downSchema, downTable, DDLs4[0:1], ti3, []*model.TableInfo{ti4_1}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, []string{})
	c.Assert(cols, DeepEquals, []string{"c2"})
	c.Assert(l.versions, DeepEquals, vers)
	ready = l.Ready()
	c.Assert(ready[sources[0]][dbs[0]][tbls[0]], IsFalse)
	c.Assert(ready[sources[0]][dbs[0]][tbls[1]], IsFalse)
	cmp, err = l.tables[sources[0]][dbs[0]][tbls[0]].Compare(l.tables[sources[0]][dbs[0]][tbls[1]])
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, -1)

	// TrySync again (only the first DDL).
	info = newInfoWithVersion(task, sources[0], dbs[0], tbls[1], downSchema, downTable, DDLs4[0:1], ti3, []*model.TableInfo{ti4_1}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, []string{})
	c.Assert(cols, DeepEquals, []string{"c2"})
	c.Assert(l.versions, DeepEquals, vers)

	// drop the second column for another table.
	info = newInfoWithVersion(task, sources[0], dbs[0], tbls[1], downSchema, downTable, DDLs4[1:2], ti4_1, []*model.TableInfo{ti4}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, []string{})
	c.Assert(cols, DeepEquals, []string{"c1"})
	c.Assert(l.versions, DeepEquals, vers)
	ready = l.Ready()
	c.Assert(ready[sources[0]][dbs[0]][tbls[0]], IsFalse)
	c.Assert(ready[sources[0]][dbs[0]][tbls[1]], IsFalse)
	cmp, err = l.tables[sources[0]][dbs[0]][tbls[0]].Compare(l.tables[sources[0]][dbs[0]][tbls[1]])
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)

	// TrySync again (for the second DDL).
	info = newInfoWithVersion(task, sources[0], dbs[0], tbls[1], downSchema, downTable, DDLs4[1:2], ti4_1, []*model.TableInfo{ti4}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, []string{})
	c.Assert(cols, DeepEquals, []string{"c1"})
	c.Assert(l.versions, DeepEquals, vers)

	// try drop columns for other tables to reach the same schema.
	remain = tableCount - 2
	for source, schemaTables := range l.Ready() {
		for schema, tables := range schemaTables {
			for table, synced2 := range tables {
				if synced2 { // do not `TrySync` again for previous two (un-synced now).
					info = newInfoWithVersion(task, source, schema, table, downSchema, downTable, DDLs4, ti3, []*model.TableInfo{ti4_1, ti4}, vers)
					DDLs, cols, err = l.TrySync(info, tts)
					c.Assert(err, IsNil)
					c.Assert(cols, DeepEquals, []string{"c2", "c1"})
					c.Assert(l.versions, DeepEquals, vers)
					remain--
					if remain == 0 {
						c.Assert(DDLs, DeepEquals, DDLs4)
					} else {
						c.Assert(DDLs, DeepEquals, []string{})
					}
				}
			}
		}
	}
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)
}

func (t *testLock) TestLockTrySyncIndex(c *C) {
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
		ti0              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT, UNIQUE INDEX idx_c1(c1))`)
		ti1              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT)`)
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
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)

	// try sync for one table, `DROP INDEX` returned directly (to make schema become more compatible).
	// `DROP INDEX` is handled like `ADD COLUMN`.
	info := newInfoWithVersion(task, source, db, tbls[0], downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1}, vers)
	DDLs, cols, err := l.TrySync(info, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs1)
	c.Assert(cols, DeepEquals, []string{})
	c.Assert(l.versions, DeepEquals, vers)
	synced, remain := l.IsSynced()
	c.Assert(synced, Equals, l.synced)
	c.Assert(synced, IsFalse)
	c.Assert(remain, Equals, 1)

	// try sync for another table, also got `DROP INDEX` now.
	info = newInfoWithVersion(task, source, db, tbls[1], downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs1)
	c.Assert(cols, DeepEquals, []string{})
	c.Assert(l.versions, DeepEquals, vers)
	t.checkLockSynced(c, l)

	// try sync for one table, `ADD INDEX` not returned directly (to keep the schema more compatible).
	// `ADD INDEX` is handled like `DROP COLUMN`.
	info = newInfoWithVersion(task, source, db, tbls[0], downSchema, downTable, DDLs2, ti1, []*model.TableInfo{ti2}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, []string{}) // no DDLs returned
	c.Assert(cols, DeepEquals, []string{})
	c.Assert(l.versions, DeepEquals, vers)
	synced, remain = l.IsSynced()
	c.Assert(synced, Equals, l.synced)
	c.Assert(synced, IsFalse)
	c.Assert(remain, Equals, 1)

	// try sync for another table, got `ADD INDEX` now.
	info = newInfoWithVersion(task, source, db, tbls[1], downSchema, downTable, DDLs2, ti1, []*model.TableInfo{ti2}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs2)
	c.Assert(cols, DeepEquals, []string{})
	c.Assert(l.versions, DeepEquals, vers)
	t.checkLockSynced(c, l)
}

func (t *testLock) TestLockTrySyncNullNotNull(c *C) {
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
		ti0              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT NULL DEFAULT 1234)`)
		ti1              = createTableInfo(c, p, se, tblID,
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
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)

	for i := 0; i < 2; i++ { // two round
		// try sync for one table, from `NULL` to `NOT NULL`, no DDLs returned.
		info := newInfoWithVersion(task, source, db, tbls[0], downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1}, vers)
		DDLs, cols, err := l.TrySync(info, tts)
		c.Assert(err, IsNil)
		c.Assert(DDLs, DeepEquals, []string{})
		c.Assert(cols, DeepEquals, []string{})
		c.Assert(l.versions, DeepEquals, vers)

		// try sync for another table, DDLs returned.
		info = newInfoWithVersion(task, source, db, tbls[1], downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1}, vers)
		DDLs, cols, err = l.TrySync(info, tts)
		c.Assert(err, IsNil)
		c.Assert(DDLs, DeepEquals, DDLs1)
		c.Assert(cols, DeepEquals, []string{})
		c.Assert(l.versions, DeepEquals, vers)

		// try sync for one table, from `NOT NULL` to `NULL`, DDLs returned.
		info = newInfoWithVersion(task, source, db, tbls[0], downSchema, downTable, DDLs2, ti1, []*model.TableInfo{ti2}, vers)
		DDLs, cols, err = l.TrySync(info, tts)
		c.Assert(err, IsNil)
		c.Assert(DDLs, DeepEquals, DDLs2)
		c.Assert(cols, DeepEquals, []string{})
		c.Assert(l.versions, DeepEquals, vers)

		// try sync for another table, from `NOT NULL` to `NULL`, DDLs, returned.
		info = newInfoWithVersion(task, source, db, tbls[1], downSchema, downTable, DDLs2, ti1, []*model.TableInfo{ti2}, vers)
		DDLs, cols, err = l.TrySync(info, tts)
		c.Assert(err, IsNil)
		c.Assert(DDLs, DeepEquals, DDLs2)
		c.Assert(cols, DeepEquals, []string{})
		c.Assert(l.versions, DeepEquals, vers)
	}
}

func (t *testLock) TestLockTrySyncIntBigint(c *C) {
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
		ti0              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT NOT NULL DEFAULT 1234)`)
		ti1              = createTableInfo(c, p, se, tblID,
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
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)

	// try sync for one table, from `INT` to `BIGINT`, DDLs returned.
	info := newInfoWithVersion(task, source, db, tbls[0], downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1}, vers)
	DDLs, cols, err := l.TrySync(info, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs1)
	c.Assert(cols, DeepEquals, []string{})
	c.Assert(l.versions, DeepEquals, vers)

	// try sync for another table, DDLs returned.
	info = newInfoWithVersion(task, source, db, tbls[1], downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs1)
	c.Assert(cols, DeepEquals, []string{})
	c.Assert(l.versions, DeepEquals, vers)
}

func (t *testLock) TestLockTrySyncNoDiff(c *C) {
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
		ti0              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT)`)
		ti1              = createTableInfo(c, p, se, tblID,
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
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)

	// try sync for one table.
	info := newInfoWithVersion(task, source, db, tbls[0], downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1}, vers)
	DDLs, cols, err := l.TrySync(info, tts)
	c.Assert(terror.ErrShardDDLOptimismNeedSkipAndRedirect.Equal(err), IsTrue)
	c.Assert(DDLs, DeepEquals, []string{})
	c.Assert(cols, DeepEquals, []string{})
	c.Assert(l.versions, DeepEquals, vers)
}

func (t *testLock) TestLockTrySyncNewTable(c *C) {
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
		ti0              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY)`)
		ti1              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT)`)

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
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)

	// TrySync for a new table as the caller.
	info := newInfoWithVersion(task, source2, db2, tbl2, downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1}, vers)
	DDLs, cols, err := l.TrySync(info, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs1)
	c.Assert(cols, DeepEquals, []string{})
	c.Assert(l.versions, DeepEquals, vers)

	ready := l.Ready()
	c.Assert(ready, HasLen, 2)
	c.Assert(ready[source1], HasLen, 1)
	c.Assert(ready[source1][db1], HasLen, 1)
	c.Assert(ready[source1][db1][tbl1], IsFalse)
	c.Assert(ready[source2], HasLen, 1)
	c.Assert(ready[source2][db2], HasLen, 1)
	c.Assert(ready[source2][db2][tbl2], IsTrue)

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
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs1)
	c.Assert(cols, DeepEquals, []string{})
	c.Assert(l.versions, DeepEquals, vers)

	ready = l.Ready()
	c.Assert(ready, HasLen, 2)
	c.Assert(ready[source1], HasLen, 1)
	c.Assert(ready[source1][db1], HasLen, 2)
	c.Assert(ready[source1][db1][tbl1], IsTrue)
	c.Assert(ready[source1][db1][tbl2], IsFalse) // new table use ti0 as init table
	c.Assert(ready[source2], HasLen, 1)
	c.Assert(ready[source2][db2], HasLen, 2)
	c.Assert(ready[source2][db2][tbl1], IsFalse)
	c.Assert(ready[source2][db2][tbl2], IsTrue)

	info = newInfoWithVersion(task, source1, db1, tbl2, downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs1)
	c.Assert(cols, DeepEquals, []string{})
	c.Assert(l.versions, DeepEquals, vers)

	ready = l.Ready()
	c.Assert(ready, HasLen, 2)
	c.Assert(ready[source1], HasLen, 1)
	c.Assert(ready[source1][db1], HasLen, 2)
	c.Assert(ready[source1][db1][tbl1], IsTrue)
	c.Assert(ready[source1][db1][tbl2], IsTrue)
	c.Assert(ready[source2], HasLen, 1)
	c.Assert(ready[source2][db2], HasLen, 2)
	c.Assert(ready[source2][db2][tbl1], IsFalse)
	c.Assert(ready[source2][db2][tbl2], IsTrue)
}

func (t *testLock) TestLockTrySyncRevert(c *C) {
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
		ti0   = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY)`)
		ti1   = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 TEXT)`)
		ti2   = ti0

		DDLs3 = []string{"ALTER TABLE bar ADD COLUMN c1 TEXT", "ALTER TABLE bar ADD COLUMN c2 INT"}
		DDLs4 = []string{"ALTER TABLE bar DROP COLUMN c2"}
		DDLs5 = []string{"ALTER TABLE bar DROP COLUMN c1"}
		ti3   = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 TEXT, c2 INT)`)
		ti4   = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 TEXT)`)
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
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)

	// CASE: revert for single DDL.
	// TrySync for one table.
	info := newInfoWithVersion(task, source, db, tbls[0], downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1}, vers)
	DDLs, cols, err := l.TrySync(info, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs1)
	c.Assert(cols, DeepEquals, []string{})
	c.Assert(l.versions, DeepEquals, vers)
	ready := l.Ready()
	c.Assert(ready[source][db][tbls[0]], IsTrue)
	c.Assert(ready[source][db][tbls[1]], IsFalse)

	joined, err := l.Joined()
	c.Assert(err, IsNil)
	cmp, err := l.tables[source][db][tbls[0]].Compare(joined)
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)
	cmp, err = l.tables[source][db][tbls[1]].Compare(joined)
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, -1)

	// revert for the table, become synced again.
	info = newInfoWithVersion(task, source, db, tbls[0], downSchema, downTable, DDLs2, ti1, []*model.TableInfo{ti2}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs2)
	c.Assert(cols, DeepEquals, []string{"c1"})
	c.Assert(l.versions, DeepEquals, vers)
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)

	// Simulate watch done operation from dm-worker
	op := NewOperation(utils.GenDDLLockID(task, downSchema, downTable), task, source, db, tbls[0], DDLs2, ConflictNone, "", true, []string{"c1"})
	c.Assert(l.DeleteColumnsByOp(op), IsNil)

	// CASE: revert for multiple DDLs.
	// TrySync for one table.
	info = newInfoWithVersion(task, source, db, tbls[0], downSchema, downTable, DDLs3, ti0, []*model.TableInfo{ti4, ti3}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs3)
	c.Assert(cols, DeepEquals, []string{})
	c.Assert(l.versions, DeepEquals, vers)
	ready = l.Ready()
	c.Assert(ready[source][db][tbls[0]], IsTrue)
	c.Assert(ready[source][db][tbls[1]], IsFalse)
	joined, err = l.Joined()
	c.Assert(err, IsNil)
	cmp, err = l.tables[source][db][tbls[0]].Compare(joined)
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)
	cmp, err = l.tables[source][db][tbls[1]].Compare(joined)
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, -1)

	// revert part of the DDLs.
	info = newInfoWithVersion(task, source, db, tbls[0], downSchema, downTable, DDLs4, ti3, []*model.TableInfo{ti4}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs4)
	c.Assert(cols, DeepEquals, []string{"c2"})
	c.Assert(l.versions, DeepEquals, vers)
	ready = l.Ready()
	c.Assert(ready[source][db][tbls[1]], IsFalse)
	joined, err = l.Joined()
	c.Assert(err, IsNil)
	cmp, err = l.tables[source][db][tbls[0]].Compare(joined)
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)
	cmp, err = l.tables[source][db][tbls[1]].Compare(joined)
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, -1)

	// revert the reset part of the DDLs.
	info = newInfoWithVersion(task, source, db, tbls[0], downSchema, downTable, DDLs5, ti4, []*model.TableInfo{ti5}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs5)
	c.Assert(cols, DeepEquals, []string{"c1"})
	c.Assert(l.versions, DeepEquals, vers)
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)

	// Simulate watch done operation from dm-worker
	op = NewOperation(utils.GenDDLLockID(task, downSchema, downTable), task, source, db, tbls[0], DDLs4, ConflictNone, "", true, []string{"c2"})
	c.Assert(l.DeleteColumnsByOp(op), IsNil)
	op = NewOperation(utils.GenDDLLockID(task, downSchema, downTable), task, source, db, tbls[0], DDLs5, ConflictNone, "", true, []string{"c1"})
	c.Assert(l.DeleteColumnsByOp(op), IsNil)

	// CASE: revert part of multiple DDLs.
	// TrySync for one table.
	info = newInfoWithVersion(task, source, db, tbls[0], downSchema, downTable, DDLs6, ti0, []*model.TableInfo{ti7, ti6}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs6)
	c.Assert(cols, DeepEquals, []string{})
	c.Assert(l.versions, DeepEquals, vers)
	ready = l.Ready()
	c.Assert(ready[source][db][tbls[1]], IsFalse)

	joined, err = l.Joined()
	c.Assert(err, IsNil)
	cmp, err = l.tables[source][db][tbls[0]].Compare(joined)
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)
	cmp, err = l.tables[source][db][tbls[1]].Compare(joined)
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, -1)

	// revert part of the DDLs.
	info = newInfoWithVersion(task, source, db, tbls[0], downSchema, downTable, DDLs7, ti3, []*model.TableInfo{ti7}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs7)
	c.Assert(cols, DeepEquals, []string{"c2"})
	c.Assert(l.versions, DeepEquals, vers)
	ready = l.Ready()
	c.Assert(ready[source][db][tbls[1]], IsFalse)

	joined, err = l.Joined()
	c.Assert(err, IsNil)
	cmp, err = l.tables[source][db][tbls[0]].Compare(joined)
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)
	cmp, err = l.tables[source][db][tbls[1]].Compare(joined)
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, -1)

	// TrySync for another table.
	info = newInfoWithVersion(task, source, db, tbls[1], downSchema, downTable, DDLs8, ti0, []*model.TableInfo{ti8}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs8)
	c.Assert(cols, DeepEquals, []string{})
	c.Assert(l.versions, DeepEquals, vers)
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)
}

func (t *testLock) TestLockTrySyncConflictNonIntrusive(c *C) {
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
		ti0              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY)`)
		ti1              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 TEXT)`)
		ti2              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 DATETIME, c2 INT)`)
		ti2_1            = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 DATETIME)`)
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
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)

	// TrySync for the first table, construct the joined schema.
	info := newInfoWithVersion(task, source, db, tbls[0], downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1}, vers)
	DDLs, cols, err := l.TrySync(info, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs1)
	c.Assert(cols, DeepEquals, []string{})
	c.Assert(l.versions, DeepEquals, vers)
	ready := l.Ready()
	c.Assert(ready[source][db][tbls[0]], IsTrue)
	c.Assert(ready[source][db][tbls[1]], IsFalse)
	joined, err := l.Joined()
	c.Assert(err, IsNil)
	cmp, err := l.tables[source][db][tbls[0]].Compare(joined)
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)
	cmp, err = l.tables[source][db][tbls[1]].Compare(joined)
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, -1)

	// TrySync for the second table with another schema (add two columns, one of them will cause conflict).
	info = newInfoWithVersion(task, source, db, tbls[1], downSchema, downTable, DDLs2, ti0, []*model.TableInfo{ti2_1, ti2}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	c.Assert(terror.ErrShardDDLOptimismTrySyncFail.Equal(err), IsTrue)
	c.Assert(DDLs, DeepEquals, []string{})
	c.Assert(cols, DeepEquals, []string{})
	c.Assert(l.versions, DeepEquals, vers)
	joined, err = l.Joined()
	c.Assert(err, IsNil)
	cmp, err = l.tables[source][db][tbls[1]].Compare(joined)
	// join table isn't updated
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, -1)
	ready = l.Ready()
	c.Assert(ready[source][db][tbls[1]], IsFalse)

	// TrySync for the first table to resolve the conflict.
	info = newInfoWithVersion(task, source, db, tbls[0], downSchema, downTable, DDLs3, ti1, []*model.TableInfo{ti3}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs3)
	c.Assert(cols, DeepEquals, []string{"c1"})
	c.Assert(l.versions, DeepEquals, vers)
	ready = l.Ready() // all table ready
	c.Assert(ready[source][db][tbls[0]], IsTrue)
	c.Assert(ready[source][db][tbls[1]], IsTrue)
	joined, err = l.Joined()
	c.Assert(err, IsNil)
	cmp, err = l.tables[source][db][tbls[0]].Compare(joined)
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)
	cmp, err = l.tables[source][db][tbls[1]].Compare(joined)
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)

	// TrySync for the second table, succeed now
	info = newInfoWithVersion(task, source, db, tbls[1], downSchema, downTable, DDLs2, ti0, []*model.TableInfo{ti2_1, ti2}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs2)
	c.Assert(cols, DeepEquals, []string{})
	c.Assert(l.versions, DeepEquals, vers)
	joined, err = l.Joined()
	c.Assert(err, IsNil)
	cmp, err = l.tables[source][db][tbls[1]].Compare(joined)
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)
	ready = l.Ready()
	c.Assert(ready[source][db][tbls[1]], IsTrue)

	// Simulate watch done operation from dm-worker
	op := NewOperation(utils.GenDDLLockID(task, downSchema, downTable), task, source, db, tbls[0], DDLs3, ConflictNone, "", true, []string{"c1"})
	c.Assert(l.DeleteColumnsByOp(op), IsNil)

	// TrySync for the first table.
	info = newInfoWithVersion(task, source, db, tbls[0], downSchema, downTable, DDLs4, ti0, []*model.TableInfo{ti4_1, ti4}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs4)
	c.Assert(cols, DeepEquals, []string{})
	c.Assert(l.versions, DeepEquals, vers)
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)
}

func (t *testLock) TestLockTrySyncConflictIntrusive(c *C) {
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
		ti0              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY)`)
		ti1              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 TEXT)`)
		ti2              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 DATETIME, c2 INT)`)
		ti3              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 DATETIME)`)

		DDLs5   = []string{"ALTER TABLE bar ADD COLUMN c2 TEXT"}
		DDLs6   = []string{"ALTER TABLE bar ADD COLUMN c2 DATETIME", "ALTER TABLE bar ADD COLUMN c3 INT"}
		DDLs7   = []string{"ALTER TABLE bar ADD COLUMN c3 INT"}
		DDLs8_1 = DDLs7
		DDLs8_2 = DDLs5
		ti5     = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 TEXT, c2 TEXT)`)
		ti6     = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 TEXT, c2 DATETIME, c3 INT)`)
		ti6_1   = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 TEXT, c2 DATETIME)`)
		ti7     = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 TEXT, c3 INT)`)
		ti8     = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 TEXT, c2 TEXT, c3 INT)`)

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
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)

	// CASE: conflict happen, revert all changes to resolve the conflict.
	// TrySync for the first table, construct the joined schema.
	info := newInfoWithVersion(task, source, db, tbls[0], downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1}, vers)
	DDLs, cols, err := l.TrySync(info, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs1)
	c.Assert(cols, DeepEquals, []string{})
	c.Assert(l.versions, DeepEquals, vers)
	ready := l.Ready()
	c.Assert(ready[source][db][tbls[0]], IsTrue)
	c.Assert(ready[source][db][tbls[1]], IsFalse)
	joined, err := l.Joined()
	c.Assert(err, IsNil)
	cmp, err := l.tables[source][db][tbls[0]].Compare(joined)
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)
	cmp, err = l.tables[source][db][tbls[1]].Compare(joined)
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, -1)

	// TrySync for the second table with another schema (add two columns, one of them will cause conflict).
	info = newInfoWithVersion(task, source, db, tbls[1], downSchema, downTable, DDLs2, ti0, []*model.TableInfo{ti3, ti2}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	c.Assert(terror.ErrShardDDLOptimismTrySyncFail.Equal(err), IsTrue)
	c.Assert(DDLs, DeepEquals, []string{})
	c.Assert(cols, DeepEquals, []string{})
	c.Assert(l.versions, DeepEquals, vers)
	joined, err = l.Joined()
	c.Assert(err, IsNil)
	cmp, err = l.tables[source][db][tbls[1]].Compare(joined)
	// join table isn't updated
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, -1)
	ready = l.Ready()
	c.Assert(ready[source][db][tbls[1]], IsFalse)

	// TrySync again.
	info = newInfoWithVersion(task, source, db, tbls[1], downSchema, downTable, DDLs2, ti0, []*model.TableInfo{ti3, ti2}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	c.Assert(terror.ErrShardDDLOptimismTrySyncFail.Equal(err), IsTrue)
	c.Assert(DDLs, DeepEquals, []string{})
	c.Assert(cols, DeepEquals, []string{})
	c.Assert(l.versions, DeepEquals, vers)
	joined, err = l.Joined()
	c.Assert(err, IsNil)
	cmp, err = l.tables[source][db][tbls[1]].Compare(joined)
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, -1)

	// TrySync for the second table to replace a new ddl without non-conflict column, the conflict should still exist.
	info = newInfoWithVersion(task, source, db, tbls[1], downSchema, downTable, DDLs3, ti0, []*model.TableInfo{ti3}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	c.Assert(terror.ErrShardDDLOptimismTrySyncFail.Equal(err), IsTrue)
	c.Assert(DDLs, DeepEquals, []string{})
	c.Assert(cols, DeepEquals, []string{})
	c.Assert(l.versions, DeepEquals, vers)
	joined, err = l.Joined()
	c.Assert(err, IsNil)
	cmp, err = l.tables[source][db][tbls[1]].Compare(joined)
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, -1)
	ready = l.Ready()
	c.Assert(ready[source][db][tbls[1]], IsFalse)

	// TrySync for the second table as we did for the first table, the lock should be synced.
	info = newInfoWithVersion(task, source, db, tbls[1], downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs)
	c.Assert(cols, DeepEquals, []string{})
	c.Assert(l.versions, DeepEquals, vers)
	joined, err = l.Joined()
	c.Assert(err, IsNil)
	cmp, err = l.tables[source][db][tbls[1]].Compare(joined)
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)

	// CASE: conflict happen, revert part of changes to resolve the conflict.
	// TrySync for the first table, construct the joined schema.
	info = newInfoWithVersion(task, source, db, tbls[0], downSchema, downTable, DDLs5, ti1, []*model.TableInfo{ti5}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs5)
	c.Assert(cols, DeepEquals, []string{})
	c.Assert(l.versions, DeepEquals, vers)
	ready = l.Ready()
	c.Assert(ready[source][db][tbls[0]], IsTrue)
	c.Assert(ready[source][db][tbls[1]], IsFalse)
	joined, err = l.Joined()
	c.Assert(err, IsNil)
	cmp, err = l.tables[source][db][tbls[0]].Compare(joined)
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)
	cmp, err = l.tables[source][db][tbls[1]].Compare(joined)
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, -1)

	// TrySync for the second table with another schema (add two columns, one of them will cause conflict).
	info = newInfoWithVersion(task, source, db, tbls[1], downSchema, downTable, DDLs6, ti1, []*model.TableInfo{ti6_1, ti6}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	c.Assert(terror.ErrShardDDLOptimismTrySyncFail.Equal(err), IsTrue)
	c.Assert(DDLs, DeepEquals, []string{})
	c.Assert(cols, DeepEquals, []string{})
	joined, err = l.Joined()
	c.Assert(err, IsNil)
	cmp, err = l.tables[source][db][tbls[1]].Compare(joined)
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, -1)
	c.Assert(l.versions, DeepEquals, vers)
	ready = l.Ready()
	c.Assert(ready[source][db][tbls[1]], IsFalse)

	// TrySync for the second table to replace a new ddl without conflict column, the conflict should be resolved.
	// but both of tables are not synced now.
	info = newInfoWithVersion(task, source, db, tbls[1], downSchema, downTable, DDLs7, ti1, []*model.TableInfo{ti7}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs7)
	c.Assert(cols, DeepEquals, []string{})
	c.Assert(l.versions, DeepEquals, vers)
	ready = l.Ready()
	c.Assert(ready[source][db][tbls[0]], IsFalse)
	c.Assert(ready[source][db][tbls[1]], IsFalse)
	joined, err = l.Joined()
	c.Assert(err, IsNil)
	cmp, err = l.tables[source][db][tbls[0]].Compare(joined)
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, -1)
	cmp, err = l.tables[source][db][tbls[1]].Compare(joined)
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, -1)

	// TrySync for the first table to become synced.
	info = newInfoWithVersion(task, source, db, tbls[0], downSchema, downTable, DDLs8_1, ti5, []*model.TableInfo{ti8}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs8_1)
	c.Assert(cols, DeepEquals, []string{})
	ready = l.Ready()
	c.Assert(ready[source][db][tbls[0]], IsTrue)

	// TrySync for the second table to become synced.
	info = newInfoWithVersion(task, source, db, tbls[1], downSchema, downTable, DDLs8_2, ti7, []*model.TableInfo{ti8}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs8_2)
	c.Assert(cols, DeepEquals, []string{})
	ready = l.Ready()
	c.Assert(ready[source][db][tbls[1]], IsTrue)

	// all tables synced now.
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)
}

func (t *testLock) TestLockTrySyncMultipleChangeDDL(c *C) {
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
		ti0   = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT)`)
		ti1_1 = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT, c2 INT)`)
		ti1   = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c2 INT)`)
		ti2   = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c3 TEXT)`)
		ti2_1 = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY)`)
		//		ti3              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT, c2 BIGINT)`)
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
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)

	// inconsistent ddls and table infos
	info := newInfoWithVersion(task, sources[0], dbs[0], tbls[0], downSchema, downTable, DDLs1[:1], ti0, []*model.TableInfo{ti1_1, ti1}, vers)
	DDLs, cols, err := l.TrySync(info, tts)
	c.Assert(DDLs, DeepEquals, []string{})
	c.Assert(cols, DeepEquals, []string{})
	c.Assert(terror.ErrMasterInconsistentOptimisticDDLsAndInfo.Equal(err), IsTrue)

	info = newInfoWithVersion(task, sources[0], dbs[0], tbls[0], downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	c.Assert(DDLs, DeepEquals, []string{})
	c.Assert(cols, DeepEquals, []string{})
	c.Assert(terror.ErrMasterInconsistentOptimisticDDLsAndInfo.Equal(err), IsTrue)

	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)

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
				c.Assert(err, IsNil)
				c.Assert(DDLs, DeepEquals, resultDDLs1[source][db][tbl])
				c.Assert(cols, DeepEquals, []string{"c1"})
				c.Assert(l.versions, DeepEquals, vers)

				syncedCount++
				synced, _ := l.IsSynced()
				c.Assert(synced, Equals, syncedCount == tableCount)
				c.Assert(synced, Equals, l.synced)
			}
		}
	}
	// synced again after all tables applied the DDL.
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)

	// CASE: TrySync again after synced is idempotent.
	// both ddl will sync again
	info = newInfoWithVersion(task, sources[0], dbs[0], tbls[0], downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1_1, ti1}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs1)
	c.Assert(cols, DeepEquals, []string{"c1"})
	c.Assert(l.versions, DeepEquals, vers)
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)

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
				c.Assert(err, IsNil)
				c.Assert(DDLs, DeepEquals, resultDDLs2[source][db][tbl])
				c.Assert(cols, DeepEquals, []string{"c2"})
				c.Assert(l.versions, DeepEquals, vers)

				syncedCount++
				synced, _ := l.IsSynced()
				c.Assert(synced, Equals, syncedCount == tableCount)
				c.Assert(synced, Equals, l.synced)
			}
		}
	}
	// synced again after all tables applied the DDL.
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)

	// CASE: TrySync again after synced is idempotent.
	info = newInfoWithVersion(task, sources[0], dbs[0], tbls[0], downSchema, downTable, DDLs2, ti1, []*model.TableInfo{ti2_1, ti2}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs2)
	c.Assert(cols, DeepEquals, []string{"c2"})
	c.Assert(l.versions, DeepEquals, vers)
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)
}

func (t *testLock) TestTryRemoveTable(c *C) {
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
		ti0              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY)`)
		ti1              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT)`)
		ti2              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT, c2 INT)`)

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
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)

	// CASE: remove a table as normal.
	// TrySync for the first table.
	info := newInfoWithVersion(task, source, db, tbl1, downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1}, vers)
	DDLs, cols, err := l.TrySync(info, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs1)
	c.Assert(cols, DeepEquals, []string{})
	c.Assert(l.versions, DeepEquals, vers)
	ready := l.Ready()
	c.Assert(ready, HasLen, 1)
	c.Assert(ready[source], HasLen, 1)
	c.Assert(ready[source][db], HasLen, 2)
	c.Assert(ready[source][db][tbl1], IsTrue)
	c.Assert(ready[source][db][tbl2], IsFalse)

	// TryRemoveTable for the second table.
	l.columns = map[string]map[string]map[string]map[string]DropColumnStage{
		"col": {
			source: {db: {tbl2: DropNotDone}},
		},
	}
	col := l.TryRemoveTable(source, db, tbl2)
	c.Assert(col, DeepEquals, []string{"col"})
	delete(vers[source][db], tbl2)
	ready = l.Ready()
	c.Assert(ready, HasLen, 1)
	c.Assert(ready[source], HasLen, 1)
	c.Assert(ready[source][db], HasLen, 1)
	c.Assert(ready[source][db][tbl1], IsTrue)
	c.Assert(l.versions, DeepEquals, vers)

	// CASE: remove a table will rebuild joined schema now.
	// TrySync to add the second back.
	vers[source][db][tbl2] = 0
	info = newInfoWithVersion(task, source, db, tbl2, downSchema, downTable, DDLs2, ti1, []*model.TableInfo{ti2}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs2)
	c.Assert(cols, DeepEquals, []string{})
	c.Assert(l.versions, DeepEquals, vers)
	ready = l.Ready()
	c.Assert(ready, HasLen, 1)
	c.Assert(ready[source], HasLen, 1)
	c.Assert(ready[source][db], HasLen, 2)
	c.Assert(ready[source][db][tbl1], IsFalse)
	c.Assert(ready[source][db][tbl2], IsTrue)

	// TryRemoveTable for the second table.
	c.Assert(l.TryRemoveTable(source, db, tbl2), HasLen, 0)
	delete(vers[source][db], tbl2)
	ready = l.Ready()
	c.Assert(ready, HasLen, 1)
	c.Assert(ready[source], HasLen, 1)
	c.Assert(ready[source][db], HasLen, 1)
	c.Assert(ready[source][db][tbl1], IsTrue) // the joined schema is rebuild.
	c.Assert(l.versions, DeepEquals, vers)

	// CASE: try to remove for not-exists table.
	c.Assert(l.TryRemoveTable(source, db, "not-exist"), HasLen, 0)
	c.Assert(l.TryRemoveTable(source, "not-exist", tbl1), HasLen, 0)
	c.Assert(l.TryRemoveTable("not-exist", db, tbl1), HasLen, 0)
}

func (t *testLock) TestTryRemoveTableWithSources(c *C) {
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
		ti0              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT)`)
		ti1              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY)`)

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
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)

	// TrySync for the first table.
	info := newInfoWithVersion(task, source1, db, tbl1, downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1}, vers)
	DDLs, cols, err := l.TrySync(info, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, []string{})
	c.Assert(cols, DeepEquals, []string{"c1"})
	c.Assert(l.versions, DeepEquals, vers)
	ready := l.Ready()
	c.Assert(ready, HasLen, 2)
	c.Assert(ready[source1], HasLen, 1)
	c.Assert(ready[source1][db], HasLen, 2)
	c.Assert(ready[source1][db][tbl1], IsFalse)
	c.Assert(ready[source1][db][tbl2], IsTrue)
	c.Assert(ready[source2], HasLen, 1)
	c.Assert(ready[source2][db], HasLen, 2)
	c.Assert(ready[source2][db][tbl1], IsTrue)
	c.Assert(ready[source2][db][tbl2], IsTrue)

	// TryRemoveTableBySources with nil
	c.Assert(len(l.TryRemoveTableBySources(nil)), Equals, 0)
	ready = l.Ready()
	c.Assert(ready, HasLen, 2)

	// TryRemoveTableBySources with wrong source
	tts = tts[:1]
	c.Assert(len(l.TryRemoveTableBySources([]string{"hahaha"})), Equals, 0)
	ready = l.Ready()
	c.Assert(ready, HasLen, 2)

	// TryRemoveTableBySources with source2
	c.Assert(len(l.TryRemoveTableBySources([]string{source2})), Equals, 0)
	ready = l.Ready()
	c.Assert(ready, HasLen, 1)
	c.Assert(ready[source1], HasLen, 1)
	c.Assert(ready[source1][db], HasLen, 2)
	c.Assert(ready[source1][db][tbl1], IsFalse)
	c.Assert(ready[source1][db][tbl2], IsTrue)
	delete(vers, source2)
	c.Assert(l.versions, DeepEquals, vers)
	c.Assert(l.HasTables(), IsTrue)

	// TrySync with second table
	info = newInfoWithVersion(task, source1, db, tbl2, downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs1)
	c.Assert(cols, DeepEquals, []string{"c1"})
	c.Assert(l.versions, DeepEquals, vers)
	ready = l.Ready()
	c.Assert(ready, HasLen, 1)
	c.Assert(ready[source1], HasLen, 1)
	c.Assert(ready[source1][db], HasLen, 2)
	c.Assert(ready[source1][db][tbl1], IsTrue)
	c.Assert(ready[source1][db][tbl2], IsTrue)

	// TryRemoveTableBySources with source1,source2
	cols = l.TryRemoveTableBySources([]string{source1})
	c.Assert(cols, DeepEquals, []string{"c1"})
	c.Assert(l.HasTables(), IsFalse)
}

func (t *testLock) TestLockTryMarkDone(c *C) {
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
		ti0              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY)`)
		ti1              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT)`)
		ti2              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT, c2 INT)`)
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
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)
	c.Assert(l.IsResolved(), IsFalse)

	// TrySync for the first table, no table has done the DDLs operation.
	info := newInfoWithVersion(task, source, db, tbls[0], downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1}, vers)
	DDLs, cols, err := l.TrySync(info, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs1)
	c.Assert(cols, DeepEquals, []string{})
	c.Assert(l.versions, DeepEquals, vers)
	t.checkLockNoDone(c, l)
	c.Assert(l.IsResolved(), IsFalse)

	// mark done for the synced table, the lock is un-resolved.
	c.Assert(l.TryMarkDone(source, db, tbls[0]), IsTrue)
	c.Assert(l.IsDone(source, db, tbls[0]), IsTrue)
	c.Assert(l.IsDone(source, db, tbls[1]), IsFalse)
	c.Assert(l.IsResolved(), IsFalse)

	// TrySync for the second table, the joined schema become larger.
	info = newInfoWithVersion(task, source, db, tbls[1], downSchema, downTable, DDLs2, ti0, []*model.TableInfo{ti1, ti2}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs2)
	c.Assert(cols, DeepEquals, []string{})
	c.Assert(l.versions, DeepEquals, vers)

	// the first table is still keep `done` (for the previous DDLs operation)
	c.Assert(l.IsDone(source, db, tbls[0]), IsTrue)
	c.Assert(l.IsDone(source, db, tbls[1]), IsFalse)
	c.Assert(l.IsResolved(), IsFalse)

	// mark done for the second table, both of them are done (for different DDLs operations)
	c.Assert(l.TryMarkDone(source, db, tbls[1]), IsTrue)
	c.Assert(l.IsDone(source, db, tbls[0]), IsTrue)
	c.Assert(l.IsDone(source, db, tbls[1]), IsTrue)
	// but the lock is still not resolved because tables have different schemas.
	c.Assert(l.IsResolved(), IsFalse)

	// TrySync for the first table, all tables become synced.
	info = newInfoWithVersion(task, source, db, tbls[0], downSchema, downTable, DDLs3, ti1, []*model.TableInfo{ti3}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs3)
	c.Assert(cols, DeepEquals, []string{})
	c.Assert(l.versions, DeepEquals, vers)

	// the first table become not-done, and the lock is un-resolved.
	c.Assert(l.IsDone(source, db, tbls[0]), IsFalse)
	c.Assert(l.IsDone(source, db, tbls[1]), IsTrue)
	c.Assert(l.IsResolved(), IsFalse)

	// mark done for the first table.
	c.Assert(l.TryMarkDone(source, db, tbls[0]), IsTrue)
	c.Assert(l.IsDone(source, db, tbls[0]), IsTrue)
	c.Assert(l.IsDone(source, db, tbls[1]), IsTrue)

	// the lock become resolved now.
	c.Assert(l.IsResolved(), IsTrue)

	// TryMarkDone for not-existing table take no effect.
	c.Assert(l.TryMarkDone(source, db, "not-exist"), IsFalse)
	c.Assert(l.TryMarkDone(source, "not-exist", tbls[0]), IsFalse)
	c.Assert(l.TryMarkDone("not-exist", db, tbls[0]), IsFalse)

	// check IsDone for not-existing table take no effect.
	c.Assert(l.IsDone(source, db, "not-exist"), IsFalse)
	c.Assert(l.IsDone(source, "not-exist", tbls[0]), IsFalse)
	c.Assert(l.IsDone("not-exist", db, tbls[0]), IsFalse)
}

func (t *testLock) TestAddDifferentFieldLenColumns(c *C) {
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
		ti0         = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY)`)
		ti1         = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 VARCHAR(4))`)
		ti2         = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 VARCHAR(5))`)

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
	c.Assert(col, Equals, "c1")
	c.Assert(err, IsNil)
	col, err = AddDifferentFieldLenColumns(ID, DDLs2[0], table2, table3)
	c.Assert(col, Equals, "c1")
	c.Assert(err, ErrorMatches, ".*add columns with different field lengths.*")
	col, err = AddDifferentFieldLenColumns(ID, DDLs1[0], table3, table2)
	c.Assert(col, Equals, "c1")
	c.Assert(err, ErrorMatches, ".*add columns with different field lengths.*")

	// the initial status is synced but not resolved.
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)
	c.Assert(l.IsResolved(), IsFalse)

	// TrySync for the first table, no table has done the DDLs operation.
	info := newInfoWithVersion(task, source, db, tbls[0], downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1}, vers)
	DDLs, cols, err := l.TrySync(info, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs1)
	c.Assert(cols, DeepEquals, []string{})
	c.Assert(l.versions, DeepEquals, vers)
	t.checkLockNoDone(c, l)
	c.Assert(l.IsResolved(), IsFalse)

	// TrySync for the second table, add a table with a larger field length
	info = newInfoWithVersion(task, source, db, tbls[1], downSchema, downTable, DDLs2, ti0, []*model.TableInfo{ti2}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	c.Assert(err, ErrorMatches, ".*add columns with different field lengths.*")
	c.Assert(DDLs, DeepEquals, []string{})
	c.Assert(cols, DeepEquals, []string{})
	c.Assert(l.versions, DeepEquals, vers)

	// case 2: add a column with a smaller field length
	l = NewLock(etcdTestCli, ID, task, downSchema, downTable, schemacmp.Encode(ti0), tts, nil)

	// TrySync for the first table, no table has done the DDLs operation.
	vers[source][db][tbls[0]]--
	info = NewInfo(task, source, db, tbls[1], downSchema, downTable, DDLs2, ti0, []*model.TableInfo{ti2})
	info.Version = vers[source][db][tbls[1]]
	DDLs, cols, err = l.TrySync(info, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs2)
	c.Assert(cols, DeepEquals, []string{})
	c.Assert(l.versions, DeepEquals, vers)
	t.checkLockNoDone(c, l)
	c.Assert(l.IsResolved(), IsFalse)

	// TrySync for the second table, add a table with a smaller field length
	info = newInfoWithVersion(task, source, db, tbls[0], downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	c.Assert(err, ErrorMatches, ".*add columns with different field lengths.*")
	c.Assert(DDLs, DeepEquals, []string{})
	c.Assert(cols, DeepEquals, []string{})
	c.Assert(l.versions, DeepEquals, vers)
}

func (t *testLock) TestAddNotFullyDroppedColumns(c *C) {
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
		ti0         = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, b int, c int)`)
		ti1         = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, b int)`)
		ti2         = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY)`)
		ti3         = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c int)`)

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
	c.Assert(col, Equals, "c")
	c.Assert(err, IsNil)
	col, err = GetColumnName(ID, DDLs2[0], ast.AlterTableDropColumn)
	c.Assert(col, Equals, "b")
	c.Assert(err, IsNil)

	// the initial status is synced but not resolved.
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)
	c.Assert(l.IsResolved(), IsFalse)

	// TrySync for the first table, drop column c
	DDLs, cols, err := l.TrySync(newInfoWithVersion(task, source, db, tbls[0], downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1}, vers), tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, []string{})
	c.Assert(cols, DeepEquals, []string{"c"})
	c.Assert(l.versions, DeepEquals, vers)
	c.Assert(l.IsResolved(), IsFalse)

	// TrySync for the first table, drop column b
	DDLs, cols, err = l.TrySync(newInfoWithVersion(task, source, db, tbls[0], downSchema, downTable, DDLs2, ti1, []*model.TableInfo{ti2}, vers), tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, []string{})
	c.Assert(cols, DeepEquals, []string{"b"})
	c.Assert(l.versions, DeepEquals, vers)
	c.Assert(l.IsResolved(), IsFalse)

	colm, _, err := GetAllDroppedColumns(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(colm, DeepEquals, colm1)

	// TrySync for the second table, drop column b, this column should be fully dropped
	DDLs, cols, err = l.TrySync(newInfoWithVersion(task, source, db, tbls[1], downSchema, downTable, DDLs2, ti0, []*model.TableInfo{ti3}, vers), tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs2)
	c.Assert(cols, DeepEquals, []string{"b"})
	c.Assert(l.versions, DeepEquals, vers)
	c.Assert(l.IsResolved(), IsFalse)
	// Simulate watch done operation from dm-worker
	op := NewOperation(utils.GenDDLLockID(task, downSchema, downTable), task, source, db, tbls[1], DDLs2, ConflictNone, "", true, []string{"b"})
	c.Assert(l.DeleteColumnsByOp(op), IsNil)

	colm, _, err = GetAllDroppedColumns(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(colm, DeepEquals, colm2)

	op = NewOperation(utils.GenDDLLockID(task, downSchema, downTable), task, source, db, tbls[0], []string{}, ConflictNone, "", true, []string{"b"})
	c.Assert(l.DeleteColumnsByOp(op), IsNil)

	colm, _, err = GetAllDroppedColumns(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(colm, DeepEquals, colm3)

	// TrySync for the first table, add column b, should succeed, because this column is fully dropped in the downstream
	DDLs, cols, err = l.TrySync(newInfoWithVersion(task, source, db, tbls[0], downSchema, downTable, DDLs3, ti2, []*model.TableInfo{ti1}, vers), tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs3)
	c.Assert(cols, DeepEquals, []string{})
	c.Assert(l.versions, DeepEquals, vers)
	c.Assert(l.IsResolved(), IsFalse)

	// TrySync for the first table, add column c, should fail, because this column isn't fully dropped in the downstream
	_, _, err = l.TrySync(newInfoWithVersion(task, source, db, tbls[0], downSchema, downTable, DDLs4, ti1, []*model.TableInfo{ti0}, vers), tts)
	c.Assert(err, ErrorMatches, ".*add column c that wasn't fully dropped in downstream.*")
	c.Assert(l.IsResolved(), IsFalse)

	// TrySync for the second table, drop column c, this column should be fully dropped
	DDLs, cols, err = l.TrySync(newInfoWithVersion(task, source, db, tbls[1], downSchema, downTable, DDLs1, ti3, []*model.TableInfo{ti2}, vers), tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs1)
	c.Assert(cols, DeepEquals, []string{"c"})
	c.Assert(l.versions, DeepEquals, vers)
	c.Assert(l.IsResolved(), IsFalse)
	// Simulate watch done operation from dm-worker
	op = NewOperation(utils.GenDDLLockID(task, downSchema, downTable), task, source, db, tbls[1], DDLs1, ConflictNone, "", true, []string{"c"})
	c.Assert(l.DeleteColumnsByOp(op), IsNil)

	// TrySync for the second table, add column c, should fail, because this column isn't fully dropped in the downstream
	_, _, err = l.TrySync(newInfoWithVersion(task, source, db, tbls[1], downSchema, downTable, DDLs4, ti1, []*model.TableInfo{ti0}, vers), tts)
	c.Assert(err, ErrorMatches, ".*add column c that wasn't fully dropped in downstream.*")
	c.Assert(l.IsResolved(), IsFalse)

	// Simulate watch done operation from dm-worker
	op = NewOperation(utils.GenDDLLockID(task, downSchema, downTable), task, source, db, tbls[0], []string{}, ConflictNone, "", true, []string{"c"})
	c.Assert(l.DeleteColumnsByOp(op), IsNil)

	// TrySync for the first table, add column c, should succeed, because this column is fully dropped in the downstream
	DDLs, cols, err = l.TrySync(newInfoWithVersion(task, source, db, tbls[0], downSchema, downTable, DDLs4, ti1, []*model.TableInfo{ti0}, vers), tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs4)
	c.Assert(cols, DeepEquals, []string{})
	c.Assert(l.versions, DeepEquals, vers)
	c.Assert(l.IsResolved(), IsFalse)

	_, _, err = l.TrySync(newInfoWithVersion(task, source, db, tbls[0], downSchema, downTable, DDLs5, ti0, []*model.TableInfo{ti1, ti0}, vers), tts)
	c.Assert(err, ErrorMatches, ".*add column c that wasn't fully dropped in downstream.*")
}

func (t *testLock) trySyncForAllTablesLarger(c *C, l *Lock,
	ddls []string, tableInfoBefore *model.TableInfo, tis []*model.TableInfo, tts []TargetTable, vers map[string]map[string]map[string]int64,
) {
	for source, schemaTables := range l.Ready() {
		for schema, tables := range schemaTables {
			for table := range tables {
				info := newInfoWithVersion(l.Task, source, schema, table, l.DownSchema, l.DownTable, ddls, tableInfoBefore, tis, vers)
				DDLs2, cols, err := l.TrySync(info, tts)
				c.Assert(err, IsNil)
				c.Assert(cols, DeepEquals, []string{})
				c.Assert(DDLs2, DeepEquals, ddls)
			}
		}
	}
}

func (t *testLock) checkLockSynced(c *C, l *Lock) {
	synced, remain := l.IsSynced()
	c.Assert(synced, Equals, l.synced)
	c.Assert(synced, IsTrue)
	c.Assert(remain, Equals, 0)

	ready := l.Ready()
	for _, schemaTables := range ready {
		for _, tables := range schemaTables {
			for _, synced := range tables {
				c.Assert(synced, IsTrue)
			}
		}
	}
}

func (t *testLock) checkLockNoDone(c *C, l *Lock) {
	c.Assert(l.IsResolved(), IsFalse)
	for source, schemaTables := range l.Ready() {
		for schema, tables := range schemaTables {
			for table := range tables {
				c.Assert(l.IsDone(source, schema, table), IsFalse)
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

func (t *testLock) TestLockTrySyncDifferentIndex(c *C) {
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
		ti0              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT, UNIQUE INDEX idx_c1(c1))`)
		ti1              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT)`)
		ti2              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT, INDEX new_idx(c1))`)
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
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)

	// try sync for one table, `DROP INDEX` returned directly (to make schema become more compatible).
	// `DROP INDEX` is handled like `ADD COLUMN`.
	info := newInfoWithVersion(task, source, db, tbls[0], downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1}, vers)
	DDLs, cols, err := l.TrySync(info, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs1)
	c.Assert(cols, DeepEquals, []string{})
	c.Assert(l.versions, DeepEquals, vers)
	synced, remain := l.IsSynced()
	c.Assert(synced, Equals, l.synced)
	c.Assert(synced, IsFalse)
	c.Assert(remain, Equals, 1)

	cmp, err := l.tables[source][db][tbls[1]].Compare(schemacmp.Encode(ti0))
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)

	// try sync ADD another INDEX for another table
	// `ADD INDEX` is handled like `DROP COLUMN`.
	info = newInfoWithVersion(task, source, db, tbls[1], downSchema, downTable, DDLs2, ti1, []*model.TableInfo{ti2}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, []string{}) // no DDLs returned
	c.Assert(cols, DeepEquals, []string{})
	c.Assert(l.versions, DeepEquals, vers)
	synced, remain = l.IsSynced()
	c.Assert(synced, Equals, l.synced)
	c.Assert(synced, IsFalse)
	c.Assert(remain, Equals, 1)

	joined, err := l.joinFinalTables()
	c.Assert(err, IsNil)
	cmp, err = l.tables[source][db][tbls[0]].Compare(joined)
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)

	// try sync ADD INDEX for first table
	info = newInfoWithVersion(task, source, db, tbls[0], downSchema, downTable, DDLs2, ti1, []*model.TableInfo{ti2}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs2)
	c.Assert(cols, DeepEquals, []string{})
	c.Assert(l.versions, DeepEquals, vers)
	t.checkLockSynced(c, l)
}

func (t *testLock) TestFetchTableInfo(c *C) {
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
		ti0              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT, UNIQUE INDEX idx_c1(c1))`)
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
	c.Assert(terror.ErrMasterOptimisticDownstreamMetaNotFound.Equal(err), IsTrue)
	c.Assert(ti, IsNil)

	// table info not exist
	l = NewLock(etcdTestCli, ID, task, downSchema, downTable, schemacmp.Encode(ti0), tts, &DownstreamMeta{dbConfig: &dbconfig.DBConfig{}, meta: meta})
	conn.DefaultDBProvider = &conn.DefaultDBProviderImpl{}
	mock := conn.InitMockDB(c)
	mock.ExpectQuery(query).WithArgs(source, schema, tbls[0]).WillReturnRows(sqlmock.NewRows([]string{"table_info"}))
	ti, err = l.FetchTableInfos(task, source, schema, tbls[0])
	c.Assert(terror.ErrDBExecuteFailed.Equal(err), IsTrue)
	c.Assert(ti, IsNil)

	// null table info
	l = NewLock(etcdTestCli, ID, task, downSchema, downTable, schemacmp.Encode(ti0), tts, &DownstreamMeta{dbConfig: &dbconfig.DBConfig{}, meta: meta})
	conn.DefaultDBProvider = &conn.DefaultDBProviderImpl{}
	mock = conn.InitMockDB(c)
	mock.ExpectQuery(query).WithArgs(source, schema, tbls[0]).WillReturnRows(sqlmock.NewRows([]string{"table_info"}).AddRow("null"))
	ti, err = l.FetchTableInfos(task, source, schema, tbls[0])
	c.Assert(terror.ErrMasterOptimisticDownstreamMetaNotFound.Equal(err), IsTrue)
	c.Assert(ti, IsNil)

	// succeed
	tiBytes, err := json.Marshal(ti0)
	c.Assert(err, IsNil)
	conn.DefaultDBProvider = &conn.DefaultDBProviderImpl{}
	mock = conn.InitMockDB(c)
	mock.ExpectQuery(query).WithArgs(source, schema, tbls[0]).WillReturnRows(sqlmock.NewRows([]string{"table_info"}).AddRow(tiBytes))
	ti, err = l.FetchTableInfos(task, source, schema, tbls[0])
	c.Assert(err, IsNil)
	c.Assert(mock.ExpectationsWereMet(), IsNil)
	c.Assert(ti, DeepEquals, ti0)
}

func (t *testLock) TestCheckAddDropColumns(c *C) {
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
		ti0              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, col int)`)
		ti1              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, col int, a VARCHAR(1))`)
		ti2              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, col int, a VARCHAR(2))`)
		ti3              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, a VARCHAR(2))`)
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

	c.Assert(err, IsNil)
	c.Assert(len(col), Equals, 0)

	l.tables[source][db][tbls[0]] = schemacmp.Encode(ti1)
	col, err = l.checkAddDropColumn(source, db, tbls[1], DDLs2, schemacmp.Encode(ti0), schemacmp.Encode(ti2), nil)
	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, ".*add columns with different field lengths.*")
	c.Assert(len(col), Equals, 0)

	col, err = l.checkAddDropColumn(source, db, tbls[0], DDLs3, schemacmp.Encode(ti2), schemacmp.Encode(ti3), nil)
	c.Assert(err, IsNil)
	c.Assert(col, Equals, "col")

	l.columns = map[string]map[string]map[string]map[string]DropColumnStage{
		"col": {
			source: {
				db: {tbls[0]: DropNotDone},
			},
		},
	}

	col, err = l.checkAddDropColumn(source, db, tbls[0], DDLs4, schemacmp.Encode(ti3), schemacmp.Encode(ti2), nil)
	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, ".*add column .* that wasn't fully dropped in downstream.*")
	c.Assert(len(col), Equals, 0)
}

func (t *testLock) TestJoinTables(c *C) {
	var (
		source       = "mysql-replica-1"
		db           = "db"
		tbls         = []string{"bar1", "bar2"}
		p            = parser.New()
		se           = mock.NewContext()
		tblID  int64 = 111
		ti0          = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, col int)`)
		ti1          = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, col int, a VARCHAR(1))`)
		ti2          = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, col varchar(4))`)
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
	c.Assert(err, IsNil)
	cmp, err := joined.Compare(t0)
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)
	joined, err = l.joinFinalTables()
	c.Assert(err, IsNil)
	cmp, err = joined.Compare(t0)
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)
	_, err = l.joinConflictTables()
	c.Assert(err, IsNil)

	l.tables[source][db][tbls[0]] = t1
	l.finalTables[source][db][tbls[0]] = t1

	joined, err = l.joinNormalTables()
	c.Assert(err, IsNil)
	cmp, err = joined.Compare(t1)
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)
	joined, err = l.joinFinalTables()
	c.Assert(err, IsNil)
	cmp, err = joined.Compare(t1)
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)
	_, err = l.joinConflictTables()
	c.Assert(err, IsNil)

	l.tables[source][db][tbls[1]] = t1
	l.finalTables[source][db][tbls[1]] = t1
	l.conflictTables = map[string]map[string]map[string]schemacmp.Table{
		source: {
			db: {tbls[0]: t2},
		},
	}

	joined, err = l.joinNormalTables()
	c.Assert(err, IsNil)
	cmp, err = joined.Compare(t1)
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)
	joined, err = l.joinFinalTables()
	c.Assert(err, IsNil)
	cmp, err = joined.Compare(t1)
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)
	joined, err = l.joinConflictTables()
	c.Assert(err, IsNil)
	cmp, err = joined.Compare(t2)
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)

	l.tables[source][db][tbls[1]] = t2
	_, err = l.joinNormalTables()
	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, ".*incompatible mysql type.*")

	l.resolveTables()
	c.Assert(l.conflictTables, HasLen, 0)
	c.Assert(l.tables, DeepEquals, l.finalTables)
}

func (t *testLock) TestAddRemoveConflictTable(c *C) {
	var (
		source       = "source"
		schema       = "schema"
		table1       = "table1"
		table2       = "table2"
		table3       = "table3"
		p            = parser.New()
		se           = mock.NewContext()
		tblID  int64 = 111
		ti0          = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, col int)`)
		t0           = schemacmp.Encode(ti0)
	)
	l := &Lock{
		conflictTables: make(map[string]map[string]map[string]schemacmp.Table),
	}
	c.Assert(l.conflictTables, HasLen, 0)

	l.addConflictTable(source, schema, table1, t0)
	c.Assert(l.conflictTables, HasLen, 1)
	c.Assert(l.conflictTables[source], HasLen, 1)
	c.Assert(l.conflictTables[source][schema], HasLen, 1)
	tb := l.conflictTables[source][schema][table1]
	cmp, err := tb.Compare(t0)
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)

	l.addConflictTable(source, schema, table1, t0)
	c.Assert(l.conflictTables, HasLen, 1)
	c.Assert(l.conflictTables[source], HasLen, 1)
	c.Assert(l.conflictTables[source][schema], HasLen, 1)

	l.addConflictTable(source, schema, table2, t0)
	c.Assert(l.conflictTables, HasLen, 1)
	c.Assert(l.conflictTables[source], HasLen, 1)
	c.Assert(l.conflictTables[source][schema], HasLen, 2)
	tb = l.conflictTables[source][schema][table2]
	cmp, err = tb.Compare(t0)
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)

	l.removeConflictTable(source, schema, table3)
	c.Assert(l.conflictTables[source][schema], HasLen, 2)

	l.removeConflictTable(source, schema, table1)
	c.Assert(l.conflictTables[source][schema], HasLen, 1)
	tb = l.conflictTables[source][schema][table2]
	cmp, err = tb.Compare(t0)
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)

	l.removeConflictTable(source, schema, table2)
	c.Assert(l.conflictTables, HasLen, 0)
}

func (t *testLock) TestAllTableSmallerLarger(c *C) {
	var (
		source       = "source"
		schema       = "schema"
		table1       = "table1"
		table2       = "table2"
		p            = parser.New()
		se           = mock.NewContext()
		tblID  int64 = 111
		ti0          = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, col int)`)
		ti1          = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, new_col int)`)
		ti2          = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, new_col varchar(4))`)
		ti3          = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (a INT PRIMARY KEY, new_col varchar(4))`)
		ti4          = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, new_col2 varchar(4))`)
		ti5          = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id varchar(4) PRIMARY KEY, new_col int)`)
		ti6          = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, col int, new_col int)`)
		ti7          = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, col int, new_col varchar(4))`)
		ti8          = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, col int, col2 int not null)`)
		ti9          = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (a INT PRIMARY KEY, new_col int)`)
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
	c.Assert(l.allFinalTableSmaller(), IsTrue)

	// rename table
	l.addConflictTable(source, schema, table1, t1)
	l.finalTables[source][schema][table1] = t1
	c.Assert(l.allConflictTableSmaller(), IsTrue)
	c.Assert(l.allConflictTableLarger(), IsTrue)
	c.Assert(l.allFinalTableSmaller(), IsFalse)
	c.Assert(l.allFinalTableLarger(), IsFalse)
	l.addConflictTable(source, schema, table2, t1)
	l.finalTables[source][schema][table2] = t1
	c.Assert(l.allConflictTableSmaller(), IsTrue)
	c.Assert(l.allConflictTableLarger(), IsTrue)
	c.Assert(l.allFinalTableSmaller(), IsTrue)
	c.Assert(l.allFinalTableLarger(), IsTrue)
	// reset
	l.resolveTables()
	c.Assert(l.conflictTables, HasLen, 0)
	c.Assert(l.tables, DeepEquals, l.finalTables)
	c.Assert(l.tables[source][schema], HasLen, 2)

	// modify column
	l.addConflictTable(source, schema, table1, t2)
	l.finalTables[source][schema][table1] = t2
	c.Assert(l.allConflictTableSmaller(), IsTrue)
	c.Assert(l.allConflictTableLarger(), IsTrue)
	c.Assert(l.allFinalTableSmaller(), IsFalse)
	c.Assert(l.allFinalTableLarger(), IsFalse)
	l.addConflictTable(source, schema, table2, t2)
	l.finalTables[source][schema][table2] = t2
	c.Assert(l.allConflictTableSmaller(), IsTrue)
	c.Assert(l.allConflictTableLarger(), IsTrue)
	c.Assert(l.allFinalTableSmaller(), IsTrue)
	c.Assert(l.allFinalTableLarger(), IsTrue)
	// reset
	l.resolveTables()
	c.Assert(l.conflictTables, HasLen, 0)
	c.Assert(l.tables, DeepEquals, l.finalTables)
	c.Assert(l.tables[source][schema], HasLen, 2)
	c.Assert(l.tables[source][schema][table1], DeepEquals, t2)
	c.Assert(l.tables[source][schema][table2], DeepEquals, t2)

	// different rename
	l.addConflictTable(source, schema, table1, t3)
	l.finalTables[source][schema][table1] = t3
	c.Assert(l.allConflictTableSmaller(), IsTrue)
	c.Assert(l.allConflictTableLarger(), IsTrue)
	c.Assert(l.allFinalTableSmaller(), IsFalse)
	c.Assert(l.allFinalTableLarger(), IsFalse)
	l.addConflictTable(source, schema, table2, t4)
	l.finalTables[source][schema][table2] = t4
	c.Assert(l.allConflictTableSmaller(), IsFalse)
	c.Assert(l.allConflictTableLarger(), IsFalse)
	c.Assert(l.allFinalTableSmaller(), IsFalse)
	c.Assert(l.allFinalTableLarger(), IsFalse)
	// reset
	l.finalTables[source][schema][table1] = t1
	l.finalTables[source][schema][table2] = t1
	l.resolveTables()
	c.Assert(l.conflictTables, HasLen, 0)
	c.Assert(l.tables, DeepEquals, l.finalTables)
	c.Assert(l.tables[source][schema], HasLen, 2)
	c.Assert(l.tables[source][schema][table1], DeepEquals, t1)
	c.Assert(l.tables[source][schema][table2], DeepEquals, t1)

	// different modify
	l.addConflictTable(source, schema, table1, t2)
	l.finalTables[source][schema][table1] = t2
	c.Assert(l.allConflictTableSmaller(), IsTrue)
	c.Assert(l.allConflictTableLarger(), IsTrue)
	c.Assert(l.allFinalTableSmaller(), IsFalse)
	c.Assert(l.allFinalTableLarger(), IsFalse)
	l.addConflictTable(source, schema, table2, t5)
	l.finalTables[source][schema][table2] = t5
	c.Assert(l.allConflictTableSmaller(), IsFalse)
	c.Assert(l.allConflictTableLarger(), IsFalse)
	c.Assert(l.allFinalTableSmaller(), IsFalse)
	c.Assert(l.allFinalTableLarger(), IsFalse)
	// reset
	l.finalTables[source][schema][table1] = t1
	l.finalTables[source][schema][table2] = t1
	l.resolveTables()
	c.Assert(l.conflictTables, HasLen, 0)
	c.Assert(l.tables, DeepEquals, l.finalTables)
	c.Assert(l.tables[source][schema], HasLen, 2)
	c.Assert(l.tables[source][schema][table1], DeepEquals, t1)
	c.Assert(l.tables[source][schema][table2], DeepEquals, t1)

	// one table rename, one table modify
	l.addConflictTable(source, schema, table1, t4)
	l.finalTables[source][schema][table1] = t4
	c.Assert(l.allConflictTableSmaller(), IsTrue)
	c.Assert(l.allConflictTableLarger(), IsTrue)
	c.Assert(l.allFinalTableSmaller(), IsFalse)
	c.Assert(l.allFinalTableLarger(), IsFalse)
	l.addConflictTable(source, schema, table2, t5)
	l.finalTables[source][schema][table2] = t5
	c.Assert(l.allConflictTableSmaller(), IsFalse)
	c.Assert(l.allConflictTableLarger(), IsFalse)
	c.Assert(l.allFinalTableSmaller(), IsFalse)
	c.Assert(l.allFinalTableLarger(), IsFalse)
	// reset
	l.finalTables[source][schema][table1] = t0
	l.finalTables[source][schema][table2] = t0
	l.resolveTables()
	c.Assert(l.conflictTables, HasLen, 0)
	c.Assert(l.tables, DeepEquals, l.finalTables)
	c.Assert(l.tables[source][schema], HasLen, 2)
	c.Assert(l.tables[source][schema][table1], DeepEquals, t0)
	c.Assert(l.tables[source][schema][table2], DeepEquals, t0)

	// one table rename, one table add and drop
	l.addConflictTable(source, schema, table1, t1)
	l.finalTables[source][schema][table1] = t1
	c.Assert(l.allConflictTableSmaller(), IsTrue)
	c.Assert(l.allConflictTableLarger(), IsTrue)
	c.Assert(l.allFinalTableSmaller(), IsFalse)
	c.Assert(l.allFinalTableLarger(), IsFalse)
	l.finalTables[source][schema][table2] = t6
	c.Assert(l.allConflictTableSmaller(), IsTrue)
	c.Assert(l.allConflictTableLarger(), IsTrue)
	c.Assert(l.allFinalTableSmaller(), IsFalse)
	c.Assert(l.allFinalTableLarger(), IsTrue)
	l.finalTables[source][schema][table2] = t1
	c.Assert(l.allConflictTableSmaller(), IsTrue)
	c.Assert(l.allConflictTableLarger(), IsTrue)
	c.Assert(l.allFinalTableSmaller(), IsTrue)
	c.Assert(l.allFinalTableLarger(), IsTrue)
	// reset
	l.finalTables[source][schema][table1] = t0
	l.finalTables[source][schema][table2] = t0
	l.resolveTables()
	c.Assert(l.conflictTables, HasLen, 0)
	c.Assert(l.tables, DeepEquals, l.finalTables)
	c.Assert(l.tables[source][schema], HasLen, 2)
	c.Assert(l.tables[source][schema][table1], DeepEquals, t0)
	c.Assert(l.tables[source][schema][table2], DeepEquals, t0)

	// one table modify, one table add and drop
	l.addConflictTable(source, schema, table1, t2)
	l.finalTables[source][schema][table1] = t2
	c.Assert(l.allConflictTableSmaller(), IsTrue)
	c.Assert(l.allConflictTableLarger(), IsTrue)
	c.Assert(l.allFinalTableSmaller(), IsFalse)
	c.Assert(l.allFinalTableLarger(), IsFalse)
	l.finalTables[source][schema][table2] = t7
	c.Assert(l.allConflictTableSmaller(), IsTrue)
	c.Assert(l.allConflictTableLarger(), IsTrue)
	c.Assert(l.allFinalTableSmaller(), IsFalse)
	c.Assert(l.allFinalTableLarger(), IsTrue)
	l.finalTables[source][schema][table2] = t2
	c.Assert(l.allConflictTableSmaller(), IsTrue)
	c.Assert(l.allConflictTableLarger(), IsTrue)
	c.Assert(l.allFinalTableSmaller(), IsTrue)
	c.Assert(l.allFinalTableLarger(), IsTrue)
	// reset
	l.finalTables[source][schema][table1] = t0
	l.finalTables[source][schema][table2] = t0
	l.resolveTables()
	c.Assert(l.conflictTables, HasLen, 0)
	c.Assert(l.tables, DeepEquals, l.finalTables)
	c.Assert(l.tables[source][schema], HasLen, 2)
	c.Assert(l.tables[source][schema][table1], DeepEquals, t0)
	c.Assert(l.tables[source][schema][table2], DeepEquals, t0)

	// not null no default
	l.addConflictTable(source, schema, table1, t8)
	l.finalTables[source][schema][table1] = t8
	c.Assert(l.allConflictTableSmaller(), IsTrue)
	c.Assert(l.allConflictTableLarger(), IsTrue)
	c.Assert(l.allFinalTableSmaller(), IsFalse)
	c.Assert(l.allFinalTableLarger(), IsFalse)
	l.addConflictTable(source, schema, table2, t8)
	l.finalTables[source][schema][table2] = t8
	c.Assert(l.allConflictTableSmaller(), IsTrue)
	c.Assert(l.allConflictTableLarger(), IsTrue)
	c.Assert(l.allFinalTableSmaller(), IsTrue)
	c.Assert(l.allFinalTableLarger(), IsTrue)
	// reset
	l.finalTables[source][schema][table1] = t0
	l.finalTables[source][schema][table2] = t0
	l.resolveTables()
	c.Assert(l.conflictTables, HasLen, 0)
	c.Assert(l.tables, DeepEquals, l.finalTables)
	c.Assert(l.tables[source][schema], HasLen, 2)
	c.Assert(l.tables[source][schema][table1], DeepEquals, t0)
	c.Assert(l.tables[source][schema][table2], DeepEquals, t0)

	// multiple rename
	// tb1: rename col to new_col
	l.addConflictTable(source, schema, table1, t1)
	l.finalTables[source][schema][table1] = t1
	c.Assert(l.allConflictTableSmaller(), IsTrue)
	c.Assert(l.allConflictTableLarger(), IsTrue)
	c.Assert(l.allFinalTableSmaller(), IsFalse)
	c.Assert(l.allFinalTableLarger(), IsFalse)
	// tb2: rename col to new_col
	l.addConflictTable(source, schema, table2, t1)
	l.finalTables[source][schema][table2] = t1
	c.Assert(l.allConflictTableSmaller(), IsTrue)
	c.Assert(l.allConflictTableLarger(), IsTrue)
	c.Assert(l.allFinalTableSmaller(), IsTrue)
	c.Assert(l.allFinalTableLarger(), IsTrue)
	l.resolveTables()
	// tb1: rename id to a
	l.addConflictTable(source, schema, table1, t9)
	l.finalTables[source][schema][table1] = t9
	c.Assert(l.noConflictWithOneNormalTable(source, schema, table1, t1, t9), IsFalse)
	c.Assert(l.allConflictTableSmaller(), IsTrue)
	c.Assert(l.allConflictTableLarger(), IsTrue)
	c.Assert(l.allFinalTableSmaller(), IsFalse)
	c.Assert(l.allFinalTableLarger(), IsFalse)
	// tb2: rename col to new_col (idempotent)
	l.tables[source][schema][table2] = t0
	l.addConflictTable(source, schema, table2, t1)
	l.finalTables[source][schema][table2] = t1
	c.Assert(l.noConflictWithOneNormalTable(source, schema, table2, t0, t1), IsTrue)
	l.removeConflictTable(source, schema, table2)
	l.tables[source][schema][table2] = t1
	// tb2: rename id to a
	l.addConflictTable(source, schema, table2, t9)
	l.finalTables[source][schema][table2] = t9
	c.Assert(l.noConflictWithOneNormalTable(source, schema, table2, t1, t9), IsFalse)
	c.Assert(l.allConflictTableSmaller(), IsTrue)
	c.Assert(l.allConflictTableLarger(), IsTrue)
	c.Assert(l.allFinalTableSmaller(), IsTrue)
	c.Assert(l.allFinalTableLarger(), IsTrue)
	// reset
	l.finalTables[source][schema][table1] = t0
	l.finalTables[source][schema][table2] = t0
	l.resolveTables()
	c.Assert(l.conflictTables, HasLen, 0)
	c.Assert(l.tables, DeepEquals, l.finalTables)
	c.Assert(l.tables[source][schema], HasLen, 2)
	c.Assert(l.tables[source][schema][table1], DeepEquals, t0)
	c.Assert(l.tables[source][schema][table2], DeepEquals, t0)
}

func (t *testLock) TestNoConflictWithOneNormalTable(c *C) {
	var (
		source       = "source"
		schema       = "schema"
		table1       = "table1"
		table2       = "table2"
		p            = parser.New()
		se           = mock.NewContext()
		tblID  int64 = 111
		ti0          = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, a int, col int)`)
		ti1          = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, a int, new_col int)`)
		ti2          = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, a int, col varchar(4))`)
		ti3          = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, a int, new_col2 int)`)
		ti4          = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, b int, new_col int)`)
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
	c.Assert(l.noConflictWithOneNormalTable(source, schema, table2, t0, t1), IsFalse)

	// mock table1 rename column already
	l.tables[source][schema][table1] = t1
	// table2 rename column
	c.Assert(l.noConflictWithOneNormalTable(source, schema, table2, t0, t1), IsTrue)
	// table2 modify column
	c.Assert(l.noConflictWithOneNormalTable(source, schema, table2, t0, t2), IsFalse)
	// table2 different rename
	c.Assert(l.noConflictWithOneNormalTable(source, schema, table2, t0, t3), IsFalse)

	// mock table1 rename another column already
	l.tables[source][schema][table1] = t4
	// same results
	// table2 rename column
	c.Assert(l.noConflictWithOneNormalTable(source, schema, table2, t0, t1), IsTrue)
	// table2 modify column
	c.Assert(l.noConflictWithOneNormalTable(source, schema, table2, t0, t2), IsFalse)
	// table2 different rename
	c.Assert(l.noConflictWithOneNormalTable(source, schema, table2, t0, t3), IsFalse)
}

func checkRedirectOp(c *C, task, source, schema, table string) bool {
	ops, _, err := GetAllOperations(etcdTestCli)
	c.Assert(err, IsNil)
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

func (t *testLock) TestTrySyncForOneDDL(c *C) {
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
		ti0              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, col1 int)`)
		ti1              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, col1 int, col2 int)`)
		ti2              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY)`)
		ti3              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, col1 int, col3 int)`)
		ti4              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, col2 int)`)
		ti5              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, col3 int)`)
		ti6              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, col3 varchar(4))`)
		ti7              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, col3 int)`)
		ti8              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, col3 varchar(4), col4 int not null)`)
		ti9              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, col2 varchar(4), col4 int not null)`)
		ti10             = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, col3 int, col4 int not null)`)
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
	c.Assert(schemaChanged, IsTrue)
	c.Assert(conflictStage, Equals, ConflictNone)

	// check alter table add column
	schemaChanged, conflictStage = l.trySyncForOneDDL(source, schema, table1, t0, t1)
	c.Assert(schemaChanged, IsTrue)
	c.Assert(conflictStage, Equals, ConflictNone)

	// check create partition, no changed since https://github.com/pingcap/tidb-tools/blob/d671b0840063bc2532941f02e02e12627402844c/pkg/schemacmp/table.go#L251
	schemaChanged, conflictStage = l.trySyncForOneDDL(source, schema, table1, t0, t1)
	c.Assert(schemaChanged, IsTrue)
	c.Assert(conflictStage, Equals, ConflictNone)

	// check alter table drop column
	schemaChanged, conflictStage = l.trySyncForOneDDL(source, schema, table2, t0, t2)
	c.Assert(schemaChanged, IsFalse)
	c.Assert(conflictStage, Equals, ConflictNone)

	// check table rename column
	schemaChanged, conflictStage = l.trySyncForOneDDL(source, schema, table1, t1, t3)
	c.Assert(schemaChanged, IsFalse)
	c.Assert(conflictStage, Equals, ConflictSkipWaitRedirect)

	// check other table add column
	schemaChanged, conflictStage = l.trySyncForOneDDL(source, schema, table2, t2, t4)
	c.Assert(schemaChanged, IsTrue)
	c.Assert(conflictStage, Equals, ConflictNone)

	// check all table rename column
	schemaChanged, conflictStage = l.trySyncForOneDDL(source, schema, table2, t4, t5)
	c.Assert(schemaChanged, IsTrue)
	c.Assert(conflictStage, Equals, ConflictNone)
	// table1 redirect
	c.Assert(checkRedirectOp(c, task, source, schema, table1), IsTrue)

	// check one table modify column
	schemaChanged, conflictStage = l.trySyncForOneDDL(source, schema, table2, t5, t6)
	c.Assert(schemaChanged, IsFalse)
	c.Assert(conflictStage, Equals, ConflictSkipWaitRedirect)

	// check other table drop column
	schemaChanged, conflictStage = l.trySyncForOneDDL(source, schema, table1, t3, t7)
	c.Assert(schemaChanged, IsTrue)
	c.Assert(conflictStage, Equals, ConflictNone)

	// check all table modify column
	schemaChanged, conflictStage = l.trySyncForOneDDL(source, schema, table1, t7, t6)
	c.Assert(schemaChanged, IsTrue)
	c.Assert(conflictStage, Equals, ConflictNone)
	// table2 redirect
	c.Assert(checkRedirectOp(c, task, source, schema, table2), IsTrue)

	// check add column not null no default
	schemaChanged, conflictStage = l.trySyncForOneDDL(source, schema, table1, t6, t8)
	c.Assert(schemaChanged, IsFalse)
	c.Assert(conflictStage, Equals, ConflictSkipWaitRedirect)
	// check idempotent.
	schemaChanged, conflictStage = l.trySyncForOneDDL(source, schema, table1, t6, t8)
	c.Assert(schemaChanged, IsFalse)
	c.Assert(conflictStage, Equals, ConflictSkipWaitRedirect)

	schemaChanged, conflictStage = l.trySyncForOneDDL(source, schema, table2, t6, t8)
	c.Assert(schemaChanged, IsTrue)
	c.Assert(conflictStage, Equals, ConflictNone)
	// table1 redirect
	c.Assert(checkRedirectOp(c, task, source, schema, table2), IsTrue)
	// check idempotent.
	schemaChanged, conflictStage = l.trySyncForOneDDL(source, schema, table2, t6, t8)
	c.Assert(schemaChanged, IsTrue)
	c.Assert(conflictStage, Equals, ConflictNone)

	// check multiple conflict DDL
	// tb1 rename column
	schemaChanged, conflictStage = l.trySyncForOneDDL(source, schema, table1, t8, t9)
	c.Assert(schemaChanged, IsFalse)
	c.Assert(conflictStage, Equals, ConflictSkipWaitRedirect)
	// tb2 modify column
	schemaChanged, conflictStage = l.trySyncForOneDDL(source, schema, table2, t8, t10)
	c.Assert(schemaChanged, IsFalse)
	c.Assert(conflictStage, Equals, ConflictDetected)
}
