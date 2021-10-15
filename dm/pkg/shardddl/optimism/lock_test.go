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
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-tools/pkg/schemacmp"
	"github.com/pingcap/tidb/util/mock"
	"go.etcd.io/etcd/integration"

	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"
)

type testLock struct{}

var _ = Suite(&testLock{})

func TestLock(t *testing.T) {
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

		l = NewLock(etcdTestCli, ID, task, downSchema, downTable, schemacmp.Encode(ti0), tts)

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

		l = NewLock(etcdTestCli, ID, task, downSchema, downTable, schemacmp.Encode(ti0), tts)

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

		l = NewLock(etcdTestCli, ID, task, downSchema, downTable, schemacmp.Encode(ti0), tts)

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

		l = NewLock(etcdTestCli, ID, task, downSchema, downTable, schemacmp.Encode(ti0), tts)

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
		DDLs1            = []string{"ALTER TABLE bar DROP COLUMN c1, ADD COLUMN c2 INT"}
		ti0              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT)`)
		ti1              = createTableInfo(c, p, se, tblID,
			`CREATE TABLE bar (id INT PRIMARY KEY, c2 INT)`) // `c1` dropped, `c2` added
		tables = map[string]map[string]struct{}{
			db: {tbls[0]: struct{}{}, tbls[1]: struct{}{}},
		}
		tts = []TargetTable{
			newTargetTable(task, source, downSchema, downTable, tables),
		}

		l = NewLock(etcdTestCli, ID, task, downSchema, downTable, schemacmp.Encode(ti0), tts)

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
	c.Assert(terror.ErrShardDDLOptimismTrySyncFail.Equal(err), IsTrue)
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
		l      = NewLock(etcdTestCli, ID, task, downSchema, downTable, schemacmp.Encode(ti0), tts)
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
	c.Assert(ready[source1][db1][tbl2], IsTrue)
	c.Assert(ready[source2], HasLen, 1)
	c.Assert(ready[source2][db2], HasLen, 2)
	c.Assert(ready[source2][db2][tbl1], IsTrue)
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
	c.Assert(ready[source2][db2][tbl1], IsTrue)
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
		l      = NewLock(etcdTestCli, ID, task, downSchema, downTable, schemacmp.Encode(ti0), tts)

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
	cmp, err := l.tables[source][db][tbls[0]].Compare(l.Joined())
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)
	cmp, err = l.tables[source][db][tbls[1]].Compare(l.Joined())
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
	cmp, err = l.tables[source][db][tbls[0]].Compare(l.Joined())
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)
	cmp, err = l.tables[source][db][tbls[1]].Compare(l.Joined())
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
	cmp, err = l.tables[source][db][tbls[0]].Compare(l.Joined())
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)
	cmp, err = l.tables[source][db][tbls[1]].Compare(l.Joined())
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
	cmp, err = l.tables[source][db][tbls[0]].Compare(l.Joined())
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)
	cmp, err = l.tables[source][db][tbls[1]].Compare(l.Joined())
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
	cmp, err = l.tables[source][db][tbls[0]].Compare(l.Joined())
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)
	cmp, err = l.tables[source][db][tbls[1]].Compare(l.Joined())
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
		l      = NewLock(etcdTestCli, ID, task, downSchema, downTable, schemacmp.Encode(ti0), tts)

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
	cmp, err := l.tables[source][db][tbls[0]].Compare(l.Joined())
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)
	cmp, err = l.tables[source][db][tbls[1]].Compare(l.Joined())
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, -1)

	// TrySync for the second table with another schema (add two columns, one of them will cause conflict).
	info = newInfoWithVersion(task, source, db, tbls[1], downSchema, downTable, DDLs2, ti0, []*model.TableInfo{ti2_1, ti2}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	c.Assert(terror.ErrShardDDLOptimismTrySyncFail.Equal(err), IsTrue)
	c.Assert(DDLs, DeepEquals, []string{})
	c.Assert(cols, DeepEquals, []string{})
	c.Assert(l.versions, DeepEquals, vers)
	cmp, err = l.tables[source][db][tbls[1]].Compare(l.Joined())
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
	cmp, err = l.tables[source][db][tbls[0]].Compare(l.Joined())
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)
	cmp, err = l.tables[source][db][tbls[1]].Compare(l.Joined())
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)

	// TrySync for the second table, succeed now
	info = newInfoWithVersion(task, source, db, tbls[1], downSchema, downTable, DDLs2, ti0, []*model.TableInfo{ti2_1, ti2}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs2)
	c.Assert(cols, DeepEquals, []string{})
	c.Assert(l.versions, DeepEquals, vers)
	cmp, err = l.tables[source][db][tbls[1]].Compare(l.Joined())
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
		l      = NewLock(etcdTestCli, ID, task, downSchema, downTable, schemacmp.Encode(ti0), tts)

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
	cmp, err := l.tables[source][db][tbls[0]].Compare(l.Joined())
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)
	cmp, err = l.tables[source][db][tbls[1]].Compare(l.Joined())
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, -1)

	// TrySync for the second table with another schema (add two columns, one of them will cause conflict).
	info = newInfoWithVersion(task, source, db, tbls[1], downSchema, downTable, DDLs2, ti0, []*model.TableInfo{ti3, ti2}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	c.Assert(terror.ErrShardDDLOptimismTrySyncFail.Equal(err), IsTrue)
	c.Assert(DDLs, DeepEquals, []string{})
	c.Assert(cols, DeepEquals, []string{})
	c.Assert(l.versions, DeepEquals, vers)
	cmp, err = l.tables[source][db][tbls[1]].Compare(l.Joined())
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
	cmp, err = l.tables[source][db][tbls[1]].Compare(l.Joined())
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, -1)

	// TrySync for the second table to replace a new ddl without non-conflict column, the conflict should still exist.
	info = newInfoWithVersion(task, source, db, tbls[1], downSchema, downTable, DDLs3, ti0, []*model.TableInfo{ti3}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	c.Assert(terror.ErrShardDDLOptimismTrySyncFail.Equal(err), IsTrue)
	c.Assert(DDLs, DeepEquals, []string{})
	c.Assert(cols, DeepEquals, []string{})
	c.Assert(l.versions, DeepEquals, vers)
	cmp, err = l.tables[source][db][tbls[1]].Compare(l.Joined())
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
	cmp, err = l.tables[source][db][tbls[1]].Compare(l.Joined())
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
	cmp, err = l.tables[source][db][tbls[0]].Compare(l.Joined())
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)
	cmp, err = l.tables[source][db][tbls[1]].Compare(l.Joined())
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, -1)

	// TrySync for the second table with another schema (add two columns, one of them will cause conflict).
	info = newInfoWithVersion(task, source, db, tbls[1], downSchema, downTable, DDLs6, ti1, []*model.TableInfo{ti6_1, ti6}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	c.Assert(terror.ErrShardDDLOptimismTrySyncFail.Equal(err), IsTrue)
	c.Assert(DDLs, DeepEquals, []string{})
	c.Assert(cols, DeepEquals, []string{})
	cmp, err = l.tables[source][db][tbls[1]].Compare(l.Joined())
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
	cmp, err = l.tables[source][db][tbls[0]].Compare(l.Joined())
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, -1)
	cmp, err = l.tables[source][db][tbls[1]].Compare(l.Joined())
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

		l = NewLock(etcdTestCli, ID, task, downSchema, downTable, schemacmp.Encode(ti0), tts)

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
	c.Assert(DDLs, DeepEquals, DDLs1[:1])
	c.Assert(cols, DeepEquals, []string{})
	c.Assert(terror.ErrMasterInconsistentOptimisticDDLsAndInfo.Equal(err), IsTrue)

	info = newInfoWithVersion(task, sources[0], dbs[0], tbls[0], downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1}, vers)
	DDLs, cols, err = l.TrySync(info, tts)
	c.Assert(DDLs, DeepEquals, DDLs1)
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
		l      = NewLock(etcdTestCli, ID, task, downSchema, downTable, schemacmp.Encode(ti0), tts)

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
	c.Assert(l.TryRemoveTable(source, db, tbl2), IsTrue)
	delete(vers[source][db], tbl2)
	ready = l.Ready()
	c.Assert(ready, HasLen, 1)
	c.Assert(ready[source], HasLen, 1)
	c.Assert(ready[source][db], HasLen, 1)
	c.Assert(ready[source][db][tbl1], IsTrue)
	c.Assert(l.versions, DeepEquals, vers)

	// CASE: remove a table will not rebuild joined schema now.
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
	c.Assert(l.TryRemoveTable(source, db, tbl2), IsTrue)
	delete(vers[source][db], tbl2)
	ready = l.Ready()
	c.Assert(ready, HasLen, 1)
	c.Assert(ready[source], HasLen, 1)
	c.Assert(ready[source][db], HasLen, 1)
	c.Assert(ready[source][db][tbl1], IsFalse) // the joined schema is not rebuild.
	c.Assert(l.versions, DeepEquals, vers)

	// CASE: try to remove for not-exists table.
	c.Assert(l.TryRemoveTable(source, db, "not-exist"), IsFalse)
	c.Assert(l.TryRemoveTable(source, "not-exist", tbl1), IsFalse)
	c.Assert(l.TryRemoveTable("not-exist", db, tbl1), IsFalse)
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
		l      = NewLock(etcdTestCli, ID, task, downSchema, downTable, schemacmp.Encode(ti0), tts)

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
		l      = NewLock(etcdTestCli, ID, task, downSchema, downTable, schemacmp.Encode(ti0), tts)

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
	c.Assert(DDLs, DeepEquals, DDLs2)
	c.Assert(cols, DeepEquals, []string{})
	c.Assert(l.versions, DeepEquals, vers)

	// case 2: add a column with a smaller field length
	l = NewLock(etcdTestCli, ID, task, downSchema, downTable, schemacmp.Encode(ti0), tts)

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
	c.Assert(DDLs, DeepEquals, DDLs1)
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

		tables = map[string]map[string]struct{}{db: {tbls[0]: struct{}{}, tbls[1]: struct{}{}}}
		tts    = []TargetTable{newTargetTable(task, source, downSchema, downTable, tables)}
		l      = NewLock(etcdTestCli, ID, task, downSchema, downTable, schemacmp.Encode(ti0), tts)

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
}

func (t *testLock) trySyncForAllTablesLarger(c *C, l *Lock,
	ddls []string, tableInfoBefore *model.TableInfo, tis []*model.TableInfo, tts []TargetTable, vers map[string]map[string]map[string]int64) {
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
	tableInfosAfter []*model.TableInfo, vers map[string]map[string]map[string]int64) Info {
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

		l = NewLock(etcdTestCli, ID, task, downSchema, downTable, schemacmp.Encode(ti0), tts)

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

	cmp, err = l.tables[source][db][tbls[0]].Compare(l.joined)
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
