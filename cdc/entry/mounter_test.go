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

package entry

import (
	"context"
	"strings"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/regionspan"
	"github.com/pingcap/ticdc/pkg/util/testleak"
	ticonfig "github.com/pingcap/tidb/config"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util/testkit"
	"go.uber.org/zap"
)

type mountTxnsSuite struct{}

var _ = check.Suite(&mountTxnsSuite{})

func (s *mountTxnsSuite) TestMounterDisableOldValue(c *check.C) {
	defer testleak.AfterTest(c)()
	testCases := []struct {
		tableName      string
		createTableDDL string
		values         [][]interface{}
	}{{
		tableName:      "simple",
		createTableDDL: "create table simple(id int primary key)",
		values:         [][]interface{}{{1}, {2}, {3}, {4}, {5}},
	}, {
		tableName:      "no_pk",
		createTableDDL: "create table no_pk(id int not null unique key)",
		values:         [][]interface{}{{1}, {2}, {3}, {4}, {5}},
	}, {
		tableName:      "many_index",
		createTableDDL: "create table many_index(id int not null unique key, c1 int unique key, c2 int, INDEX (c2))",
		values:         [][]interface{}{{1, 1, 1}, {2, 2, 2}, {3, 3, 3}, {4, 4, 4}, {5, 5, 5}},
	}, {
		tableName:      "default_value",
		createTableDDL: "create table default_value(id int primary key, c1 int, c2 int not null default 5, c3 varchar(20), c4 varchar(20) not null default '666')",
		values:         [][]interface{}{{1}, {2}, {3}, {4}, {5}},
	}, {
		tableName: "partition_table",
		createTableDDL: `CREATE TABLE partition_table  (
			id INT NOT NULL AUTO_INCREMENT UNIQUE KEY,
			fname VARCHAR(25) NOT NULL,
			lname VARCHAR(25) NOT NULL,
			store_id INT NOT NULL,
			department_id INT NOT NULL,
			INDEX (department_id)
		)

		PARTITION BY RANGE(id)  (
			PARTITION p0 VALUES LESS THAN (5),
			PARTITION p1 VALUES LESS THAN (10),
			PARTITION p2 VALUES LESS THAN (15),
			PARTITION p3 VALUES LESS THAN (20)
		)`,
		values: [][]interface{}{
			{1, "aa", "bb", 12, 12},
			{6, "aac", "bab", 51, 51},
			{11, "aad", "bsb", 71, 61},
			{18, "aae", "bbf", 21, 14},
			{15, "afa", "bbc", 11, 12},
		},
	}, {
		tableName: "tp_int",
		createTableDDL: `create table tp_int
		(
			id          int auto_increment,
			c_tinyint   tinyint   null,
			c_smallint  smallint  null,
			c_mediumint mediumint null,
			c_int       int       null,
			c_bigint    bigint    null,
			constraint pk
				primary key (id)
		);`,
		values: [][]interface{}{
			{1, 1, 2, 3, 4, 5},
			{2},
			{3, 3, 4, 5, 6, 7},
			{4, 127, 32767, 8388607, 2147483647, 9223372036854775807},
			{5, -128, -32768, -8388608, -2147483648, -9223372036854775808},
		},
	}, {
		tableName: "tp_text",
		createTableDDL: `create table tp_text
		(
			id           int auto_increment,
			c_tinytext   tinytext      null,
			c_text       text          null,
			c_mediumtext mediumtext    null,
			c_longtext   longtext      null,
			c_varchar    varchar(16)   null,
			c_char       char(16)      null,
			c_tinyblob   tinyblob      null,
			c_blob       blob          null,
			c_mediumblob mediumblob    null,
			c_longblob   longblob      null,
			c_binary     binary(16)    null,
			c_varbinary  varbinary(16) null,
			constraint pk
				primary key (id)
		);`,
		values: [][]interface{}{
			{1},
			{
				2, "89504E470D0A1A0A", "89504E470D0A1A0A", "89504E470D0A1A0A", "89504E470D0A1A0A", "89504E470D0A1A0A",
				"89504E470D0A1A0A",
				[]byte{0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A},
				[]byte{0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A},
				[]byte{0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A},
				[]byte{0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A},
				[]byte{0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A},
				[]byte{0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A},
			},
			{
				3, "bug free", "bug free", "bug free", "bug free", "bug free", "bug free", "bug free", "bug free",
				"bug free", "bug free", "bug free", "bug free",
			},
			{4, "", "", "", "", "", "", "", "", "", "", "", ""},
			{5, "你好", "我好", "大家好", "道路", "千万条", "安全", "第一条", "行车", "不规范", "亲人", "两行泪", "！"},
			{6, "😀", "😃", "😄", "😁", "😆", "😅", "😂", "🤣", "☺️", "😊", "😇", "🙂"},
		},
	}, {
		tableName: "tp_time",
		createTableDDL: `create table tp_time
		(
			id          int auto_increment,
			c_date      date      null,
			c_datetime  datetime  null,
			c_timestamp timestamp null,
			c_time      time      null,
			c_year      year      null,
			constraint pk
				primary key (id)
		);`,
		values: [][]interface{}{
			{1},
			{2, "2020-02-20", "2020-02-20 02:20:20", "2020-02-20 02:20:20", "02:20:20", "2020"},
		},
	}, {
		tableName: "tp_real",
		createTableDDL: `create table tp_real
		(
			id        int auto_increment,
			c_float   float   null,
			c_double  double  null,
			c_decimal decimal null,
			constraint pk
				primary key (id)
		);`,
		values: [][]interface{}{
			{1},
			{2, "2020.0202", "2020.0303", "2020.0404"},
		},
	}, {
		tableName: "tp_other",
		createTableDDL: `create table tp_other
		(
			id     int auto_increment,
			c_enum enum ('a','b','c') null,
			c_set  set ('a','b','c')  null,
			c_bit  bit(64)            null,
			c_json json               null,
			constraint pk
				primary key (id)
		);`,
		values: [][]interface{}{
			{1},
			{2, "a", "a,c", 888, `{"aa":"bb"}`},
		},
	}, {
		tableName:      "clustered_index1",
		createTableDDL: "CREATE TABLE clustered_index1 (id VARCHAR(255) PRIMARY KEY, data INT);",
		values: [][]interface{}{
			{"hhh"},
			{"你好😘", 666},
			{"世界🤪", 888},
		},
	}, {
		tableName:      "clustered_index2",
		createTableDDL: "CREATE TABLE clustered_index2 (id VARCHAR(255), data INT, ddaa date, PRIMARY KEY (id, data, ddaa), UNIQUE KEY (id, data, ddaa));",
		values: [][]interface{}{
			{"你好😘", 666, "2020-11-20"},
			{"世界🤪", 888, "2020-05-12"},
		},
	}}
	for _, tc := range testCases {
		testMounterDisableOldValue(c, tc)
	}
}

func testMounterDisableOldValue(c *check.C, tc struct {
	tableName      string
	createTableDDL string
	values         [][]interface{}
}) {
	store, err := mockstore.NewMockStore()
	c.Assert(err, check.IsNil)
	defer store.Close() //nolint:errcheck
	ticonfig.UpdateGlobal(func(conf *ticonfig.Config) {
		// we can update the tidb config here
	})
	session.SetSchemaLease(0)
	session.DisableStats4Test()
	domain, err := session.BootstrapSession(store)
	c.Assert(err, check.IsNil)
	defer domain.Close()
	domain.SetStatsUpdating(true)
	tk := testkit.NewTestKit(c, store)
	tk.MustExec("set @@tidb_enable_clustered_index=1;")
	tk.MustExec("use test;")

	tk.MustExec(tc.createTableDDL)

	jobs, err := getAllHistoryDDLJob(store)
	c.Assert(err, check.IsNil)
	scheamStorage, err := NewSchemaStorage(nil, 0, nil, false)
	c.Assert(err, check.IsNil)
	for _, job := range jobs {
		err := scheamStorage.HandleDDLJob(job)
		c.Assert(err, check.IsNil)
	}
	tableInfo, ok := scheamStorage.GetLastSnapshot().GetTableByName("test", tc.tableName)
	c.Assert(ok, check.IsTrue)
	if tableInfo.IsCommonHandle {
		// we can check this log to make sure if the clustered-index is enabled
		log.Info("this table is enable the clustered index", zap.String("tableName", tableInfo.Name.L))
	}

	for _, params := range tc.values {
		insertSQL := prepareInsertSQL(c, tableInfo, len(params))
		tk.MustExec(insertSQL, params...)
	}

	ver, err := store.CurrentVersion()
	c.Assert(err, check.IsNil)
	scheamStorage.AdvanceResolvedTs(ver.Ver)
	mounter := NewMounter(scheamStorage, 1, false).(*mounterImpl)
	mounter.tz = time.Local
	ctx := context.Background()

	mountAndCheckRowInTable := func(tableID int64, f func(key []byte, value []byte) *model.RawKVEntry) int {
		var rows int
		walkTableSpanInStore(c, store, tableID, func(key []byte, value []byte) {
			rawKV := f(key, value)
			row, err := mounter.unmarshalAndMountRowChanged(ctx, rawKV)
			c.Assert(err, check.IsNil)
			if row == nil {
				return
			}
			rows++
			c.Assert(row.Table.Table, check.Equals, tc.tableName)
			c.Assert(row.Table.Schema, check.Equals, "test")
			// TODO: test column flag, column type and index columns
			if len(row.Columns) != 0 {
				checkSQL, params := prepareCheckSQL(c, tc.tableName, row.Columns)
				result := tk.MustQuery(checkSQL, params...)
				result.Check([][]interface{}{{"1"}})
			}
			if len(row.PreColumns) != 0 {
				checkSQL, params := prepareCheckSQL(c, tc.tableName, row.PreColumns)
				result := tk.MustQuery(checkSQL, params...)
				result.Check([][]interface{}{{"1"}})
			}
		})
		return rows
	}

	mountAndCheckRow := func(f func(key []byte, value []byte) *model.RawKVEntry) int {
		partitionInfo := tableInfo.GetPartitionInfo()
		if partitionInfo == nil {
			return mountAndCheckRowInTable(tableInfo.ID, f)
		}
		var rows int
		for _, p := range partitionInfo.Definitions {
			rows += mountAndCheckRowInTable(p.ID, f)
		}
		return rows
	}

	rows := mountAndCheckRow(func(key []byte, value []byte) *model.RawKVEntry {
		return &model.RawKVEntry{
			OpType:  model.OpTypePut,
			Key:     key,
			Value:   value,
			StartTs: ver.Ver - 1,
			CRTs:    ver.Ver,
		}
	})
	c.Assert(rows, check.Equals, len(tc.values))

	rows = mountAndCheckRow(func(key []byte, value []byte) *model.RawKVEntry {
		return &model.RawKVEntry{
			OpType:  model.OpTypeDelete,
			Key:     key,
			Value:   nil, // delete event doesn't include a value when old-value is disabled
			StartTs: ver.Ver - 1,
			CRTs:    ver.Ver,
		}
	})
	c.Assert(rows, check.Equals, len(tc.values))
}

func prepareInsertSQL(c *check.C, tableInfo *model.TableInfo, columnLens int) string {
	var sb strings.Builder
	_, err := sb.WriteString("INSERT INTO " + tableInfo.Name.O + "(")
	c.Assert(err, check.IsNil)
	for i := 0; i < columnLens; i++ {
		col := tableInfo.Columns[i]
		if i != 0 {
			_, err = sb.WriteString(", ")
			c.Assert(err, check.IsNil)
		}
		_, err = sb.WriteString(col.Name.O)
		c.Assert(err, check.IsNil)
	}
	_, err = sb.WriteString(") VALUES (")
	c.Assert(err, check.IsNil)
	for i := 0; i < columnLens; i++ {
		if i != 0 {
			_, err = sb.WriteString(", ")
			c.Assert(err, check.IsNil)
		}
		_, err = sb.WriteString("?")
		c.Assert(err, check.IsNil)
	}
	_, err = sb.WriteString(")")
	c.Assert(err, check.IsNil)
	return sb.String()
}

func prepareCheckSQL(c *check.C, tableName string, cols []*model.Column) (string, []interface{}) {
	var sb strings.Builder
	_, err := sb.WriteString("SELECT count(1) FROM " + tableName + " WHERE ")
	c.Assert(err, check.IsNil)
	params := make([]interface{}, 0, len(cols))
	for i, col := range cols {
		if col == nil {
			continue
		}
		if i != 0 {
			_, err = sb.WriteString(" AND ")
			c.Assert(err, check.IsNil)
		}
		if col.Value == nil {
			_, err = sb.WriteString(col.Name + " IS NULL")
			c.Assert(err, check.IsNil)
			continue
		}
		params = append(params, col.Value)
		if col.Type == mysql.TypeJSON {
			_, err = sb.WriteString(col.Name + " = CAST(? AS JSON)")
		} else {
			_, err = sb.WriteString(col.Name + " = ?")
		}
		c.Assert(err, check.IsNil)
	}
	return sb.String(), params
}

func walkTableSpanInStore(c *check.C, store tidbkv.Storage, tableID int64, f func(key []byte, value []byte)) {
	txn, err := store.Begin()
	c.Assert(err, check.IsNil)
	defer txn.Rollback() //nolint:errcheck
	tableSpan := regionspan.GetTableSpan(tableID, false)
	kvIter, err := txn.Iter(tableSpan.Start, tableSpan.End)
	c.Assert(err, check.IsNil)
	defer kvIter.Close()
	for kvIter.Valid() {
		f(kvIter.Key(), kvIter.Value())
		err = kvIter.Next()
		c.Assert(err, check.IsNil)
	}
}
