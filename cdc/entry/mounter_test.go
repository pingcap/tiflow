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
	"math"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/log"
	ticonfig "github.com/pingcap/tidb/config"
	tidbkv "github.com/pingcap/tidb/kv"
	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tiflow/cdc/model"
<<<<<<< HEAD
	"github.com/pingcap/tiflow/pkg/regionspan"
=======
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/filter"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/pingcap/tiflow/pkg/sqlmodel"
>>>>>>> d30f48b689 (mounter(ticdc): mount float32 value correctly to avoid the precision lost. (#8502))
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

func TestMounterDisableOldValue(t *testing.T) {
	testCases := []struct {
		tableName      string
		createTableDDL string
<<<<<<< HEAD
		values         [][]interface{}
=======
		// [] for rows, []interface{} for columns.
		values [][]interface{}
		// [] for table partition if there is any,
		// []int for approximateBytes of rows.
		putApproximateBytes [][]int
		delApproximateBytes [][]int
>>>>>>> d30f48b689 (mounter(ticdc): mount float32 value correctly to avoid the precision lost. (#8502))
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
<<<<<<< HEAD
		(
			id        int auto_increment,
			c_float   float   null,
			c_double  double  null,
			c_decimal decimal null,
			constraint pk
				primary key (id)
		);`,
=======
	(
		id        int auto_increment,
		c_float   float   null,
		c_double  double  null,
		c_decimal decimal null,
		constraint pk
		primary key (id)
	);`,
>>>>>>> d30f48b689 (mounter(ticdc): mount float32 value correctly to avoid the precision lost. (#8502))
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
		testMounterDisableOldValue(t, tc)
	}
}

func testMounterDisableOldValue(t *testing.T, tc struct {
	tableName      string
	createTableDDL string
	values         [][]interface{}
}) {
	store, err := mockstore.NewMockStore()
	require.Nil(t, err)
	defer store.Close() //nolint:errcheck
	ticonfig.UpdateGlobal(func(conf *ticonfig.Config) {
		// we can update the tidb config here
	})
	session.SetSchemaLease(0)
	session.DisableStats4Test()
	domain, err := session.BootstrapSession(store)
	require.Nil(t, err)
	defer domain.Close()
	domain.SetStatsUpdating(true)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_enable_clustered_index=1;")
	tk.MustExec("use test;")

	tk.MustExec(tc.createTableDDL)

	jobs, err := getAllHistoryDDLJob(store)
	require.Nil(t, err)
	scheamStorage, err := NewSchemaStorage(nil, 0, nil, false)
	require.Nil(t, err)
	for _, job := range jobs {
		err := scheamStorage.HandleDDLJob(job)
		require.Nil(t, err)
	}
	tableInfo, ok := scheamStorage.GetLastSnapshot().GetTableByName("test", tc.tableName)
	require.True(t, ok)
	if tableInfo.IsCommonHandle {
		// we can check this log to make sure if the clustered-index is enabled
		log.Info("this table is enable the clustered index", zap.String("tableName", tableInfo.Name.L))
	}

	for _, params := range tc.values {
		insertSQL := prepareInsertSQL(t, tableInfo, len(params))
		tk.MustExec(insertSQL, params...)
	}

	ver, err := store.CurrentVersion(oracle.GlobalTxnScope)
	require.Nil(t, err)
	scheamStorage.AdvanceResolvedTs(ver.Ver)
<<<<<<< HEAD
	mounter := NewMounter(scheamStorage, 1, false).(*mounterImpl)
=======
	config := config.GetDefaultReplicaConfig()
	filter, err := filter.NewFilter(config, "")
	require.Nil(t, err)
	mounter := NewMounter(scheamStorage,
		model.DefaultChangeFeedID("c1"),
		time.UTC, filter, false).(*mounter)
>>>>>>> d30f48b689 (mounter(ticdc): mount float32 value correctly to avoid the precision lost. (#8502))
	mounter.tz = time.Local
	ctx := context.Background()

	mountAndCheckRowInTable := func(tableID int64, f func(key []byte, value []byte) *model.RawKVEntry) int {
		var rows int
		walkTableSpanInStore(t, store, tableID, func(key []byte, value []byte) {
			rawKV := f(key, value)
			row, err := mounter.unmarshalAndMountRowChanged(ctx, rawKV)
			require.Nil(t, err)
			if row == nil {
				return
			}
			rows++
			require.Equal(t, row.Table.Table, tc.tableName)
			require.Equal(t, row.Table.Schema, "test")
			// TODO: test column flag, column type and index columns
			if len(row.Columns) != 0 {
				checkSQL, params := prepareCheckSQL(t, tc.tableName, row.Columns)
				result := tk.MustQuery(checkSQL, params...)
				result.Check([][]interface{}{{"1"}})
			}
			if len(row.PreColumns) != 0 {
				checkSQL, params := prepareCheckSQL(t, tc.tableName, row.PreColumns)
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
	require.Equal(t, rows, len(tc.values))

	rows = mountAndCheckRow(func(key []byte, value []byte) *model.RawKVEntry {
		return &model.RawKVEntry{
			OpType:  model.OpTypeDelete,
			Key:     key,
			Value:   nil, // delete event doesn't include a value when old-value is disabled
			StartTs: ver.Ver - 1,
			CRTs:    ver.Ver,
		}
	})
	require.Equal(t, rows, len(tc.values))
}

func prepareInsertSQL(t *testing.T, tableInfo *model.TableInfo, columnLens int) string {
	var sb strings.Builder
	_, err := sb.WriteString("INSERT INTO " + tableInfo.Name.O + "(")
	require.Nil(t, err)
	for i := 0; i < columnLens; i++ {
		col := tableInfo.Columns[i]
		if i != 0 {
			_, err = sb.WriteString(", ")
			require.Nil(t, err)
		}
		_, err = sb.WriteString(col.Name.O)
		require.Nil(t, err)
	}
	_, err = sb.WriteString(") VALUES (")
	require.Nil(t, err)
	for i := 0; i < columnLens; i++ {
		if i != 0 {
			_, err = sb.WriteString(", ")
			require.Nil(t, err)
		}
		_, err = sb.WriteString("?")
		require.Nil(t, err)
	}
	_, err = sb.WriteString(")")
	require.Nil(t, err)
	return sb.String()
}

func prepareCheckSQL(t *testing.T, tableName string, cols []*model.Column) (string, []interface{}) {
	var sb strings.Builder
	_, err := sb.WriteString("SELECT count(1) FROM " + tableName + " WHERE ")
	require.Nil(t, err)
	params := make([]interface{}, 0, len(cols))
	for i, col := range cols {
		// Since float type has precision problem, so skip it to avoid compare float number.
		if col == nil || col.Type == mysql.TypeFloat {
			continue
		}
		if i != 0 {
			_, err = sb.WriteString(" AND ")
			require.Nil(t, err)
		}
		if col.Value == nil {
			_, err = sb.WriteString(col.Name + " IS NULL")
			require.Nil(t, err)
			continue
		}
		params = append(params, col.Value)
		if col.Type == mysql.TypeJSON {
			_, err = sb.WriteString(col.Name + " = CAST(? AS JSON)")
		} else {
			_, err = sb.WriteString(col.Name + " = ?")
		}
		require.Nil(t, err)
	}
	return sb.String(), params
}

func walkTableSpanInStore(t *testing.T, store tidbkv.Storage, tableID int64, f func(key []byte, value []byte)) {
	txn, err := store.Begin()
	require.Nil(t, err)
	defer txn.Rollback() //nolint:errcheck
	tableSpan := regionspan.GetTableSpan(tableID)
	kvIter, err := txn.Iter(tableSpan.Start, tableSpan.End)
	require.Nil(t, err)
	defer kvIter.Close()
	for kvIter.Valid() {
		f(kvIter.Key(), kvIter.Value())
		err = kvIter.Next()
		require.Nil(t, err)
	}
}

// Check following MySQL type, ref to:
// https://github.com/pingcap/tidb/blob/master/parser/mysql/type.go
type columnInfoAndResult struct {
	ColInfo timodel.ColumnInfo
	Res     interface{}
}

// We use OriginDefaultValue instead of DefaultValue in the ut, pls ref to
// https://github.com/pingcap/tiflow/issues/4048
// FIXME: OriginDefaultValue seems always to be string, and test more corner case
// Ref: https://github.com/pingcap/tidb/blob/d2c352980a43bb593db81fd1db996f47af596d91/table/column.go#L489
func TestGetDefaultZeroValue(t *testing.T) {
	colAndRess := []columnInfoAndResult{
		// mysql flag null
		{
			ColInfo: timodel.ColumnInfo{
				FieldType: types.FieldType{
					Flag: uint(0),
				},
			},
			Res: nil,
		},
		// mysql.TypeTiny + notnull + nodefault
		{
			ColInfo: timodel.ColumnInfo{
				FieldType: types.FieldType{
					Tp:   mysql.TypeTiny,
					Flag: mysql.NotNullFlag,
				},
			},
			Res: int64(0),
		},
		// mysql.TypeTiny + notnull + default
		{
			ColInfo: timodel.ColumnInfo{
				OriginDefaultValue: -1314,
				FieldType: types.FieldType{
					Tp:   mysql.TypeTiny,
					Flag: mysql.NotNullFlag,
				},
			},
			Res: int64(-1314),
		},
		// mysql.TypeTiny + notnull + default + unsigned
		{
			ColInfo: timodel.ColumnInfo{
				FieldType: types.FieldType{
					Tp:   mysql.TypeTiny,
					Flag: mysql.NotNullFlag | mysql.UnsignedFlag,
				},
			},
			Res: uint64(0),
		},
		// mysql.TypeTiny + notnull + unsigned
		{
			ColInfo: timodel.ColumnInfo{
				OriginDefaultValue: uint64(1314),
				FieldType: types.FieldType{
					Tp:   mysql.TypeTiny,
					Flag: mysql.NotNullFlag | mysql.UnsignedFlag,
				},
			},
			Res: uint64(1314),
		},
		// mysql.TypeTiny + null + default
		{
			ColInfo: timodel.ColumnInfo{
				OriginDefaultValue: -1314,
				FieldType: types.FieldType{
					Tp:   mysql.TypeTiny,
					Flag: uint(0),
				},
			},
			Res: int64(-1314),
		},
		// mysql.TypeTiny + null + nodefault
		{
			ColInfo: timodel.ColumnInfo{
				FieldType: types.FieldType{
					Tp:   mysql.TypeTiny,
					Flag: uint(0),
				},
			},
			Res: nil,
		},
		// mysql.TypeShort, others testCases same as tiny
		{
			ColInfo: timodel.ColumnInfo{
				FieldType: types.FieldType{
					Tp:   mysql.TypeShort,
					Flag: mysql.NotNullFlag,
				},
			},
			Res: int64(0),
		},
		// mysql.TypeLong, others testCases same as tiny
		{
			ColInfo: timodel.ColumnInfo{
				FieldType: types.FieldType{
					Tp:   mysql.TypeLong,
					Flag: mysql.NotNullFlag,
				},
			},
			Res: int64(0),
		},
		// mysql.TypeLonglong, others testCases same as tiny
		{
			ColInfo: timodel.ColumnInfo{
				FieldType: types.FieldType{
					Tp:   mysql.TypeLonglong,
					Flag: mysql.NotNullFlag,
				},
			},
			Res: int64(0),
		},
		// mysql.TypeInt24, others testCases same as tiny
		{
			ColInfo: timodel.ColumnInfo{
				FieldType: types.FieldType{
					Tp:   mysql.TypeInt24,
					Flag: mysql.NotNullFlag,
				},
			},
			Res: int64(0),
		},
		// mysql.TypeFloat + notnull + nodefault
		{
<<<<<<< HEAD
			ColInfo: timodel.ColumnInfo{
				FieldType: types.FieldType{
					Tp:   mysql.TypeFloat,
					Flag: mysql.NotNullFlag,
				},
			},
			Res: float64(0),
=======
			Name:    "mysql.TypeFloat + notnull + nodefault",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeFloatNotNull},
			Res:     float32(0),
			Default: nil,
>>>>>>> d30f48b689 (mounter(ticdc): mount float32 value correctly to avoid the precision lost. (#8502))
		},
		// mysql.TypeFloat + notnull + default
		{
			ColInfo: timodel.ColumnInfo{
<<<<<<< HEAD
				OriginDefaultValue: -3.1415,
				FieldType: types.FieldType{
					Tp:   mysql.TypeFloat,
					Flag: mysql.NotNullFlag,
				},
			},
			Res: float64(-3.1415),
=======
				OriginDefaultValue: float32(-3.1415),
				FieldType:          *ftTypeFloatNotNull,
			},
			Res:     float32(-3.1415),
			Default: float32(-3.1415),
>>>>>>> d30f48b689 (mounter(ticdc): mount float32 value correctly to avoid the precision lost. (#8502))
		},
		// mysql.TypeFloat + notnull + default + unsigned
		{
			ColInfo: timodel.ColumnInfo{
<<<<<<< HEAD
				OriginDefaultValue: 3.1415,
				FieldType: types.FieldType{
					Tp:   mysql.TypeFloat,
					Flag: mysql.NotNullFlag | mysql.UnsignedFlag,
				},
			},
			Res: float64(3.1415),
=======
				OriginDefaultValue: float32(3.1415),
				FieldType:          *ftTypeFloatNotNullUnSigned,
			},
			Res:     float32(3.1415),
			Default: float32(3.1415),
>>>>>>> d30f48b689 (mounter(ticdc): mount float32 value correctly to avoid the precision lost. (#8502))
		},
		// mysql.TypeFloat + notnull + unsigned
		{
			ColInfo: timodel.ColumnInfo{
				FieldType: types.FieldType{
					Tp:   mysql.TypeFloat,
					Flag: mysql.NotNullFlag | mysql.UnsignedFlag,
				},
			},
<<<<<<< HEAD
			Res: float64(0),
=======
			Res:     float32(0),
			Default: nil,
>>>>>>> d30f48b689 (mounter(ticdc): mount float32 value correctly to avoid the precision lost. (#8502))
		},
		// mysql.TypeFloat + null + default
		{
			ColInfo: timodel.ColumnInfo{
<<<<<<< HEAD
				OriginDefaultValue: -3.1415,
				FieldType: types.FieldType{
					Tp:   mysql.TypeFloat,
					Flag: uint(0),
				},
			},
			Res: float64(-3.1415),
=======
				OriginDefaultValue: float32(-3.1415),
				FieldType:          *ftTypeFloatNull,
			},
			Res:     float32(-3.1415),
			Default: float32(-3.1415),
>>>>>>> d30f48b689 (mounter(ticdc): mount float32 value correctly to avoid the precision lost. (#8502))
		},
		// mysql.TypeFloat + null + nodefault
		{
			ColInfo: timodel.ColumnInfo{
				FieldType: types.FieldType{
					Tp:   mysql.TypeFloat,
					Flag: uint(0),
				},
			},
			Res: nil,
		},
		// mysql.TypeDouble, other testCases same as float
		{
			ColInfo: timodel.ColumnInfo{
				FieldType: types.FieldType{
					Tp:   mysql.TypeDouble,
					Flag: mysql.NotNullFlag,
				},
			},
			Res: float64(0),
		},
		// mysql.TypeNewDecimal + notnull + nodefault
		{
			ColInfo: timodel.ColumnInfo{
				FieldType: types.FieldType{
					Tp:      mysql.TypeNewDecimal,
					Flag:    mysql.NotNullFlag,
					Flen:    5,
					Decimal: 2,
				},
			},
			Res: "0", // related with Flen and Decimal
		},
		// mysql.TypeNewDecimal + null + nodefault
		{
			ColInfo: timodel.ColumnInfo{
				FieldType: types.FieldType{
					Tp:      mysql.TypeNewDecimal,
					Flag:    uint(0),
					Flen:    5,
					Decimal: 2,
				},
			},
			Res: nil,
		},
		// mysql.TypeNewDecimal + null + default
		{
			ColInfo: timodel.ColumnInfo{
				OriginDefaultValue: "-3.14", // no float
				FieldType: types.FieldType{
					Tp:      mysql.TypeNewDecimal,
					Flag:    uint(0),
					Flen:    5,
					Decimal: 2,
				},
			},
			Res: "-3.14",
		},
		// mysql.TypeNull
		{
			ColInfo: timodel.ColumnInfo{
				FieldType: types.FieldType{
					Tp: mysql.TypeNull,
				},
			},
			Res: nil,
		},
		// mysql.TypeTimestamp + notnull + nodefault
		{
			ColInfo: timodel.ColumnInfo{
				FieldType: types.FieldType{
					Tp:   mysql.TypeTimestamp,
					Flag: mysql.NotNullFlag,
				},
			},
			Res: "0000-00-00 00:00:00",
		},
		// mysql.TypeTimestamp + notnull + default
		{
			ColInfo: timodel.ColumnInfo{
				OriginDefaultValue: "2020-11-19 12:12:12",
				FieldType: types.FieldType{
					Tp:   mysql.TypeTimestamp,
					Flag: mysql.NotNullFlag,
				},
			},
			Res: "2020-11-19 12:12:12",
		},
		// mysql.TypeTimestamp + null + default
		{
			ColInfo: timodel.ColumnInfo{
				OriginDefaultValue: "2020-11-19 12:12:12",
				FieldType: types.FieldType{
					Tp:   mysql.TypeTimestamp,
					Flag: mysql.NotNullFlag,
				},
			},
			Res: "2020-11-19 12:12:12",
		},
		// mysql.TypeDate, other testCases same as TypeTimestamp
		{
			ColInfo: timodel.ColumnInfo{
				FieldType: types.FieldType{
					Tp:   mysql.TypeDate,
					Flag: mysql.NotNullFlag,
				},
			},
			Res: "0000-00-00",
		},
		// mysql.TypeDuration, other testCases same as TypeTimestamp
		{
			ColInfo: timodel.ColumnInfo{
				FieldType: types.FieldType{
					Tp:   mysql.TypeDuration,
					Flag: mysql.NotNullFlag,
				},
			},
			Res: "00:00:00",
		},
		// mysql.TypeDatetime, other testCases same as TypeTimestamp
		{
			ColInfo: timodel.ColumnInfo{
				FieldType: types.FieldType{
					Tp:   mysql.TypeDatetime,
					Flag: mysql.NotNullFlag,
				},
			},
			Res: "0000-00-00 00:00:00",
		},
		// mysql.TypeYear + notnull + nodefault
		{
			ColInfo: timodel.ColumnInfo{
				FieldType: types.FieldType{
					Tp:   mysql.TypeYear,
					Flag: mysql.NotNullFlag,
				},
			},
			Res: int64(0),
		},
		// mysql.TypeYear + notnull + default
		{
			ColInfo: timodel.ColumnInfo{
				OriginDefaultValue: "2021",
				FieldType: types.FieldType{
					Tp:   mysql.TypeYear,
					Flag: mysql.NotNullFlag,
				},
			},
			// TypeYear default value will be a string and then translate to []byte
			Res: "2021",
		},
		// mysql.TypeNewDate
		{
			ColInfo: timodel.ColumnInfo{
				FieldType: types.FieldType{
					Tp:   mysql.TypeNewDate,
					Flag: mysql.NotNullFlag,
				},
			},
			Res: nil, // [TODO] seems not support by TiDB, need check
		},
		// mysql.TypeVarchar + notnull + nodefault
		{
			ColInfo: timodel.ColumnInfo{
				FieldType: types.FieldType{
					Tp:   mysql.TypeVarchar,
					Flag: mysql.NotNullFlag,
				},
			},
			Res: []byte{},
		},
		// mysql.TypeVarchar + notnull + default
		{
			ColInfo: timodel.ColumnInfo{
				OriginDefaultValue: "e0",
				FieldType: types.FieldType{
					Tp:   mysql.TypeVarchar,
					Flag: mysql.NotNullFlag,
				},
			},
			// TypeVarchar default value will be a string and then translate to []byte
			Res: "e0",
		},
		// mysql.TypeTinyBlob
		{
			ColInfo: timodel.ColumnInfo{
				FieldType: types.FieldType{
					Tp:   mysql.TypeTinyBlob,
					Flag: mysql.NotNullFlag,
				},
			},
			Res: []byte{},
		},
		// mysql.TypeMediumBlob
		{
			ColInfo: timodel.ColumnInfo{
				FieldType: types.FieldType{
					Tp:   mysql.TypeMediumBlob,
					Flag: mysql.NotNullFlag,
				},
			},
			Res: []byte{},
		},
		// mysql.TypeLongBlob
		{
			ColInfo: timodel.ColumnInfo{
				FieldType: types.FieldType{
					Tp:   mysql.TypeLongBlob,
					Flag: mysql.NotNullFlag,
				},
			},
			Res: []byte{},
		},
		// mysql.TypeBlob
		{
			ColInfo: timodel.ColumnInfo{
				FieldType: types.FieldType{
					Tp:   mysql.TypeBlob,
					Flag: mysql.NotNullFlag,
				},
			},
			Res: []byte{},
		},
		// mysql.TypeVarString
		{
			ColInfo: timodel.ColumnInfo{
				FieldType: types.FieldType{
					Tp:   mysql.TypeVarString,
					Flag: mysql.NotNullFlag,
				},
			},
			Res: []byte{},
		},
		// mysql.TypeString
		{
			ColInfo: timodel.ColumnInfo{
				FieldType: types.FieldType{
					Tp:   mysql.TypeString,
					Flag: mysql.NotNullFlag,
				},
			},
			Res: []byte{},
		},
		// mysql.TypeBit
		{
			ColInfo: timodel.ColumnInfo{
				FieldType: types.FieldType{
					Flag: mysql.NotNullFlag,
					Tp:   mysql.TypeBit,
				},
			},
			Res: uint64(0),
		},
		// BLOB, TEXT, GEOMETRY or JSON column can't have a default value
		// mysql.TypeJSON
		{
			ColInfo: timodel.ColumnInfo{
				FieldType: types.FieldType{
					Tp:   mysql.TypeJSON,
					Flag: mysql.NotNullFlag,
				},
			},
			Res: "null",
		},
		// mysql.TypeEnum + notnull + nodefault
		{
			ColInfo: timodel.ColumnInfo{
				FieldType: types.FieldType{
					Tp:    mysql.TypeEnum,
					Flag:  mysql.NotNullFlag,
					Elems: []string{"e0", "e1"},
				},
			},
			// TypeEnum value will be a string and then translate to []byte
			// NotNull && no default will choose first element
			Res: uint64(0),
		},
		// mysql.TypeEnum + notnull + default
		{
			ColInfo: timodel.ColumnInfo{
				OriginDefaultValue: "e1",
				FieldType: types.FieldType{
					Tp:    mysql.TypeEnum,
					Flag:  mysql.NotNullFlag,
					Elems: []string{"e0", "e1"},
				},
			},
			// TypeEnum default value will be a string and then translate to []byte
			Res: "e1",
		},
		// mysql.TypeSet + notnull
		{
			ColInfo: timodel.ColumnInfo{
				FieldType: types.FieldType{
					Tp:   mysql.TypeSet,
					Flag: mysql.NotNullFlag,
				},
			},
			Res: uint64(0),
		},
		// mysql.TypeSet + notnull + default
		{
			ColInfo: timodel.ColumnInfo{
				OriginDefaultValue: "1,e",
				FieldType: types.FieldType{
					Tp:   mysql.TypeSet,
					Flag: mysql.NotNullFlag,
				},
			},
			// TypeSet default value will be a string and then translate to []byte
			Res: "1,e",
		},
		// mysql.TypeGeometry
		{
<<<<<<< HEAD
			ColInfo: timodel.ColumnInfo{
				FieldType: types.FieldType{
					Tp:   mysql.TypeGeometry,
					Flag: mysql.NotNullFlag,
				},
=======
			Name:    "mysql.TypeGeometry",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeGeometryNotNull},
			Res:     nil, // not support yet
			Default: nil,
		},
	}

	for _, tc := range testCases {
		_, val, _, _, _ := getDefaultOrZeroValue(&tc.ColInfo)
		require.Equal(t, tc.Res, val, tc.Name)
		val = getDDLDefaultDefinition(&tc.ColInfo)
		require.Equal(t, tc.Default, val, tc.Name)
	}
}

// TestDecodeEventIgnoreRow tests a PolymorphicEvent.Row is nil
// if this event should be filter out by filter.
func TestDecodeEventIgnoreRow(t *testing.T) {
	helper := NewSchemaTestHelper(t)
	defer helper.Close()
	helper.Tk().MustExec("use test;")

	ddls := []string{
		"create table test.student(id int primary key, name char(50), age int, gender char(10))",
		"create table test.computer(id int primary key, brand char(50), price int)",
		"create table test.poet(id int primary key, name char(50), works char(100))",
	}

	cfID := model.DefaultChangeFeedID("changefeed-test-ignore-event")

	cfg := config.GetDefaultReplicaConfig()
	cfg.Filter.Rules = []string{"test.student", "test.computer"}
	filter, err := filter.NewFilter(cfg, "")
	require.Nil(t, err)
	ver, err := helper.Storage().CurrentVersion(oracle.GlobalTxnScope)
	require.Nil(t, err)
	schemaStorage, err := NewSchemaStorage(helper.GetCurrentMeta(),
		ver.Ver, false, cfID)
	require.Nil(t, err)
	// apply ddl to schemaStorage
	for _, ddl := range ddls {
		job := helper.DDL2Job(ddl)
		err = schemaStorage.HandleDDLJob(job)
		require.Nil(t, err)
	}

	ts := schemaStorage.GetLastSnapshot().CurrentTs()
	schemaStorage.AdvanceResolvedTs(ver.Ver)
	mounter := NewMounter(schemaStorage, cfID, time.Local, filter, true).(*mounter)

	type testCase struct {
		schema  string
		table   string
		columns []interface{}
		ignored bool
	}

	testCases := []testCase{
		{
			schema:  "test",
			table:   "student",
			columns: []interface{}{1, "dongmen", 20, "male"},
			ignored: false,
		},
		{
			schema:  "test",
			table:   "computer",
			columns: []interface{}{1, "apple", 19999},
			ignored: false,
		},
		// This case should be ignored by its table name.
		{
			schema:  "test",
			table:   "poet",
			columns: []interface{}{1, "李白", "静夜思"},
			ignored: true,
		},
	}

	ignoredTables := make([]string, 0)
	tables := make([]string, 0)
	for _, tc := range testCases {
		tableInfo, ok := schemaStorage.GetLastSnapshot().TableByName(tc.schema, tc.table)
		require.True(t, ok)
		// TODO: add other dml event type
		insertSQL := prepareInsertSQL(t, tableInfo, len(tc.columns))
		if tc.ignored {
			ignoredTables = append(ignoredTables, tc.table)
		} else {
			tables = append(tables, tc.table)
		}
		helper.tk.MustExec(insertSQL, tc.columns...)
	}
	ctx := context.Background()

	decodeAndCheckRowInTable := func(tableID int64, f func(key []byte, value []byte) *model.RawKVEntry) int {
		var rows int
		walkTableSpanInStore(t, helper.Storage(), tableID, func(key []byte, value []byte) {
			rawKV := f(key, value)
			pEvent := model.NewPolymorphicEvent(rawKV)
			err := mounter.DecodeEvent(ctx, pEvent)
			require.Nil(t, err)
			if pEvent.Row == nil {
				return
			}
			row := pEvent.Row
			rows++
			require.Equal(t, row.Table.Schema, "test")
			// Now we only allow filter dml event by table, so we only check row's table.
			require.NotContains(t, ignoredTables, row.Table.Table)
			require.Contains(t, tables, row.Table.Table)
		})
		return rows
	}

	toRawKV := func(key []byte, value []byte) *model.RawKVEntry {
		return &model.RawKVEntry{
			OpType:  model.OpTypePut,
			Key:     key,
			Value:   value,
			StartTs: ts - 1,
			CRTs:    ts + 1,
		}
	}

	for _, tc := range testCases {
		tableInfo, ok := schemaStorage.GetLastSnapshot().TableByName(tc.schema, tc.table)
		require.True(t, ok)
		decodeAndCheckRowInTable(tableInfo.ID, toRawKV)
	}
}

func TestBuildTableInfo(t *testing.T) {
	cases := []struct {
		origin              string
		recovered           string
		recoveredWithNilCol string
	}{
		{
			"CREATE TABLE t1 (c INT PRIMARY KEY)",
			"CREATE TABLE `BuildTiDBTableInfo` (\n" +
				"  `c` int(0) NOT NULL,\n" +
				"  PRIMARY KEY (`c`(0)) /*T![clustered_index] CLUSTERED */\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
			"CREATE TABLE `BuildTiDBTableInfo` (\n" +
				"  `c` int(0) NOT NULL,\n" +
				"  PRIMARY KEY (`c`(0)) /*T![clustered_index] CLUSTERED */\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
		},
		{
			"CREATE TABLE t1 (" +
				" c INT UNSIGNED," +
				" c2 VARCHAR(10) NOT NULL," +
				" c3 BIT(10) NOT NULL," +
				" UNIQUE KEY (c2, c3)" +
				")",
			// CDC discards field length.
			"CREATE TABLE `BuildTiDBTableInfo` (\n" +
				"  `c` int(0) unsigned DEFAULT NULL,\n" +
				"  `c2` varchar(0) NOT NULL,\n" +
				"  `c3` bit(0) NOT NULL,\n" +
				"  UNIQUE KEY `idx_0` (`c2`(0),`c3`(0))\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
			"CREATE TABLE `BuildTiDBTableInfo` (\n" +
				"  `omitted` unspecified GENERATED ALWAYS AS (pass_generated_check) VIRTUAL,\n" +
				"  `c2` varchar(0) NOT NULL,\n" +
				"  `c3` bit(0) NOT NULL,\n" +
				"  UNIQUE KEY `idx_0` (`c2`(0),`c3`(0))\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
		},
		{
			"CREATE TABLE t1 (" +
				" c INT UNSIGNED," +
				" gen INT AS (c+1) VIRTUAL," +
				" c2 VARCHAR(10) NOT NULL," +
				" gen2 INT AS (c+2) STORED," +
				" c3 BIT(10) NOT NULL," +
				" PRIMARY KEY (c, c2)" +
				")",
			// CDC discards virtual generated column, and generating expression of stored generated column.
			"CREATE TABLE `BuildTiDBTableInfo` (\n" +
				"  `c` int(0) unsigned NOT NULL,\n" +
				"  `c2` varchar(0) NOT NULL,\n" +
				"  `gen2` int(0) GENERATED ALWAYS AS (pass_generated_check) STORED,\n" +
				"  `c3` bit(0) NOT NULL,\n" +
				"  PRIMARY KEY (`c`(0),`c2`(0)) /*T![clustered_index] CLUSTERED */\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
			"CREATE TABLE `BuildTiDBTableInfo` (\n" +
				"  `c` int(0) unsigned NOT NULL,\n" +
				"  `c2` varchar(0) NOT NULL,\n" +
				"  `omitted` unspecified GENERATED ALWAYS AS (pass_generated_check) VIRTUAL,\n" +
				"  `omitted` unspecified GENERATED ALWAYS AS (pass_generated_check) VIRTUAL,\n" +
				"  PRIMARY KEY (`c`(0),`c2`(0)) /*T![clustered_index] CLUSTERED */\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
		},
		{
			"CREATE TABLE `t1` (" +
				"  `a` int(11) NOT NULL," +
				"  `b` int(11) DEFAULT NULL," +
				"  `c` int(11) DEFAULT NULL," +
				"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */," +
				"  UNIQUE KEY `b` (`b`)" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
			"CREATE TABLE `BuildTiDBTableInfo` (\n" +
				"  `a` int(0) NOT NULL,\n" +
				"  `b` int(0) DEFAULT NULL,\n" +
				"  `c` int(0) DEFAULT NULL,\n" +
				"  PRIMARY KEY (`a`(0)) /*T![clustered_index] CLUSTERED */,\n" +
				"  UNIQUE KEY `idx_1` (`b`(0))\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
			"CREATE TABLE `BuildTiDBTableInfo` (\n" +
				"  `a` int(0) NOT NULL,\n" +
				"  `omitted` unspecified GENERATED ALWAYS AS (pass_generated_check) VIRTUAL,\n" +
				"  `omitted` unspecified GENERATED ALWAYS AS (pass_generated_check) VIRTUAL,\n" +
				"  PRIMARY KEY (`a`(0)) /*T![clustered_index] CLUSTERED */\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
		},
	}
	p := parser.New()
	for _, c := range cases {
		stmt, err := p.ParseOneStmt(c.origin, "", "")
		require.NoError(t, err)
		originTI, err := ddl.BuildTableInfoFromAST(stmt.(*ast.CreateTableStmt))
		require.NoError(t, err)
		cdcTableInfo := model.WrapTableInfo(0, "test", 0, originTI)
		cols, _, err := datum2Column(cdcTableInfo, map[int64]types.Datum{}, true)
		require.NoError(t, err)
		recoveredTI := model.BuildTiDBTableInfo(cols, cdcTableInfo.IndexColumnsOffset)
		handle := sqlmodel.GetWhereHandle(recoveredTI, recoveredTI)
		require.NotNil(t, handle.UniqueNotNullIdx)
		require.Equal(t, c.recovered, showCreateTable(t, recoveredTI))

		// mimic the columns are set to nil when old value feature is disabled
		for i := range cols {
			if !cols[i].Flag.IsHandleKey() {
				cols[i] = nil
			}
		}
		recoveredTI = model.BuildTiDBTableInfo(cols, cdcTableInfo.IndexColumnsOffset)
		handle = sqlmodel.GetWhereHandle(recoveredTI, recoveredTI)
		require.NotNil(t, handle.UniqueNotNullIdx)
		require.Equal(t, c.recoveredWithNilCol, showCreateTable(t, recoveredTI))
	}
}

var tiCtx = mock.NewContext()

func showCreateTable(t *testing.T, ti *timodel.TableInfo) string {
	result := bytes.NewBuffer(make([]byte, 0, 512))
	err := executor.ConstructResultOfShowCreateTable(tiCtx, ti, autoid.Allocators{}, result)
	require.NoError(t, err)
	return result.String()
}

func TestNewDMRowChange(t *testing.T) {
	cases := []struct {
		origin    string
		recovered string
	}{
		{
			"CREATE TABLE t1 (id INT," +
				" a1 INT NOT NULL," +
				" a3 INT NOT NULL," +
				" UNIQUE KEY dex1(a1, a3));",
			"CREATE TABLE `BuildTiDBTableInfo` (\n" +
				"  `id` int(0) DEFAULT NULL,\n" +
				"  `a1` int(0) NOT NULL,\n" +
				"  `a3` int(0) NOT NULL,\n" +
				"  UNIQUE KEY `idx_0` (`a1`(0),`a3`(0))\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
		},
	}
	p := parser.New()
	for _, c := range cases {
		stmt, err := p.ParseOneStmt(c.origin, "", "")
		require.NoError(t, err)
		originTI, err := ddl.BuildTableInfoFromAST(stmt.(*ast.CreateTableStmt))
		require.NoError(t, err)
		cdcTableInfo := model.WrapTableInfo(0, "test", 0, originTI)
		cols := []*model.Column{
			{
				Name: "id", Type: 3, Charset: "binary", Flag: 65, Value: 1, Default: nil,
>>>>>>> d30f48b689 (mounter(ticdc): mount float32 value correctly to avoid the precision lost. (#8502))
			},
			Res: nil, // not support yet
		},
	}
	testGetDefaultZeroValue(t, colAndRess)
}

func testGetDefaultZeroValue(t *testing.T, colAndRess []columnInfoAndResult) {
	for _, colAndRes := range colAndRess {
		val, _, _ := getDefaultOrZeroValue(&colAndRes.ColInfo)
		require.Equal(t, colAndRes.Res, val)
	}
}

func TestFormatColVal(t *testing.T) {
	t.Parallel()

	ftTypeFloatNotNull := types.NewFieldType(mysql.TypeFloat)
	ftTypeFloatNotNull.SetFlag(mysql.NotNullFlag)
	col := &timodel.ColumnInfo{FieldType: *ftTypeFloatNotNull}

	var datum types.Datum

	datum.SetFloat32(123.99)
	value, _, _, err := formatColVal(datum, col)
	require.NoError(t, err)
	require.EqualValues(t, float32(123.99), value)

	datum.SetFloat32(float32(math.NaN()))
	value, _, warn, err := formatColVal(datum, col)
	require.NoError(t, err)
	require.Equal(t, float32(0), value)
	require.NotZero(t, warn)

	datum.SetFloat32(float32(math.Inf(1)))
	value, _, warn, err = formatColVal(datum, col)
	require.NoError(t, err)
	require.Equal(t, float32(0), value)
	require.NotZero(t, warn)

	datum.SetFloat32(float32(math.Inf(-1)))
	value, _, warn, err = formatColVal(datum, col)
	require.NoError(t, err)
	require.Equal(t, float32(0), value)
	require.NotZero(t, warn)
}
