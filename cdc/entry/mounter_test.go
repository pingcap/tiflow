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
	"github.com/pingcap/tiflow/pkg/config"
	pfilter "github.com/pingcap/tiflow/pkg/filter"
	"github.com/pingcap/tiflow/pkg/regionspan"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

var dummyChangeFeedID = model.DefaultChangeFeedID("dummy_changefeed")

func TestMounterDisableOldValue(t *testing.T) {
	testCases := []struct {
		tableName      string
		createTableDDL string
		// [] for rows, []infterface{} for columns.
		values [][]interface{}
		// [] for table partition if there is any,
		// []int for approximateBytes of rows.
		putApproximateBytes [][]int
		delApproximateBytes [][]int
	}{{
		tableName:           "simple",
		createTableDDL:      "create table simple(id int primary key)",
		values:              [][]interface{}{{1}, {2}, {3}, {4}, {5}},
		putApproximateBytes: [][]int{{346, 346, 346, 346, 346}},
		delApproximateBytes: [][]int{{346, 346, 346, 346, 346}},
	}, {
		tableName:           "no_pk",
		createTableDDL:      "create table no_pk(id int not null unique key)",
		values:              [][]interface{}{{1}, {2}, {3}, {4}, {5}},
		putApproximateBytes: [][]int{{345, 345, 345, 345, 345}},
		delApproximateBytes: [][]int{{217, 217, 217, 217, 217}},
	}, {
		tableName:           "many_index",
		createTableDDL:      "create table many_index(id int not null unique key, c1 int unique key, c2 int, INDEX (c2))",
		values:              [][]interface{}{{1, 1, 1}, {2, 2, 2}, {3, 3, 3}, {4, 4, 4}, {5, 5, 5}},
		putApproximateBytes: [][]int{{638, 638, 638, 638, 638}},
		delApproximateBytes: [][]int{{254, 254, 254, 254, 254}},
	}, {
		tableName:           "default_value",
		createTableDDL:      "create table default_value(id int primary key, c1 int, c2 int not null default 5, c3 varchar(20), c4 varchar(20) not null default '666')",
		values:              [][]interface{}{{1}, {2}, {3}, {4}, {5}},
		putApproximateBytes: [][]int{{676, 676, 676, 676, 676}},
		delApproximateBytes: [][]int{{353, 353, 353, 353, 353}},
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
		putApproximateBytes: [][]int{{775}, {777}, {777}, {777, 777}},
		delApproximateBytes: [][]int{{227}, {227}, {227}, {227, 227}},
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
		putApproximateBytes: [][]int{{986, 626, 986, 986, 986}},
		delApproximateBytes: [][]int{{346, 346, 346, 346, 346}},
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
		putApproximateBytes: [][]int{{1019, 1459, 1411, 1323, 1398, 1369}},
		delApproximateBytes: [][]int{{347, 347, 347, 347, 347, 347}},
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
		putApproximateBytes: [][]int{{627, 819}},
		delApproximateBytes: [][]int{{347, 347}},
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
		putApproximateBytes: [][]int{{563, 551}},
		delApproximateBytes: [][]int{{347, 347}},
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
		putApproximateBytes: [][]int{{636, 624}},
		delApproximateBytes: [][]int{{348, 348}},
	}, {
		tableName:      "clustered_index1",
		createTableDDL: "CREATE TABLE clustered_index1 (id VARCHAR(255) PRIMARY KEY, data INT);",
		values: [][]interface{}{
			{"hhh"},
			{"你好😘", 666},
			{"世界🤪", 888},
		},
		putApproximateBytes: [][]int{{383, 446, 446}},
		delApproximateBytes: [][]int{{311, 318, 318}},
	}, {
		tableName:      "clustered_index2",
		createTableDDL: "CREATE TABLE clustered_index2 (id VARCHAR(255), data INT, ddaa date, PRIMARY KEY (id, data, ddaa), UNIQUE KEY (id, data, ddaa));",
		values: [][]interface{}{
			{"你好😘", 666, "2020-11-20"},
			{"世界🤪", 888, "2020-05-12"},
		},
		putApproximateBytes: [][]int{{592, 592}},
		delApproximateBytes: [][]int{{592, 592}},
	}}
	for _, tc := range testCases {
		testMounterDisableOldValue(t, tc)
	}
}

func testMounterDisableOldValue(t *testing.T, tc struct {
	tableName           string
	createTableDDL      string
	values              [][]interface{}
	putApproximateBytes [][]int
	delApproximateBytes [][]int
},
) {
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
	scheamStorage, err := NewSchemaStorage(nil, 0, nil, false, dummyChangeFeedID)
	require.Nil(t, err)
	for _, job := range jobs {
		err := scheamStorage.HandleDDLJob(job)
		require.Nil(t, err)
	}
	tableInfo, ok := scheamStorage.GetLastSnapshot().TableByName("test", tc.tableName)
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
	config := config.GetDefaultReplicaConfig()
	filter, err := pfilter.NewFilter(config)
	require.Nil(t, err)
	mounter := NewMounter(scheamStorage,
		model.DefaultChangeFeedID("c1"),
		time.UTC, filter, false).(*mounterImpl)
	mounter.tz = time.Local
	ctx := context.Background()

	// [TODO] check size and readd rowBytes
	mountAndCheckRowInTable := func(tableID int64, _ []int, f func(key []byte, value []byte) *model.RawKVEntry) int {
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
			// [TODO] check size and reopen this check
			// require.Equal(t, rowBytes[rows-1], row.ApproximateBytes(), row)
			t.Log("ApproximateBytes", tc.tableName, rows-1, row.ApproximateBytes())
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
	mountAndCheckRow := func(rowsBytes [][]int, f func(key []byte, value []byte) *model.RawKVEntry) int {
		partitionInfo := tableInfo.GetPartitionInfo()
		if partitionInfo == nil {
			return mountAndCheckRowInTable(tableInfo.ID, rowsBytes[0], f)
		}
		var rows int
		for i, p := range partitionInfo.Definitions {
			rows += mountAndCheckRowInTable(p.ID, rowsBytes[i], f)
		}
		return rows
	}

	rows := mountAndCheckRow(tc.putApproximateBytes, func(key []byte, value []byte) *model.RawKVEntry {
		return &model.RawKVEntry{
			OpType:  model.OpTypePut,
			Key:     key,
			Value:   value,
			StartTs: ver.Ver - 1,
			CRTs:    ver.Ver,
		}
	})
	require.Equal(t, rows, len(tc.values))

	rows = mountAndCheckRow(tc.delApproximateBytes, func(key []byte, value []byte) *model.RawKVEntry {
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
		if col == nil {
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

// We use OriginDefaultValue instead of DefaultValue in the ut, pls ref to
// https://github.com/pingcap/tiflow/issues/4048
// FIXME: OriginDefaultValue seems always to be string, and test more corner case
// Ref: https://github.com/pingcap/tidb/blob/d2c352980a43bb593db81fd1db996f47af596d91/table/column.go#L489
func TestGetDefaultZeroValue(t *testing.T) {
	// Check following MySQL type, ref to:
	// https://github.com/pingcap/tidb/blob/master/parser/mysql/type.go

	// mysql flag null
	ftNull := types.NewFieldType(mysql.TypeUnspecified)

	// mysql.TypeTiny + notnull
	ftTinyIntNotNull := types.NewFieldType(mysql.TypeTiny)
	ftTinyIntNotNull.AddFlag(mysql.NotNullFlag)

	// mysql.TypeTiny + notnull +  unsigned
	ftTinyIntNotNullUnSigned := types.NewFieldType(mysql.TypeTiny)
	ftTinyIntNotNullUnSigned.SetFlag(mysql.NotNullFlag)
	ftTinyIntNotNullUnSigned.AddFlag(mysql.UnsignedFlag)

	// mysql.TypeTiny + null
	ftTinyIntNull := types.NewFieldType(mysql.TypeTiny)

	// mysql.TypeShort + notnull
	ftShortNotNull := types.NewFieldType(mysql.TypeShort)
	ftShortNotNull.SetFlag(mysql.NotNullFlag)

	// mysql.TypeLong + notnull
	ftLongNotNull := types.NewFieldType(mysql.TypeLong)
	ftLongNotNull.SetFlag(mysql.NotNullFlag)

	// mysql.TypeLonglong + notnull
	ftLongLongNotNull := types.NewFieldType(mysql.TypeLonglong)
	ftLongLongNotNull.SetFlag(mysql.NotNullFlag)

	// mysql.TypeInt24 + notnull
	ftInt24NotNull := types.NewFieldType(mysql.TypeInt24)
	ftInt24NotNull.SetFlag(mysql.NotNullFlag)

	// mysql.TypeFloat + notnull
	ftTypeFloatNotNull := types.NewFieldType(mysql.TypeFloat)
	ftTypeFloatNotNull.SetFlag(mysql.NotNullFlag)

	// mysql.TypeFloat + notnull + unsigned
	ftTypeFloatNotNullUnSigned := types.NewFieldType(mysql.TypeFloat)
	ftTypeFloatNotNullUnSigned.SetFlag(mysql.NotNullFlag | mysql.UnsignedFlag)

	// mysql.TypeFloat + null
	ftTypeFloatNull := types.NewFieldType(mysql.TypeFloat)

	// mysql.TypeDouble + notnull
	ftTypeDoubleNotNull := types.NewFieldType(mysql.TypeDouble)
	ftTypeDoubleNotNull.SetFlag(mysql.NotNullFlag)

	// mysql.TypeNewDecimal + notnull
	ftTypeNewDecimalNull := types.NewFieldType(mysql.TypeNewDecimal)
	ftTypeNewDecimalNull.SetFlen(5)
	ftTypeNewDecimalNull.SetDecimal(2)

	// mysql.TypeNewDecimal + notnull
	ftTypeNewDecimalNotNull := types.NewFieldType(mysql.TypeNewDecimal)
	ftTypeNewDecimalNotNull.SetFlag(mysql.NotNullFlag)
	ftTypeNewDecimalNotNull.SetFlen(5)
	ftTypeNewDecimalNotNull.SetDecimal(2)

	// mysql.TypeNull
	ftTypeNull := types.NewFieldType(mysql.TypeNull)

	// mysql.TypeTimestamp + notnull
	ftTypeTimestampNotNull := types.NewFieldType(mysql.TypeTimestamp)
	ftTypeTimestampNotNull.SetFlag(mysql.NotNullFlag)

	// mysql.TypeTimestamp + notnull
	ftTypeTimestampNull := types.NewFieldType(mysql.TypeTimestamp)

	// mysql.TypeDate + notnull
	ftTypeDateNotNull := types.NewFieldType(mysql.TypeDate)
	ftTypeDateNotNull.SetFlag(mysql.NotNullFlag)

	// mysql.TypeDuration + notnull
	ftTypeDurationNotNull := types.NewFieldType(mysql.TypeDuration)
	ftTypeDurationNotNull.SetFlag(mysql.NotNullFlag)

	// mysql.TypeDatetime + notnull
	ftTypeDatetimeNotNull := types.NewFieldType(mysql.TypeDatetime)
	ftTypeDatetimeNotNull.SetFlag(mysql.NotNullFlag)

	// mysql.TypeYear + notnull
	ftTypeYearNotNull := types.NewFieldType(mysql.TypeYear)
	ftTypeYearNotNull.SetFlag(mysql.NotNullFlag)

	// mysql.TypeNewDate + notnull
	ftTypeNewDateNotNull := types.NewFieldType(mysql.TypeNewDate)
	ftTypeNewDateNotNull.SetFlag(mysql.NotNullFlag)

	// mysql.TypeVarchar + notnull
	ftTypeVarcharNotNull := types.NewFieldType(mysql.TypeVarchar)
	ftTypeVarcharNotNull.SetFlag(mysql.NotNullFlag)

	// mysql.TypeTinyBlob + notnull
	ftTypeTinyBlobNotNull := types.NewFieldType(mysql.TypeTinyBlob)
	ftTypeTinyBlobNotNull.SetFlag(mysql.NotNullFlag)

	// mysql.TypeMediumBlob + notnull
	ftTypeMediumBlobNotNull := types.NewFieldType(mysql.TypeMediumBlob)
	ftTypeMediumBlobNotNull.SetFlag(mysql.NotNullFlag)

	// mysql.TypeLongBlob + notnull
	ftTypeLongBlobNotNull := types.NewFieldType(mysql.TypeLongBlob)
	ftTypeLongBlobNotNull.SetFlag(mysql.NotNullFlag)

	// mysql.TypeBlob + notnull
	ftTypeBlobNotNull := types.NewFieldType(mysql.TypeBlob)
	ftTypeBlobNotNull.SetFlag(mysql.NotNullFlag)

	// mysql.TypeVarString + notnull
	ftTypeVarStringNotNull := types.NewFieldType(mysql.TypeVarString)
	ftTypeVarStringNotNull.SetFlag(mysql.NotNullFlag)

	// mysql.TypeString + notnull
	ftTypeStringNotNull := types.NewFieldType(mysql.TypeString)
	ftTypeStringNotNull.SetFlag(mysql.NotNullFlag)

	// mysql.TypeBit + notnull
	ftTypeBitNotNull := types.NewFieldType(mysql.TypeBit)
	ftTypeBitNotNull.SetFlag(mysql.NotNullFlag)

	// mysql.TypeJSON + notnull
	ftTypeJSONNotNull := types.NewFieldType(mysql.TypeJSON)
	ftTypeJSONNotNull.SetFlag(mysql.NotNullFlag)

	// mysql.TypeEnum + notnull + nodefault
	ftTypeEnumNotNull := types.NewFieldType(mysql.TypeEnum)
	ftTypeEnumNotNull.SetFlag(mysql.NotNullFlag)
	ftTypeEnumNotNull.SetElems([]string{"e0", "e1"})

	// mysql.TypeSet + notnull
	ftTypeSetNotNull := types.NewFieldType(mysql.TypeSet)
	ftTypeSetNotNull.SetFlag(mysql.NotNullFlag)

	// mysql.TypeGeometry + notnull
	ftTypeGeometryNotNull := types.NewFieldType(mysql.TypeGeometry)
	ftTypeGeometryNotNull.SetFlag(mysql.NotNullFlag)

	testCases := []struct {
		Name    string
		ColInfo timodel.ColumnInfo
		Res     interface{}
		Default interface{}
	}{
		// mysql flag null
		{
			Name:    "mysql flag null",
			ColInfo: timodel.ColumnInfo{FieldType: *ftNull},
			Res:     nil,
			Default: nil,
		},
		// mysql.TypeTiny + notnull + nodefault
		{
			Name:    "mysql.TypeTiny + notnull + nodefault",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTinyIntNotNull.Clone()},
			Res:     int64(0),
			Default: nil,
		},
		// mysql.TypeTiny + notnull + default
		{
			Name: "mysql.TypeTiny + notnull + default",
			ColInfo: timodel.ColumnInfo{
				OriginDefaultValue: -1314,
				FieldType:          *ftTinyIntNotNull,
			},
			Res:     int64(-1314),
			Default: int64(-1314),
		},
		// mysql.TypeTiny + notnull + unsigned
		{
			Name:    "mysql.TypeTiny + notnull + default + unsigned",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTinyIntNotNullUnSigned},
			Res:     uint64(0),
			Default: nil,
		},
		// mysql.TypeTiny + notnull + default + unsigned
		{
			Name:    "mysql.TypeTiny + notnull + unsigned",
			ColInfo: timodel.ColumnInfo{OriginDefaultValue: uint64(1314), FieldType: *ftTinyIntNotNullUnSigned},
			Res:     uint64(1314),
			Default: uint64(1314),
		},
		// mysql.TypeTiny + null + default
		{
			Name: "mysql.TypeTiny + null + default",
			ColInfo: timodel.ColumnInfo{
				OriginDefaultValue: -1314,
				FieldType:          *ftTinyIntNull,
			},
			Res:     int64(-1314),
			Default: int64(-1314),
		},
		// mysql.TypeTiny + null + nodefault
		{
			Name:    "mysql.TypeTiny + null + nodefault",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTinyIntNull},
			Res:     nil,
			Default: nil,
		},
		// mysql.TypeShort, others testCases same as tiny
		{
			Name:    "mysql.TypeShort, others testCases same as tiny",
			ColInfo: timodel.ColumnInfo{FieldType: *ftShortNotNull},
			Res:     int64(0),
			Default: nil,
		},
		// mysql.TypeLong, others testCases same as tiny
		{
			Name:    "mysql.TypeLong, others testCases same as tiny",
			ColInfo: timodel.ColumnInfo{FieldType: *ftLongNotNull},
			Res:     int64(0),
			Default: nil,
		},
		// mysql.TypeLonglong, others testCases same as tiny
		{
			Name:    "mysql.TypeLonglong, others testCases same as tiny",
			ColInfo: timodel.ColumnInfo{FieldType: *ftLongLongNotNull},
			Res:     int64(0),
			Default: nil,
		},
		// mysql.TypeInt24, others testCases same as tiny
		{
			Name:    "mysql.TypeInt24, others testCases same as tiny",
			ColInfo: timodel.ColumnInfo{FieldType: *ftInt24NotNull},
			Res:     int64(0),
			Default: nil,
		},
		// mysql.TypeFloat + notnull + nodefault
		{
			Name:    "mysql.TypeFloat + notnull + nodefault",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeFloatNotNull},
			Res:     float64(0),
			Default: nil,
		},
		// mysql.TypeFloat + notnull + default
		{
			Name: "mysql.TypeFloat + notnull + default",
			ColInfo: timodel.ColumnInfo{
				OriginDefaultValue: -3.1415,
				FieldType:          *ftTypeFloatNotNull,
			},
			Res:     float64(-3.1415),
			Default: float64(-3.1415),
		},
		// mysql.TypeFloat + notnull + default + unsigned
		{
			Name: "mysql.TypeFloat + notnull + default + unsigned",
			ColInfo: timodel.ColumnInfo{
				OriginDefaultValue: 3.1415,
				FieldType:          *ftTypeFloatNotNullUnSigned,
			},
			Res:     float64(3.1415),
			Default: float64(3.1415),
		},
		// mysql.TypeFloat + notnull + unsigned
		{
			Name: "mysql.TypeFloat + notnull + unsigned",
			ColInfo: timodel.ColumnInfo{
				FieldType: *ftTypeFloatNotNullUnSigned,
			},
			Res:     float64(0),
			Default: nil,
		},
		// mysql.TypeFloat + null + default
		{
			Name: "mysql.TypeFloat + null + default",
			ColInfo: timodel.ColumnInfo{
				OriginDefaultValue: -3.1415,
				FieldType:          *ftTypeFloatNull,
			},
			Res:     float64(-3.1415),
			Default: float64(-3.1415),
		},
		// mysql.TypeFloat + null + nodefault
		{
			Name: "mysql.TypeFloat + null + nodefault",
			ColInfo: timodel.ColumnInfo{
				FieldType: *ftTypeFloatNull,
			},
			Res:     nil,
			Default: nil,
		},
		// mysql.TypeDouble, other testCases same as float
		{
			Name:    "mysql.TypeDouble, other testCases same as float",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeDoubleNotNull},
			Res:     float64(0),
			Default: nil,
		},
		// mysql.TypeNewDecimal + notnull + nodefault
		{
			Name:    "mysql.TypeNewDecimal + notnull + nodefault",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeNewDecimalNotNull},
			Res:     "0", // related with Flen and Decimal
			Default: nil,
		},
		// mysql.TypeNewDecimal + null + nodefault
		{
			Name:    "mysql.TypeNewDecimal + null + nodefault",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeNewDecimalNull},
			Res:     nil,
			Default: nil,
		},
		// mysql.TypeNewDecimal + null + default
		{
			Name: "mysql.TypeNewDecimal + null + default",
			ColInfo: timodel.ColumnInfo{
				OriginDefaultValue: "-3.14", // no float
				FieldType:          *ftTypeNewDecimalNotNull,
			},
			Res:     "-3.14",
			Default: "-3.14",
		},
		// mysql.TypeNull
		{
			Name:    "mysql.TypeNull",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeNull},
			Res:     nil,
			Default: nil,
		},
		// mysql.TypeTimestamp + notnull + nodefault
		{
			Name:    "mysql.TypeTimestamp + notnull + nodefault",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeTimestampNotNull},
			Res:     "0000-00-00 00:00:00",
			Default: nil,
		},
		// mysql.TypeTimestamp + notnull + default
		{
			Name: "mysql.TypeTimestamp + notnull + default",
			ColInfo: timodel.ColumnInfo{
				OriginDefaultValue: "2020-11-19 12:12:12",
				FieldType:          *ftTypeTimestampNotNull,
			},
			Res:     "2020-11-19 12:12:12",
			Default: "2020-11-19 12:12:12",
		},
		// mysql.TypeTimestamp + null + default
		{
			Name: "mysql.TypeTimestamp + null + default",
			ColInfo: timodel.ColumnInfo{
				OriginDefaultValue: "2020-11-19 12:12:12",
				FieldType:          *ftTypeTimestampNull,
			},
			Res:     "2020-11-19 12:12:12",
			Default: "2020-11-19 12:12:12",
		},
		// mysql.TypeDate, other testCases same as TypeTimestamp
		{
			Name:    "mysql.TypeDate, other testCases same as TypeTimestamp",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeDateNotNull},
			Res:     "0000-00-00",
			Default: nil,
		},
		// mysql.TypeDuration, other testCases same as TypeTimestamp
		{
			Name:    "mysql.TypeDuration, other testCases same as TypeTimestamp",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeDurationNotNull},
			Res:     "00:00:00",
			Default: nil,
		},
		// mysql.TypeDatetime, other testCases same as TypeTimestamp
		{
			Name:    "mysql.TypeDatetime, other testCases same as TypeTimestamp",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeDatetimeNotNull},
			Res:     "0000-00-00 00:00:00",
			Default: nil,
		},
		// mysql.TypeYear + notnull + nodefault
		{
			Name:    "mysql.TypeYear + notnull + nodefault",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeYearNotNull},
			Res:     int64(0),
			Default: nil,
		},
		// mysql.TypeYear + notnull + default
		{
			Name: "mysql.TypeYear + notnull + default",
			ColInfo: timodel.ColumnInfo{
				OriginDefaultValue: "2021",
				FieldType:          *ftTypeYearNotNull,
			},
			// TypeYear default value will be a string and then translate to []byte
			Res:     "2021",
			Default: "2021",
		},
		// mysql.TypeNewDate
		{
			Name:    "mysql.TypeNewDate",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeNewDateNotNull},
			Res:     nil, // [TODO] seems not support by TiDB, need check
			Default: nil,
		},
		// mysql.TypeVarchar + notnull + nodefault
		{
			Name:    "mysql.TypeVarchar + notnull + nodefault",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeVarcharNotNull},
			Res:     []byte{},
			Default: nil,
		},
		// mysql.TypeVarchar + notnull + default
		{
			Name: "mysql.TypeVarchar + notnull + default",
			ColInfo: timodel.ColumnInfo{
				OriginDefaultValue: "e0",
				FieldType:          *ftTypeVarcharNotNull,
			},
			// TypeVarchar default value will be a string and then translate to []byte
			Res:     "e0",
			Default: "e0",
		},
		// mysql.TypeTinyBlob
		{
			Name:    "mysql.TypeTinyBlob",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeTinyBlobNotNull},
			Res:     []byte{},
			Default: nil,
		},
		// mysql.TypeMediumBlob
		{
			Name:    "mysql.TypeMediumBlob",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeMediumBlobNotNull},
			Res:     []byte{},
			Default: nil,
		},
		// mysql.TypeLongBlob
		{
			Name:    "mysql.TypeLongBlob",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeLongBlobNotNull},
			Res:     []byte{},
			Default: nil,
		},
		// mysql.TypeBlob
		{
			Name:    "mysql.TypeBlob",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeBlobNotNull},
			Res:     []byte{},
			Default: nil,
		},
		// mysql.TypeVarString
		{
			Name:    "mysql.TypeVarString",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeVarStringNotNull},
			Res:     []byte{},
			Default: nil,
		},
		// mysql.TypeString
		{
			Name:    "mysql.TypeString",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeStringNotNull},
			Res:     []byte{},
			Default: nil,
		},
		// mysql.TypeBit
		{
			Name:    "mysql.TypeBit",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeBitNotNull},
			Res:     uint64(0),
			Default: nil,
		},
		// BLOB, TEXT, GEOMETRY or JSON column can't have a default value
		// mysql.TypeJSON
		{
			Name:    "mysql.TypeJSON",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeJSONNotNull},
			Res:     "null",
			Default: nil,
		},
		// mysql.TypeEnum + notnull + nodefault
		{
			Name:    "mysql.TypeEnum + notnull + nodefault",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeEnumNotNull},
			// TypeEnum value will be a string and then translate to []byte
			// NotNull && no default will choose first element
			Res:     uint64(0),
			Default: nil,
		},
		// mysql.TypeEnum + notnull + default
		{
			Name: "mysql.TypeEnum + notnull + default",
			ColInfo: timodel.ColumnInfo{
				OriginDefaultValue: "e1",
				FieldType:          *ftTypeEnumNotNull,
			},
			// TypeEnum default value will be a string and then translate to []byte
			Res:     "e1",
			Default: "e1",
		},
		// mysql.TypeSet + notnull
		{
			Name:    "mysql.TypeSet + notnull",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeSetNotNull},
			Res:     uint64(0),
			Default: nil,
		},
		// mysql.TypeSet + notnull + default
		{
			Name: "mysql.TypeSet + notnull + default",
			ColInfo: timodel.ColumnInfo{
				OriginDefaultValue: "1,e",
				FieldType:          *ftTypeSetNotNull,
			},
			// TypeSet default value will be a string and then translate to []byte
			Res:     "1,e",
			Default: "1,e",
		},
		// mysql.TypeGeometry
		{
			Name:    "mysql.TypeGeometry",
			ColInfo: timodel.ColumnInfo{FieldType: *ftTypeGeometryNotNull},
			Res:     nil, // not support yet
			Default: nil,
		},
	}

	for _, tc := range testCases {
		val, _, _, _ := getDefaultOrZeroValue(&tc.ColInfo)
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
	filter, err := pfilter.NewFilter(cfg)
	require.Nil(t, err)
	ver, err := helper.Storage().CurrentVersion(oracle.GlobalTxnScope)
	require.Nil(t, err)
	schemaStorage, err := NewSchemaStorage(helper.GetCurrentMeta(),
		ver.Ver, filter, false, cfID)
	require.Nil(t, err)

	// apply ddl to schemaStorage
	for _, ddl := range ddls {
		job := helper.DDL2Job(ddl)
		err = schemaStorage.HandleDDLJob(job)
		require.Nil(t, err)
	}

	ts := schemaStorage.GetLastSnapshot().CurrentTs()
	schemaStorage.AdvanceResolvedTs(ver.Ver)
	mounter := NewMounter(schemaStorage, cfID, time.Local, filter, true).(*mounterImpl)

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
			ignored, err := mounter.DecodeEvent(ctx, pEvent)
			require.Nil(t, err)
			if ignored {
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
