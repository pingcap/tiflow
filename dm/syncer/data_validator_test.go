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

package syncer

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/pingcap/tiflow/dm/pkg/log"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	parsermysql "github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"github.com/pingcap/tiflow/dm/pkg/binlog/event"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	"github.com/pingcap/tiflow/dm/pkg/gtid"
	"github.com/pingcap/tiflow/dm/syncer/dbconn"
)

type testDataValidatorSuite struct {
	eventsGenerator *event.Generator
	cfg             *config.SubTaskConfig
}

var _ = Suite(&testDataValidatorSuite{})

func (d *testDataValidatorSuite) SetUpSuite(c *C) {
	previousGTIDSetStr := "3ccc475b-2343-11e7-be21-6c0b84d59f30:1-14,406a3f61-690d-11e7-87c5-6c92bf46f384:1-94321383"
	previousGTIDSet, err := gtid.ParserGTID(mysql.MySQLFlavor, previousGTIDSetStr)
	if err != nil {
		c.Fatal(err)
	}
	latestGTIDStr := "3ccc475b-2343-11e7-be21-6c0b84d59f30:14"
	latestGTID, err := gtid.ParserGTID(mysql.MySQLFlavor, latestGTIDStr)
	c.Assert(err, IsNil)
	d.eventsGenerator, err = event.NewGenerator(mysql.MySQLFlavor, 1, 0, latestGTID, previousGTIDSet, 0)
	if err != nil {
		c.Fatal(err)
	}
	loaderDir, err := os.MkdirTemp("", "loader")
	c.Assert(err, IsNil)
	loaderCfg := config.LoaderConfig{
		Dir: loaderDir,
	}
	d.cfg = &config.SubTaskConfig{
		From:             config.GetDBConfigForTest(),
		To:               config.GetDBConfigForTest(),
		ServerID:         101,
		MetaSchema:       "test",
		Name:             "validator_ut",
		ShadowTableRules: []string{config.DefaultShadowTableRules},
		TrashTableRules:  []string{config.DefaultTrashTableRules},
		Mode:             config.ModeIncrement,
		Flavor:           "mysql",
		LoaderConfig:     loaderCfg,
	}
	d.cfg.Experimental.AsyncCheckpointFlush = true
	d.cfg.From.Adjust()
	d.cfg.To.Adjust()

	d.cfg.UseRelay = false
	c.Assert(log.InitLogger(&log.Config{}), IsNil)
}

func (d *testDataValidatorSuite) genDBConn(c *C, db *sql.DB) *dbconn.DBConn {
	baseDB := conn.NewBaseDB(db, func() {})
	baseConn, err := baseDB.GetBaseConn(context.Background())
	c.Assert(err, IsNil)
	cfg, err := d.cfg.Clone()
	c.Assert(err, IsNil)
	return &dbconn.DBConn{
		BaseConn: baseConn,
		Cfg:      cfg,
	}
}

func (d *testDataValidatorSuite) TestRowDataIteratorImpl(c *C) {
	var (
		iter   RowDataIterator
		dbConn *dbconn.DBConn
	)
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	type testTableSchm struct {
		a string
		b string
	}
	rowVals := []testTableSchm{
		{
			a: "a",
			b: "100",
		},
		{
			a: "c",
			b: "200",
		},
		{
			a: "d",
			b: "300",
		},
		{
			a: "e",
			b: "400",
		},
	}
	expectRows := sqlmock.NewRows([]string{"a", "b"})
	for i := 0; i < len(rowVals); i++ {
		expectRows = expectRows.AddRow(rowVals[i].a, rowVals[i].b)
	}
	mock.ExpectQuery(
		"SELECT .* FROM .* WHERE a in .*",
	).WithArgs(
		"a", "c", "d", "e",
	).WillReturnRows(expectRows)
	c.Assert(err, IsNil)
	cond := formatCond(c, db, "test_row", "test", `
		create table if not exists test_row.test(
			a char(1),
			b int,
			primary key(a)
		);
	`, [][]string{{"a"}, {"c"}, {"d"}, {"e"}})
	c.Assert(err, IsNil)
	dbConn = d.genDBConn(c, db)
	iter, err = getRowsFrom(cond, dbConn)
	c.Assert(err, IsNil)
	for i := range rowVals {
		curVal, err := iter.Next()
		c.Assert(err, IsNil)
		keys := make([]string, 0)
		for key := range curVal {
			keys = append(keys, key)
		}
		sort.Slice(keys, func(i, j int) bool {
			return keys[i] < keys[j]
		})
		c.Assert(keys[0], Equals, "a")
		c.Assert(keys[1], Equals, "b")
		c.Assert(string(curVal["a"].Data), Equals, rowVals[i].a)
		c.Assert(string(curVal["b"].Data), Equals, rowVals[i].b)
	}
	nxtVal, err := iter.Next()
	c.Assert(nxtVal, IsNil)
	c.Assert(err, IsNil)
}

func (r *testDataValidatorSuite) TestRowChangeIteratorImpl(c *C) {
	db, _, err := sqlmock.New()
	c.Assert(err, IsNil)
	rows := []*rowChange{
		{
			pk:      []string{"pk", "1"},
			data:    []interface{}{"pk", "1", "some data"},
			theType: rowInsert,
		},
		{
			pk:      []string{"pk", "2"},
			data:    []interface{}{"pk", "2", "some data"},
			theType: rowDeleted,
		},
		{
			pk:      []string{"pkg", "2"},
			data:    []interface{}{"pkg", "2", "some data"},
			theType: rowDeleted,
		},
		{
			pk:      []string{"pke", "2"},
			data:    []interface{}{"pke", "2", "some data"},
			theType: rowUpdated,
		},
	}
	testCases := []map[string]string{
		{
			"pk1":   "pk",
			"pk2":   "1",
			"other": "some data",
		},
		{
			"pk1":   "pk",
			"pk2":   "2",
			"other": "some data",
		},
		{
			"pk1":   "pke",
			"pk2":   "2",
			"other": "some data",
		},
		{
			"pk1":   "pkg",
			"pk2":   "2",
			"other": "some data",
		},
	}
	creatTbl := `create table if not exists test_it.test1 (
		pk1 varchar(4),
		pk2 int,
		other text,
		primary key(pk1, pk2)
		);`
	tableDiff := getTableDiff(c, db, "test_it", "test1", creatTbl)
	var iter RowDataIterator
	iter, err = getRowChangeIterator(tableDiff, rows)
	c.Assert(err, IsNil)
	for i := 0; i < len(rows); i++ {
		res, err := iter.Next()
		c.Assert(err, IsNil)
		c.Assert(res, NotNil)
		colNames := []string{}
		for key := range res {
			colNames = append(colNames, key)
			c.Assert(string(res[key].Data), Equals, testCases[i][key])
		}
		sort.Slice(colNames, func(left, right int) bool {
			return colNames[left] < colNames[right]
		})
		c.Assert(colNames[0], Equals, "other")
		c.Assert(colNames[1], Equals, "pk1")
		c.Assert(colNames[2], Equals, "pk2")
	}
}

func (d *testDataValidatorSuite) TestGetRowsFrom(c *C) {
	type testCase struct {
		schemaName string
		tblName    string
		creatSQL   string
		pkValues   [][]string
		allCols    []string
		rowData    [][]string
		querySQL   string
	}
	testCases := []testCase{
		{
			schemaName: "test1",
			tblName:    "tbl1",
			creatSQL: `create table if not exists test1.tbl1(
				a int,
				b int,
				c int,
				primary key(a, b)
			);`,
			pkValues: [][]string{
				{"1", "2"}, {"3", "4"}, {"5", "6"},
			},
			allCols: []string{"a", "b", "c"},
			rowData: [][]string{
				{"1", "2", "3"}, {"3", "4", "5"}, {"5", "6", "7"},
			},
			querySQL: "SELECT .* FROM .*test1.*",
		},
		{
			schemaName: "test2",
			tblName:    "tbl2",
			creatSQL: `create table if not exists test2.tbl2(
				a char(10),
				other text,
				primary key(a)
			);`,
			pkValues: [][]string{
				{"a"}, {"b"}, {"c"},
			},
			allCols: []string{"a", "other"},
			rowData: [][]string{
				{"a", "some data"}, {"b", "some data"}, {"c", "some data"},
			},
			querySQL: "SELECT .* FROM .*test2.*",
		},
	}
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	for i, testCase := range testCases {
		args := []driver.Value{}
		for _, arr := range testCases[i].pkValues {
			for _, val := range arr {
				args = append(args, val)
			}
		}
		dataRows := mock.NewRows(testCase.allCols)
		for j := range testCases[i].rowData {
			args := []driver.Value{}
			for _, val := range testCase.rowData[j] {
				args = append(args, val)
			}
			dataRows = dataRows.AddRow(args...)
		}
		mock.ExpectQuery(testCase.querySQL).WithArgs(args...).WillReturnRows(dataRows)
		cond := formatCond(
			c, db,
			testCase.schemaName,
			testCase.tblName,
			testCase.creatSQL,
			testCase.pkValues,
		)
		dbConn := d.genDBConn(c, db)
		var iter RowDataIterator
		iter, err = getRowsFrom(cond, dbConn)
		c.Assert(err, IsNil)
		var res map[string]*dbutil.ColumnData
		for j := 0; j < 3; j++ {
			res, err = iter.Next()
			c.Assert(err, IsNil)
			pkRes := getPKValues(res, cond)
			// for each primary key
			for k, val := range testCase.pkValues[j] {
				c.Assert(pkRes[k], Equals, val)
			}
			// for each col
			for k, val := range testCase.rowData[j] {
				colName := testCase.allCols[k]
				c.Assert(string(res[colName].Data), Equals, val)
			}
		}
		res, err = iter.Next()
		c.Assert(res, IsNil)
		c.Assert(err, IsNil)
	}
}

func (d *testDataValidatorSuite) TestRowToString(c *C) {
	testCases := []map[string]*dbutil.ColumnData{
		{
			"pk-1": &dbutil.ColumnData{
				Data:   []byte("some data"),
				IsNull: false,
			},
			"pk-2": &dbutil.ColumnData{
				Data:   []byte("simple data"),
				IsNull: false,
			},
		},
		{
			"pk-3": &dbutil.ColumnData{
				Data:   nil,
				IsNull: true,
			},
			"pk-4": &dbutil.ColumnData{
				Data:   []byte("data"),
				IsNull: false,
			},
		},
	}
	expectedStr := []string{
		"({ pk-1: some data, pk-2: simple data,  }|{ pk-2: simple data, pk-1: some data,  })",
		"({ pk-3: IsNull, pk-4: data,  }|{ pk-4: data, pk-3: IsNull,  })",
	}
	for i, testCase := range testCases {
		ret := rowToString(testCase)
		c.Assert(ret, Matches, expectedStr[i])
	}
}

func (d *testDataValidatorSuite) TestNeedQuotes(c *C) {
	testCases := map[byte]bool{
		parsermysql.TypeTiny:       false,
		parsermysql.TypeShort:      false,
		parsermysql.TypeLong:       false,
		parsermysql.TypeLonglong:   false,
		parsermysql.TypeInt24:      false,
		parsermysql.TypeYear:       false,
		parsermysql.TypeFloat:      false,
		parsermysql.TypeDouble:     false,
		parsermysql.TypeNewDecimal: false,
		parsermysql.TypeDuration:   true,
		parsermysql.TypeDatetime:   true,
		parsermysql.TypeNewDate:    true,
		parsermysql.TypeVarchar:    true,
		parsermysql.TypeBlob:       true,
		parsermysql.TypeVarString:  true,
		parsermysql.TypeString:     true,
	}
	for typ, val := range testCases {
		c.Assert(NeedQuotes(typ), Equals, val)
	}
}

func (d *testDataValidatorSuite) TestGetPKValues(c *C) {
	testCases := []map[string]*dbutil.ColumnData{
		{
			"col1": &dbutil.ColumnData{
				Data: []byte("some data"),
			},
			"col2": &dbutil.ColumnData{
				Data: []byte("data"),
			},
			"col3": &dbutil.ColumnData{
				IsNull: true,
			},
		},
		{
			"c1": &dbutil.ColumnData{
				Data: []byte("lk"),
			},
			"c2": &dbutil.ColumnData{
				Data: []byte("1001"),
			},
		},
	}
	expectedOut := [][]string{
		{"some data"},
		{"lk", "1001"},
	}
	type testTable struct {
		schemaName string
		tableName  string
		creatSQL   string
	}
	testTables := []testTable{
		{
			schemaName: "test1",
			tableName:  "tbl1",
			creatSQL: `create table if not exists test1.tbl1(
				col1 varchar(10),
				col2 text,
				col3 varchar(20),
				primary key(col1)
			);`,
		},
		{
			schemaName: "test2",
			tableName:  "tbl2",
			creatSQL: `create table if not exists test2.tbl2(
				c1 char(2),
				c2 int,
				primary key(c1, c2)
			);`,
		},
	}
	db, _, err := sqlmock.New()
	c.Assert(err, IsNil)
	for i := range testCases {
		cond := formatCond(
			c,
			db,
			testTables[i].schemaName,
			testTables[i].tableName,
			testTables[i].creatSQL,
			[][]string{},
		)
		ret := getPKValues(testCases[i], cond)
		for j, val := range expectedOut[i] {
			c.Assert(ret[j], Equals, val)
		}
	}
}

func (d *testDataValidatorSuite) generateEvents(binlogEvents mockBinlogEvents, c *C) []*replication.BinlogEvent {
	events := make([]*replication.BinlogEvent, 0, 1024)
	for _, e := range binlogEvents {
		switch e.typ {
		case DBCreate:
			evs, _, err := d.eventsGenerator.GenCreateDatabaseEvents(e.args[0].(string))
			c.Assert(err, IsNil)
			events = append(events, evs...)
		case DBDrop:
			evs, _, err := d.eventsGenerator.GenDropDatabaseEvents(e.args[0].(string))
			c.Assert(err, IsNil)
			events = append(events, evs...)
		case TableCreate:
			evs, _, err := d.eventsGenerator.GenCreateTableEvents(e.args[0].(string), e.args[1].(string))
			c.Assert(err, IsNil)
			events = append(events, evs...)
		case TableDrop:
			evs, _, err := d.eventsGenerator.GenDropTableEvents(e.args[0].(string), e.args[1].(string))
			c.Assert(err, IsNil)
			events = append(events, evs...)

		case DDL:
			evs, _, err := d.eventsGenerator.GenDDLEvents(e.args[0].(string), e.args[1].(string), 0)
			c.Assert(err, IsNil)
			events = append(events, evs...)

		case Write, Update, Delete:
			dmlData := []*event.DMLData{
				{
					TableID:    e.args[0].(uint64),
					Schema:     e.args[1].(string),
					Table:      e.args[2].(string),
					ColumnType: e.args[3].([]byte),
					Rows:       e.args[4].([][]interface{}),
				},
			}
			var eventType replication.EventType
			switch e.typ {
			case Write:
				eventType = replication.WRITE_ROWS_EVENTv2
			case Update:
				eventType = replication.UPDATE_ROWS_EVENTv2
			case Delete:
				eventType = replication.DELETE_ROWS_EVENTv2
			default:
				c.Fatal(fmt.Sprintf("mock event generator don't support event type: %d", e.typ))
			}
			evs, _, err := d.eventsGenerator.GenDMLEvents(eventType, dmlData, 0)
			c.Assert(err, IsNil)
			events = append(events, evs...)
		}
	}
	return events
}

func (d *testDataValidatorSuite) TestDoValidate(c *C) {
	type testCase struct {
		schemaName string
		tblName    string
		creatSQL   string
		binlogEvs  []mockBinlogEvent
		selectSQLs []string // mock in toDB
		retRows    [][][]string
		colNames   []string
		failRowCnt int
		failRowPKs []string
	}
	batchSize := 2
	testCases := []testCase{
		{
			schemaName: "test1",
			tblName:    "tbl1",
			creatSQL: `create table if not exists test1.tbl1(
				a int,
				b int,
				c text,
				primary key(a, b));`,
			binlogEvs: []mockBinlogEvent{
				{typ: Write, args: []interface{}{uint64(8), "test1", "tbl1", []byte{mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_STRING}, [][]interface{}{{int32(1), int32(2), "some data1"}}}},
				{typ: Write, args: []interface{}{uint64(8), "test1", "tbl1", []byte{mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_STRING}, [][]interface{}{{int32(3), int32(4), "some data2"}}}},

				{typ: Update, args: []interface{}{uint64(8), "test1", "tbl1", []byte{mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_STRING}, [][]interface{}{{int32(1), int32(2), "some data1"}, {int32(1), int32(3), "some data3"}}}},
				{typ: Delete, args: []interface{}{uint64(8), "test1", "tbl1", []byte{mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_STRING}, [][]interface{}{{int32(3), int32(4), "some data2"}}}},

				{typ: Write, args: []interface{}{uint64(8), "test1", "tbl1", []byte{mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_STRING}, [][]interface{}{{int32(5), int32(6), "some data4"}}}},
				{typ: Update, args: []interface{}{uint64(8), "test1", "tbl1", []byte{mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_STRING}, [][]interface{}{{int32(5), int32(6), "some data4"}, {int32(5), int32(7), "some data4"}}}},
			},
			selectSQLs: []string{
				"SELECT .* FROM .*test1.* WHERE .*a,b.* in .*", // batch1: insert row1, row2
				"SELECT .* FROM .*test1.* WHERE .*a,b.* in .*", // batch2: update row1
				"SELECT .* FROM .*test1.* WHERE .*a,b.* in .*", // batch2: delete row2
				"SELECT .* FROM .*test1.* WHERE .*a,b.* in .*", // batch3: insert and update row3, only query (5, 7)
				"SELECT .* FROM .*test1.* WHERE .*a,b.* in .*", // batch3: update query (5, 6)
			},
			retRows: [][][]string{
				{
					{"1", "2", "some data1"}, // insert
					{"3", "4", "some data2"},
				},
				{
					{"1", "3", "some data3"}, // update
				},
				{
					{}, // delete
				},
				{
					{"5", "7", "some data4"},
				},
				{
					{},
				},
			},
			colNames:   []string{"a", "b", "c"},
			failRowCnt: 0,
			failRowPKs: []string{},
		},
		{
			// stale read in downstream and got erronous result
			// but row2's primary key is reused and inserted again
			// the error is restored
			schemaName: "test2",
			tblName:    "tbl2",
			creatSQL: `create table if not exists test2.tbl2(
				a varchar(10),
				b int,
				c float,
				primary key(a));`,
			binlogEvs: []mockBinlogEvent{
				{typ: Write, args: []interface{}{uint64(8), "test2", "tbl2", []byte{mysql.MYSQL_TYPE_STRING, mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_FLOAT}, [][]interface{}{{"val1", int32(1), float32(1.2)}}}},
				{typ: Write, args: []interface{}{uint64(8), "test2", "tbl2", []byte{mysql.MYSQL_TYPE_STRING, mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_FLOAT}, [][]interface{}{{"val2", int32(2), float32(2.2)}}}},

				{typ: Delete, args: []interface{}{uint64(8), "test2", "tbl2", []byte{mysql.MYSQL_TYPE_STRING, mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_FLOAT}, [][]interface{}{{"val1", int32(1), float32(1.2)}}}},
				{typ: Delete, args: []interface{}{uint64(8), "test2", "tbl2", []byte{mysql.MYSQL_TYPE_STRING, mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_FLOAT}, [][]interface{}{{"val2", int32(2), float32(2.2)}}}},

				{typ: Write, args: []interface{}{uint64(8), "test2", "tbl2", []byte{mysql.MYSQL_TYPE_STRING, mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_FLOAT}, [][]interface{}{{"val3", int32(3), float32(3.2)}}}},
				{typ: Write, args: []interface{}{uint64(8), "test2", "tbl2", []byte{mysql.MYSQL_TYPE_STRING, mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_FLOAT}, [][]interface{}{{"val2", int32(2), float32(4.2)}}}},
			},
			selectSQLs: []string{
				"SELECT .* FROM .*test2.* WHERE .*a.* in.*", // batch1: query row1 and row2
				"SELECT .* FROM .*test2.* WHERE .*a.* in.*", // batch2: query row1 and row2
				"SELECT .* FROM .*test2.* WHERE .*a.* in.*", // batch3: query row2 and row3
			},
			retRows: [][][]string{
				{
					{"val1", "1", "1.2"},
					{"val2", "2", "2.2"},
				},
				{
					{"val1", "1", "1.2"},
					{"val2", "2", "2.2"},
				},
				{
					{"val2", "2", "4.2"},
					{"val3", "3", "3.2"},
				},
			},
			colNames:   []string{"a", "b", "c"},
			failRowCnt: 1,
			failRowPKs: []string{"val1"},
		},
	}
	for _, testCase := range testCases {
		var (
			fromDB, toDB     *sql.DB
			fromMock, toMock sqlmock.Sqlmock
			err              error
		)
		fromDB, fromMock, err = sqlmock.New()
		c.Assert(err, IsNil)
		toDB, toMock, err = sqlmock.New()
		c.Assert(err, IsNil)
		syncerObj := NewSyncer(d.cfg, nil, nil)
		c.Assert(syncerObj, NotNil)
		validator := NewContinuousDataValidator(d.cfg, syncerObj)
		validator.Start(pb.Stage_Paused)      // init but will return soon
		validator.result = pb.ProcessResult{} // clear error
		validator.workerCnt = 1
		validator.retryInterval = 100 * time.Second // never retry
		events1 := testCase.binlogEvs
		mockStreamerProducer := &MockStreamProducer{d.generateEvents(events1, c)}
		mockStreamer, err := mockStreamerProducer.generateStreamer(binlog.NewLocation(""))
		c.Assert(err, IsNil)
		validator.streamerController = &StreamerController{
			streamerProducer: mockStreamerProducer,
			streamer:         mockStreamer,
			closed:           false,
		}
		// validate every 2 rows updated
		validator.batchRowCnt = batchSize
		validator.fromDB = conn.NewBaseDB(fromDB, func() {})
		validator.fromDBConn = d.genDBConn(c, fromDB)
		validator.toDB = conn.NewBaseDB(toDB, func() {})
		validator.toDBConn = d.genDBConn(c, toDB)
		fromMock.ExpectQuery("SHOW CREATE TABLE " + fmt.Sprintf("`%s`.`%s`", testCase.schemaName, testCase.tblName)).WillReturnRows(
			sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(
				"tbl1", testCase.creatSQL,
			),
		)
		fromMock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(
			fromMock.NewRows([]string{"Variable_name", "Value"}).AddRow(
				"sql_mode", "",
			),
		)
		for i := range testCase.selectSQLs {
			rawRetRows := testCase.retRows[i]
			rows := sqlmock.NewRows(testCase.colNames)
			for j := range rawRetRows {
				if len(rawRetRows[j]) == 0 {
					break // for delete query
				}
				rowVals := []driver.Value{}
				for k := range rawRetRows[j] {
					rowVals = append(rowVals, rawRetRows[j][k])
				}
				rows.AddRow(rowVals...)
			}
			toMock.ExpectQuery(testCase.selectSQLs[i]).WillReturnRows(rows)
		}
		validator.doValidate()
		// wait for all routine finished
		time.Sleep(1 * time.Second)
		validator.cancel()
		validator.wg.Wait()
		// failed row
		c.Assert(int(validator.failedRowCnt.Load()), Equals, testCase.failRowCnt)
		// valid table
		fullTableName := testCase.schemaName + "." + testCase.tblName
		table, ok := validator.diffTables[fullTableName]
		c.Assert(ok, IsTrue)
		c.Assert(len(table.Info.Columns), Equals, len(testCase.colNames))
		if testCase.failRowCnt > 0 {
			// validate failed rows
			_, ok := validator.failedChangesMap[0][fullTableName]
			c.Assert(ok, IsTrue)
			allRowsPKs := []string{}
			for key := range validator.failedChangesMap[0][fullTableName].rows {
				allRowsPKs = append(allRowsPKs, key)
			}
			sort.Slice(allRowsPKs, func(l, r int) bool {
				return allRowsPKs[l] < allRowsPKs[r]
			})
			c.Assert(allRowsPKs, DeepEquals, testCase.failRowPKs)
		}
		c.Assert(len(validator.result.Errors), Equals, 0)
	}
}
