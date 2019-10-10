package cdc

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	. "github.com/pingcap/check"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-cdc/cdc/kv"
	"github.com/pingcap/tidb-cdc/cdc/mock"
	"github.com/pingcap/tidb-cdc/cdc/util"
	"go.uber.org/zap"
)

type CDCSuite struct {
	database string
	puller   *mock.MockTiDB
	mock     sqlmock.Sqlmock
	mounter  *TxnMounter
	sink     Sink
}

var _ = Suite(NewCDCSuite())

func NewCDCSuite() *CDCSuite {
	// create a mock puller
	puller, err := mock.NewMockPuller()
	if err != nil {
		panic(err.Error())
	}
	cdcSuite := &CDCSuite{
		database: "test",
		puller:   puller,
	}
	jobs, err := puller.GetAllHistoryDDLJobs()
	if err != nil {
		panic(err.Error())
	}
	// create a schema
	schema, err := NewSchema(jobs, false)
	if err != nil {
		panic(err.Error())
	}

	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		panic(err.Error())
	}
	cdcSuite.mock = mock

	inspector := &cachedInspector{
		db:    db,
		cache: make(map[string]*tableInfo),
		tableGetter: func(_ *sql.DB, schemaName string, tableName string) (*tableInfo, error) {
			info, err := getTableInfoFromSchema(schema, schemaName, tableName)
			log.Info("tableInfo", zap.Reflect("columns", info.columns), zap.Reflect("uniqueKeys", info.uniqueKeys), zap.Reflect("primaryKey", info.primaryKey))
			return info, err
		},
	}
	sink := &mysqlSink{
		db:           db,
		infoGetter:   schema,
		tblInspector: inspector,
	}
	cdcSuite.sink = sink

	mounter, err := NewTxnMounter(schema, time.Local)
	if err != nil {
		panic(err.Error())
	}
	cdcSuite.mounter = mounter
	return cdcSuite
}

func (s *CDCSuite) Forward(span util.Span, ts uint64) bool {
	return true
}

func (s *CDCSuite) RunAndCheckSync(c *C, execute func(func(string, ...interface{})), expect func(sqlmock.Sqlmock)) {
	expect(s.mock)
	var rawKVs []*kv.RawKVEntry
	executeSql := func(sql string, args ...interface{}) {
		log.Info("b rawKVs", zap.Reflect("rawKVs", rawKVs), zap.String("sql", sql))
		kvs := s.puller.MustExec(c, sql, args...)
		log.Info("kvs", zap.Reflect("kvs", kvs), zap.Int("len", len(kvs)))
		rawKVs = append(rawKVs, kvs...)
		log.Info("c rawKVs", zap.Reflect("rawKVs", rawKVs))
	}
	execute(executeSql)
	log.Info("all kvs", zap.Int("len", len(rawKVs)))
	txn, err := s.mounter.Mount(RawTxn{ts: rawKVs[len(rawKVs)-1].Ts, entries: rawKVs})
	c.Assert(err, IsNil)
	log.Info("txn", zap.Reflect("txn", txn))
	err = s.sink.Emit(context.Background(), *txn)
	c.Assert(err, IsNil)
	err = s.mock.ExpectationsWereMet()
	c.Assert(err, IsNil)
}

func (s *CDCSuite) TestSimple(c *C) {
	s.RunAndCheckSync(c, func(execute func(string, ...interface{})) {
		execute("create table test.simple_test (id bigint primary key)")
	}, func(mock sqlmock.Sqlmock) {
		mock.ExpectBegin()
		mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectExec("create table test.simple_test (id bigint primary key)").WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectCommit()
	})
}

func (s *CDCSuite) TestPKorUKCases(c *C) {
	cases := []struct {
		Tp     string
		Value  interface{}
		Update interface{}
	}{
		{
			Tp:     "BIGINT UNSIGNED",
			Value:  uint64(math.MaxInt64),
			Update: uint64(math.MaxInt64) - 1,
		},
		{
			Tp:     "BIGINT SIGNED",
			Value:  int64(math.MaxInt64),
			Update: int64(math.MaxInt64) - 1,
		},
		{
			Tp:     "INT UNSIGNED",
			Value:  uint32(math.MaxUint32),
			Update: uint32(math.MaxUint32) - 1,
		},
		{
			Tp:     "INT SIGNED",
			Value:  int32(math.MaxInt32),
			Update: int32(math.MaxInt32) - 1,
		},
		{
			Tp:     "SMALLINT UNSIGNED",
			Value:  uint16(math.MaxUint16),
			Update: uint16(math.MaxUint16) - 1,
		},
		{
			Tp:     "SMALLINT SIGNED",
			Value:  int16(math.MaxInt16),
			Update: int16(math.MaxInt16) - 1,
		},
		{
			Tp:     "TINYINT UNSIGNED",
			Value:  uint8(math.MaxUint8),
			Update: uint8(math.MaxUint8) - 1,
		},
		{
			Tp:     "TINYINT SIGNED",
			Value:  int8(math.MaxInt8),
			Update: int8(math.MaxInt8) - 1,
		},
	}

	for _, cs := range cases {
		for _, pkOrUK := range []string{"UNIQUE KEY NOT NULL", "PRIMARY KEY"} {

			sql := fmt.Sprintf("CREATE TABLE test.pk_or_uk(id %s %s)", cs.Tp, pkOrUK)
			s.RunAndCheckSync(c, func(execute func(string, ...interface{})) {
				execute(sql)
			}, func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectExec(sql).WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectCommit()
			})

			sql = fmt.Sprintf("INSERT INTO test.pk_or_uk(id) values(%d)", cs.Value)
			s.RunAndCheckSync(c, func(execute func(string, ...interface{})) {
				execute(sql)
			}, func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				// pk is not handle
				if pkOrUK != "PRIMARY KEY" {
					mock.ExpectExec("DELETE FROM `test`.`pk_or_uk` WHERE `id` = ? LIMIT 1;").WithArgs(cs.Value).WillReturnResult(sqlmock.NewResult(1, 1))
				}
				mock.ExpectExec("REPLACE INTO `test`.`pk_or_uk`(`id`) VALUES (?);").WithArgs(cs.Value).WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectCommit()
			})

			sql = fmt.Sprintf("UPDATE test.pk_or_uk set id = %d where id = %d", cs.Update, cs.Value)
			s.RunAndCheckSync(c, func(execute func(string, ...interface{})) {
				execute(sql)
			}, func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				// pk is not handle
				if pkOrUK != "PRIMARY KEY" {
					mock.ExpectExec("DELETE FROM `test`.`pk_or_uk` WHERE `id` = ? LIMIT 1;").WithArgs(cs.Update).WillReturnResult(sqlmock.NewResult(1, 1))
				}
				mock.ExpectExec("DELETE FROM `test`.`pk_or_uk` WHERE `id` = ? LIMIT 1;").WithArgs(cs.Value).WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectExec("REPLACE INTO `test`.`pk_or_uk`(`id`) VALUES (?);").WithArgs(cs.Update).WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectCommit()
			})

			sql = fmt.Sprintf("DELETE from test.pk_or_uk where id = %d", cs.Update)
			s.RunAndCheckSync(c, func(execute func(string, ...interface{})) {
				execute(sql)
			}, func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec("DELETE FROM `test`.`pk_or_uk` WHERE `id` = ? LIMIT 1;").WithArgs(cs.Update).WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectCommit()
			})

			sql = "DROP TABLE test.pk_or_uk"
			s.RunAndCheckSync(c, func(execute func(string, ...interface{})) {
				execute(sql)
			}, func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectExec(sql).WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectCommit()
			})
		}
	}
}

func (s *CDCSuite) TestMultiDataType(c *C) {
	ddlExpectFunc := func(sql string, mock sqlmock.Sqlmock) {
		mock.ExpectBegin()
		mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectExec(sql).WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectCommit()
	}

	expectedReplaceSQL := "REPLACE INTO `test`.`cdc_multi_data_type`" +
		"(`id`,`t_boolean`,`t_bigint`,`t_double`,`t_decimal`,`t_bit`," +
		"`t_date`,`t_datetime`,`t_timestamp`,`t_time`,`t_year`," +
		"`t_char`,`t_varchar`,`t_blob`,`t_text`,`t_enum`," +
		"`t_set`,`t_json`) VALUES " +
		"(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"

	// test different data type of mysql
	// mysql will change boolean to tinybit(1)
	cases := []testCases{{`CREATE TABLE test.cdc_multi_data_type (
			id INT AUTO_INCREMENT,
			t_boolean BOOLEAN,
			t_bigint BIGINT,
			t_double DOUBLE,
			t_decimal DECIMAL(38,19),
			t_bit BIT(64),
			t_date DATE,
			t_datetime DATETIME,
			t_timestamp TIMESTAMP NULL,
			t_time TIME,
			t_year YEAR,
			t_char CHAR,
			t_varchar VARCHAR(10),
			t_blob BLOB,
			t_text TEXT,
			t_enum ENUM('enum1', 'enum2', 'enum3'),
			t_set SET('a', 'b', 'c'),
			t_json JSON,
			PRIMARY KEY(id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;`, ddlExpectFunc},

		{`INSERT INTO test.cdc_multi_data_type(t_boolean, t_bigint, t_double, t_decimal, t_bit
		,t_date, t_datetime, t_timestamp, t_time, t_year
		,t_char, t_varchar, t_blob, t_text, t_enum
		,t_set, t_json) VALUES
		(true, 9223372036854775807, 123.123, 123456789012.123456789012, b'1000001'
		,'1000-01-01', '9999-12-31 23:59:59', '19731230153000', '23:59:59', 1970
		,'测', '测试', 'blob', '测试text', 'enum2'
		,'a,b', NULL);`,
			func(sql string, mock sqlmock.Sqlmock) {
				b1000001, err := strconv.ParseInt("1000001", 2, 64)
				c.Assert(err, IsNil)
				mock.ExpectBegin()
				mock.ExpectExec(expectedReplaceSQL).
					WithArgs(1, 1, 9223372036854775807, 123.123, "123456789012.1234567890120000000", b1000001, "1000-01-01", "9999-12-31 23:59:59", "1973-12-30 15:30:00", "23:59:59", 1970, []byte("测"), []byte("测试"), []byte("blob"), []byte("测试text"), 2, 3, nil).WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectCommit()
			}},

		{`INSERT INTO test.cdc_multi_data_type(t_boolean) VALUES(TRUE);`, func(sql string, mock sqlmock.Sqlmock) {
			mock.ExpectBegin()
			mock.ExpectExec(expectedReplaceSQL).
				WithArgs(2, 1, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil).WillReturnResult(sqlmock.NewResult(1, 1))
			mock.ExpectCommit()
		}},

		{`INSERT INTO test.cdc_multi_data_type(t_boolean) VALUES(FALSE);`, func(sql string, mock sqlmock.Sqlmock) {
			mock.ExpectBegin()
			mock.ExpectExec(expectedReplaceSQL).
				WithArgs(3, 0, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil).WillReturnResult(sqlmock.NewResult(1, 1))
			mock.ExpectCommit()
		}},

		{`INSERT INTO test.cdc_multi_data_type(t_bigint) VALUES(-9223372036854775808);`, func(sql string, mock sqlmock.Sqlmock) {
			mock.ExpectBegin()
			mock.ExpectExec(expectedReplaceSQL).
				WithArgs(4, nil, -9223372036854775808, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil).WillReturnResult(sqlmock.NewResult(1, 1))
			mock.ExpectCommit()
		}},

		{`INSERT INTO test.cdc_multi_data_type(t_bigint) VALUES(9223372036854775807);`, func(sql string, mock sqlmock.Sqlmock) {
			mock.ExpectBegin()
			mock.ExpectExec(expectedReplaceSQL).
				WithArgs(5, nil, 9223372036854775807, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil).WillReturnResult(sqlmock.NewResult(1, 1))
			mock.ExpectCommit()
		}},

		{`INSERT INTO test.cdc_multi_data_type(t_json) VALUES('{"key1": "value1", "key2": "value2"}');`, func(sql string, mock sqlmock.Sqlmock) {
			mock.ExpectBegin()
			mock.ExpectExec(expectedReplaceSQL).
				WithArgs(6, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, `{"key1": "value1", "key2": "value2"}`).WillReturnResult(sqlmock.NewResult(1, 1))
			mock.ExpectCommit()
		}},

		{"DROP TABLE test.cdc_multi_data_type", ddlExpectFunc},
	}
	s.RunTestCases(c, cases)
}

type testCases struct {
	execSQL string
	expect  func(string, sqlmock.Sqlmock)
}

func (s *CDCSuite) RunTestCases(c *C, cases []testCases) {
	for _, cs := range cases {
		s.RunAndCheckSync(c, func(execute func(string, ...interface{})) {
			execute(cs.execSQL)
		}, func(mock sqlmock.Sqlmock) {
			cs.expect(cs.execSQL, mock)
		})
	}
}
