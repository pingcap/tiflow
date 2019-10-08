package cdc

import (
	"context"
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

	sink := &mysqlSink{
		db:           db,
		infoGetter:   schema,
		tblInspector: newCachedInspector(db),
	}
	cdcSuite.sink = sink

	mounter, err := NewTxnMounter(schema, time.UTC)
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
