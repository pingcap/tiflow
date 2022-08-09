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

package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	dmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink"
	"github.com/pingcap/tiflow/cdc/sinkv2/metrics"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func mockTestDB(adjustSQLMode bool) (*sql.DB, error) {
	// mock for test db, which is used querying TiDB session variable
	db, mock, err := sqlmock.New()
	if err != nil {
		return nil, err
	}
	if adjustSQLMode {
		mock.ExpectQuery("SELECT @@SESSION.sql_mode;").
			WillReturnRows(sqlmock.NewRows([]string{"@@SESSION.sql_mode"}).
				AddRow("ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE"))
	}

	columns := []string{"Variable_name", "Value"}
	mock.ExpectQuery("show session variables like 'allow_auto_random_explicit_insert';").WillReturnRows(
		sqlmock.NewRows(columns).AddRow("allow_auto_random_explicit_insert", "0"),
	)
	mock.ExpectQuery("show session variables like 'tidb_txn_mode';").WillReturnRows(
		sqlmock.NewRows(columns).AddRow("tidb_txn_mode", "pessimistic"),
	)
	mock.ExpectQuery("show session variables like 'transaction_isolation';").WillReturnRows(
		sqlmock.NewRows(columns).AddRow("transaction_isolation", "REPEATED-READ"),
	)
	mock.ExpectQuery("show session variables like 'tidb_placement_mode';").
		WillReturnRows(
			sqlmock.NewRows(columns).
				AddRow("tidb_placement_mode", "IGNORE"),
		)
	mock.ExpectQuery("select character_set_name from information_schema.character_sets " +
		"where character_set_name = 'gbk';").WillReturnRows(
		sqlmock.NewRows([]string{"character_set_name"}).AddRow("gbk"),
	)

	mock.ExpectClose()
	return db, nil
}

func newMySQLSink4Test(ctx context.Context, t *testing.T) *mysqlSink {
	params := defaultParams.Clone()
	params.batchReplaceEnabled = false
	ctx, cancel := context.WithCancel(ctx)
	return &mysqlSink{
		statistics: metrics.NewStatistics(ctx, metrics.SinkTypeDB),
		params:     params,
		cancel:     cancel,
	}
}

func TestMustFlushOnNilEvent(t *testing.T) {
	mc := newMySQLSink4Test(context.Background(), t)
	needFlush := mc.OnTxnEvent(&eventsink.TxnCallbackableEvent{Event: nil})
	require.True(t, needFlush)
	require.Nil(t, mc.Close())
}

func TestPrepareDML(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		input    []*model.RowChangedEvent
		expected *preparedDMLs
	}{
		{
			input: []*model.RowChangedEvent{},
			expected: &preparedDMLs{
				startTs: []model.Ts{},
				sqls:    []string{},
				values:  [][]interface{}{},
			},
		}, {
			input: []*model.RowChangedEvent{
				{
					StartTs:  418658114257813514,
					CommitTs: 418658114257813515,
					Table:    &model.TableName{Schema: "common_1", Table: "uk_without_pk"},
					PreColumns: []*model.Column{nil, {
						Name:  "a1",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag,
						Value: 1,
					}, {
						Name:  "a3",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag,
						Value: 1,
					}},
					IndexColumns: [][]int{{1, 2}},
				},
			},
			expected: &preparedDMLs{
				startTs:  []model.Ts{418658114257813514},
				sqls:     []string{"DELETE FROM `common_1`.`uk_without_pk` WHERE `a1` = ? AND `a3` = ? LIMIT 1;"},
				values:   [][]interface{}{{1, 1}},
				rowCount: 1,
			},
		}, {
			input: []*model.RowChangedEvent{
				{
					StartTs:  418658114257813516,
					CommitTs: 418658114257813517,
					Table:    &model.TableName{Schema: "common_1", Table: "uk_without_pk"},
					Columns: []*model.Column{nil, {
						Name:  "a1",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag,
						Value: 2,
					}, {
						Name:  "a3",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag,
						Value: 2,
					}},
					IndexColumns: [][]int{{1, 2}},
				},
			},
			expected: &preparedDMLs{
				startTs:  []model.Ts{418658114257813516},
				sqls:     []string{"REPLACE INTO `common_1`.`uk_without_pk`(`a1`,`a3`) VALUES (?,?);"},
				values:   [][]interface{}{{2, 2}},
				rowCount: 1,
			},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ms := newMySQLSink4Test(ctx, t)
	for _, tc := range testCases {
		ms.events = make([]*eventsink.TxnCallbackableEvent, 1)
		ms.events[0] = &eventsink.TxnCallbackableEvent{
			Event: &model.SingleTableTxn{Rows: tc.input},
		}
		ms.rows = len(tc.input)
		dmls := ms.prepareDMLs()
		require.Equal(t, tc.expected, dmls)
	}
}

func TestAdjustSQLMode(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dbIndex := 0
	mockGetDBConn := func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		defer func() { dbIndex++ }()

		if dbIndex == 0 {
			// test db
			db, err := mockTestDB(true)
			require.Nil(t, err)
			return db, nil
		}

		// normal db
		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		require.Nil(t, err)
		mock.ExpectClose()
		return db, nil
	}
	backupGetDBConn := GetDBConnImpl
	GetDBConnImpl = mockGetDBConn
	defer func() { GetDBConnImpl = backupGetDBConn }()

	changefeed := "test-changefeed"
	sinkURI, err := url.Parse("mysql://127.0.0.1:4000/?time-zone=UTC&worker-count=4")
	require.Nil(t, err)
	sink, err := NewMySQLSink(ctx, model.DefaultChangeFeedID(changefeed), sinkURI)
	require.Nil(t, err)
	require.Nil(t, sink.Close())
}

type mockUnavailableMySQL struct {
	listener net.Listener
	quit     chan interface{}
	wg       sync.WaitGroup
}

func newMockUnavailableMySQL(addr string, t *testing.T) *mockUnavailableMySQL {
	s := &mockUnavailableMySQL{
		quit: make(chan interface{}),
	}
	l, err := net.Listen("tcp", addr)
	require.Nil(t, err)
	s.listener = l
	s.wg.Add(1)
	go s.serve(t)
	return s
}

func (s *mockUnavailableMySQL) serve(t *testing.T) {
	defer s.wg.Done()

	for {
		_, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.quit:
				return
			default:
				require.Error(t, err)
			}
		} else {
			s.wg.Add(1)
			go func() {
				// don't read from TCP connection, to simulate database service unavailable
				<-s.quit
				s.wg.Done()
			}()
		}
	}
}

func (s *mockUnavailableMySQL) Stop() {
	close(s.quit)
	s.listener.Close()
	s.wg.Wait()
}

func TestNewMySQLTimeout(t *testing.T) {
	addr := "127.0.0.1:33333"
	mockMySQL := newMockUnavailableMySQL(addr, t)
	defer mockMySQL.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changefeed := "test-changefeed"
	sinkURI, err := url.Parse(fmt.Sprintf("mysql://%s/?read-timeout=1s&timeout=1s", addr))
	require.Nil(t, err)
	require.Nil(t, err)
	_, err = NewMySQLSink(ctx, model.DefaultChangeFeedID(changefeed), sinkURI)
	require.Equal(t, driver.ErrBadConn, errors.Cause(err))
}

// Test OnTxnEvent and Flush interfaces. Event callbacks should be called correctly after flush.
func TestNewMySQLSinkExecDML(t *testing.T) {
	dbIndex := 0
	mockGetDBConn := func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		defer func() { dbIndex++ }()

		if dbIndex == 0 {
			// test db
			db, err := mockTestDB(true)
			require.Nil(t, err)
			return db, nil
		}

		// normal db
		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		require.Nil(t, err)
		mock.ExpectBegin()
		mock.ExpectExec("REPLACE INTO `s1`.`t1`(`a`,`b`) VALUES (?,?),(?,?)").
			WithArgs(1, "test", 2, "test").
			WillReturnResult(sqlmock.NewResult(2, 2))
		mock.ExpectExec("REPLACE INTO `s1`.`t2`(`a`,`b`) VALUES (?,?),(?,?)").
			WithArgs(1, "test", 2, "test").
			WillReturnResult(sqlmock.NewResult(2, 2))
		mock.ExpectCommit()
		mock.ExpectClose()
		return db, nil
	}

	backupGetDBConn := GetDBConnImpl
	GetDBConnImpl = mockGetDBConn
	defer func() { GetDBConnImpl = backupGetDBConn }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changefeed := "test-changefeed"
	sinkURI, err := url.Parse("mysql://127.0.0.1:4000/?time-zone=UTC&worker-count=4")
	require.Nil(t, err)
	sink, err := NewMySQLSink(ctx, model.DefaultChangeFeedID(changefeed), sinkURI)
	require.Nil(t, err)

	rows := []*model.RowChangedEvent{
		{
			StartTs:  1,
			CommitTs: 2,
			Table:    &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
			Columns: []*model.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
					Value: 1,
				},
				{
					Name:  "b",
					Type:  mysql.TypeVarchar,
					Flag:  0,
					Value: "test",
				},
			},
		},
		{
			StartTs:  5,
			CommitTs: 6,
			Table:    &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
			Columns: []*model.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
					Value: 2,
				},
				{
					Name:  "b",
					Type:  mysql.TypeVarchar,
					Flag:  0,
					Value: "test",
				},
			},
		},
		{
			StartTs:  3,
			CommitTs: 4,
			Table:    &model.TableName{Schema: "s1", Table: "t2", TableID: 2},
			Columns: []*model.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
					Value: 1,
				},
				{
					Name:  "b",
					Type:  mysql.TypeVarchar,
					Flag:  0,
					Value: "test",
				},
			},
		},
		{
			StartTs:  7,
			CommitTs: 8,
			Table:    &model.TableName{Schema: "s1", Table: "t2", TableID: 2},
			Columns: []*model.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
					Value: 2,
				},
				{
					Name:  "b",
					Type:  mysql.TypeVarchar,
					Flag:  0,
					Value: "test",
				},
			},
		},
	}

	var flushedTs uint64 = 0
	_ = sink.OnTxnEvent(&eventsink.TxnCallbackableEvent{
		Event: &model.SingleTableTxn{Rows: rows},
		Callback: func() {
			for _, row := range rows {
				if flushedTs < row.CommitTs {
					flushedTs = row.CommitTs
				}
			}
		},
	})

	err = sink.Flush(context.Background())
	require.Nil(t, err)
	require.Equal(t, uint64(8), flushedTs)

	require.Nil(t, sink.Close())
}

func TestExecDMLRollbackErrDatabaseNotExists(t *testing.T) {
	rows := []*model.RowChangedEvent{
		{
			Table: &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
			Columns: []*model.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
					Value: 1,
				},
			},
		},
		{
			Table: &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
			Columns: []*model.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
					Value: 2,
				},
			},
		},
	}

	errDatabaseNotExists := &dmysql.MySQLError{
		Number: uint16(infoschema.ErrDatabaseNotExists.Code()),
	}

	dbIndex := 0
	mockGetDBConnErrDatabaseNotExists := func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		defer func() { dbIndex++ }()

		if dbIndex == 0 {
			// test db
			db, err := mockTestDB(true)
			require.Nil(t, err)
			return db, nil
		}

		// normal db
		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		require.Nil(t, err)
		mock.ExpectBegin()
		mock.ExpectExec("REPLACE INTO `s1`.`t1`(`a`) VALUES (?),(?)").
			WithArgs(1, 2).
			WillReturnError(errDatabaseNotExists)
		mock.ExpectRollback()
		mock.ExpectClose()
		return db, nil
	}

	backupGetDBConn := GetDBConnImpl
	GetDBConnImpl = mockGetDBConnErrDatabaseNotExists
	defer func() { GetDBConnImpl = backupGetDBConn }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changefeed := "test-changefeed"
	sinkURI, err := url.Parse("mysql://127.0.0.1:4000/?time-zone=UTC&worker-count=1")
	require.Nil(t, err)
	sink, err := NewMySQLSink(ctx, model.DefaultChangeFeedID(changefeed), sinkURI)
	require.Nil(t, err)

	_ = sink.OnTxnEvent(&eventsink.TxnCallbackableEvent{
		Event: &model.SingleTableTxn{Rows: rows},
	})
	err = sink.Flush(context.Background())
	require.Equal(t, errDatabaseNotExists, errors.Cause(err))

	require.Nil(t, sink.Close())
}

func TestExecDMLRollbackErrTableNotExists(t *testing.T) {
	rows := []*model.RowChangedEvent{
		{
			Table: &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
			Columns: []*model.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
					Value: 1,
				},
			},
		},
		{
			Table: &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
			Columns: []*model.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
					Value: 2,
				},
			},
		},
	}

	errTableNotExists := &dmysql.MySQLError{
		Number: uint16(infoschema.ErrTableNotExists.Code()),
	}

	dbIndex := 0
	mockGetDBConnErrDatabaseNotExists := func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		defer func() { dbIndex++ }()

		if dbIndex == 0 {
			// test db
			db, err := mockTestDB(true)
			require.Nil(t, err)
			return db, nil
		}

		// normal db
		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		require.Nil(t, err)
		mock.ExpectBegin()
		mock.ExpectExec("REPLACE INTO `s1`.`t1`(`a`) VALUES (?),(?)").
			WithArgs(1, 2).
			WillReturnError(errTableNotExists)
		mock.ExpectRollback()
		mock.ExpectClose()
		return db, nil
	}

	backupGetDBConn := GetDBConnImpl
	GetDBConnImpl = mockGetDBConnErrDatabaseNotExists
	defer func() { GetDBConnImpl = backupGetDBConn }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changefeed := "test-changefeed"
	sinkURI, err := url.Parse("mysql://127.0.0.1:4000/?time-zone=UTC&worker-count=1")
	require.Nil(t, err)
	sink, err := NewMySQLSink(ctx, model.DefaultChangeFeedID(changefeed), sinkURI)
	require.Nil(t, err)

	_ = sink.OnTxnEvent(&eventsink.TxnCallbackableEvent{
		Event: &model.SingleTableTxn{Rows: rows},
	})
	err = sink.Flush(context.Background())
	require.Equal(t, errTableNotExists, errors.Cause(err))

	require.Nil(t, sink.Close())
}

func TestExecDMLRollbackErrRetryable(t *testing.T) {
	rows := []*model.RowChangedEvent{
		{
			Table: &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
			Columns: []*model.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
					Value: 1,
				},
			},
		},
		{
			Table: &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
			Columns: []*model.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
					Value: 2,
				},
			},
		},
	}

	errLockDeadlock := &dmysql.MySQLError{
		Number: mysql.ErrLockDeadlock,
	}

	dbIndex := 0
	mockGetDBConnErrDatabaseNotExists := func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		defer func() { dbIndex++ }()

		if dbIndex == 0 {
			// test db
			db, err := mockTestDB(true)
			require.Nil(t, err)
			return db, nil
		}

		// normal db
		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		require.Nil(t, err)
		for i := 0; i < int(defaultDMLMaxRetry); i++ {
			mock.ExpectBegin()
			mock.ExpectExec("REPLACE INTO `s1`.`t1`(`a`) VALUES (?),(?)").
				WithArgs(1, 2).
				WillReturnError(errLockDeadlock)
			mock.ExpectRollback()
		}
		mock.ExpectClose()
		return db, nil
	}
	backupGetDBConn := GetDBConnImpl
	GetDBConnImpl = mockGetDBConnErrDatabaseNotExists
	backupMaxRetry := defaultDMLMaxRetry
	defaultDMLMaxRetry = 2
	defer func() {
		GetDBConnImpl = backupGetDBConn
		defaultDMLMaxRetry = backupMaxRetry
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changefeed := "test-changefeed"
	sinkURI, err := url.Parse("mysql://127.0.0.1:4000/?time-zone=UTC&worker-count=1")
	require.Nil(t, err)
	sink, err := NewMySQLSink(ctx, model.DefaultChangeFeedID(changefeed), sinkURI)
	require.Nil(t, err)

	_ = sink.OnTxnEvent(&eventsink.TxnCallbackableEvent{
		Event: &model.SingleTableTxn{Rows: rows},
	})
	err = sink.Flush(context.Background())
	require.Equal(t, errLockDeadlock, errors.Cause(err))

	require.Nil(t, sink.Close())
}

func TestMysqlSinkNotRetryErrDupEntry(t *testing.T) {
	errDup := mysql.NewErr(mysql.ErrDupEntry)
	rows := []*model.RowChangedEvent{
		{
			StartTs:       2,
			CommitTs:      3,
			ReplicatingTs: 1,
			Table:         &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
			Columns: []*model.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
					Value: 1,
				},
			},
		},
	}

	dbIndex := 0
	mockDBInsertDupEntry := func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		defer func() { dbIndex++ }()

		if dbIndex == 0 {
			// test db
			db, err := mockTestDB(true)
			require.Nil(t, err)
			return db, nil
		}

		// normal db
		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		require.Nil(t, err)
		mock.ExpectBegin()
		mock.ExpectExec("INSERT INTO `s1`.`t1`(`a`) VALUES (?)").
			WithArgs(1).
			WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectCommit().
			WillReturnError(errDup)
		mock.ExpectClose()
		return db, nil
	}
	backupGetDBConn := GetDBConnImpl
	GetDBConnImpl = mockDBInsertDupEntry
	backupMaxRetry := defaultDMLMaxRetry
	defaultDMLMaxRetry = 1
	defer func() {
		GetDBConnImpl = backupGetDBConn
		defaultDMLMaxRetry = backupMaxRetry
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changefeed := "test-changefeed"
	sinkURI, err := url.Parse("mysql://127.0.0.1:4000/?time-zone=UTC&worker-count=1&safe-mode=false")
	require.Nil(t, err)
	sink, err := NewMySQLSink(ctx, model.DefaultChangeFeedID(changefeed), sinkURI)
	require.Nil(t, err)

	_ = sink.OnTxnEvent(&eventsink.TxnCallbackableEvent{
		Event: &model.SingleTableTxn{Rows: rows},
	})
	err = sink.Flush(context.Background())
	require.Equal(t, errDup, errors.Cause(err))

	require.Nil(t, sink.Close())
}

func TestNewMySQLSinkExecDDL(t *testing.T) {
	// TODO: fill it.
}

func TestNeedSwitchDB(t *testing.T) {
	// TODO: fill it.
}

func TestNewMySQLSink(t *testing.T) {
	dbIndex := 0
	mockGetDBConn := func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		defer func() { dbIndex++ }()

		if dbIndex == 0 {
			// test db
			db, err := mockTestDB(true)
			require.Nil(t, err)
			return db, nil
		}

		// normal db
		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		mock.ExpectClose()
		require.Nil(t, err)
		return db, nil
	}
	backupGetDBConn := GetDBConnImpl
	GetDBConnImpl = mockGetDBConn
	defer func() { GetDBConnImpl = backupGetDBConn }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	changefeed := "test-changefeed"
	sinkURI, err := url.Parse("mysql://127.0.0.1:4000/?time-zone=UTC&worker-count=4")
	require.Nil(t, err)
	sink, err := NewMySQLSink(ctx, model.DefaultChangeFeedID(changefeed), sinkURI)

	require.Nil(t, err)
	require.Nil(t, sink.Close())
	// Test idempotency of `Close` interface
	require.Nil(t, sink.Close())
}

func TestNewMySQLSinkWithIPv6Address(t *testing.T) {
	dbIndex := 0
	mockGetDBConn := func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		require.Contains(t, dsnStr, "root@tcp([::1]:3306)")
		defer func() { dbIndex++ }()

		if dbIndex == 0 {
			// test db
			db, err := mockTestDB(true)
			require.Nil(t, err)
			return db, nil
		}

		// normal db
		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		mock.ExpectClose()
		require.Nil(t, err)
		return db, nil
	}
	backupGetDBConn := GetDBConnImpl
	GetDBConnImpl = mockGetDBConn
	defer func() { GetDBConnImpl = backupGetDBConn }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changefeed := model.DefaultChangeFeedID("test-changefeed")
	// See https://www.ietf.org/rfc/rfc2732.txt, we have to use brackets to wrap IPv6 address.
	sinkURI, err := url.Parse("mysql://[::1]:3306/?time-zone=UTC&worker-count=4")
	require.Nil(t, err)
	sink, err := NewMySQLSink(ctx, changefeed, sinkURI)
	require.Nil(t, err)
	require.Nil(t, sink.Close())
}

func TestGBKSupported(t *testing.T) {
	dbIndex := 0
	mockGetDBConn := func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		defer func() { dbIndex++ }()

		if dbIndex == 0 {
			// test db
			db, err := mockTestDB(true)
			require.Nil(t, err)
			return db, nil
		}

		// normal db
		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		mock.ExpectClose()
		require.Nil(t, err)
		return db, nil
	}
	backupGetDBConn := GetDBConnImpl
	GetDBConnImpl = mockGetDBConn
	defer func() { GetDBConnImpl = backupGetDBConn }()

	zapcore, logs := observer.New(zap.WarnLevel)
	conf := &log.Config{Level: "warn", File: log.FileLogConfig{}}
	_, r, _ := log.InitLogger(conf)
	logger := zap.New(zapcore)
	restoreFn := log.ReplaceGlobals(logger, r)
	defer restoreFn()

	ctx := context.Background()
	changefeed := "test-changefeed"
	sinkURI, err := url.Parse("mysql://127.0.0.1:4000/?time-zone=UTC&worker-count=4")
	require.Nil(t, err)
	sink, err := NewMySQLSink(ctx, model.DefaultChangeFeedID(changefeed), sinkURI)
	require.Nil(t, err)

	// no gbk-related warning log will be output because GBK charset is supported
	require.Equal(t, logs.FilterMessage("gbk charset is not supported").Len(), 0)

	require.Nil(t, sink.Close())
}

func TestHolderString(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		count    int
		expected string
	}{
		{1, "?"},
		{2, "?,?"},
		{10, "?,?,?,?,?,?,?,?,?,?"},
	}
	for _, tc := range testCases {
		s := placeHolder(tc.count)
		require.Equal(t, tc.expected, s)
	}
	// test invalid input
	require.Panics(t, func() { placeHolder(0) }, "strings.Builder.Grow: negative count")
	require.Panics(t, func() { placeHolder(-1) }, "strings.Builder.Grow: negative count")
}

func TestMySQLSinkExecDMLError(t *testing.T) {
	dbIndex := 0
	mockGetDBConn := func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		defer func() { dbIndex++ }()

		if dbIndex == 0 {
			// test db
			db, err := mockTestDB(true)
			require.Nil(t, err)
			return db, nil
		}

		// normal db
		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		require.Nil(t, err)
		mock.ExpectBegin()
		mock.ExpectExec("REPLACE INTO `s1`.`t1`(`a`,`b`) VALUES (?,?)").WillDelayFor(1 * time.Second).
			WillReturnError(&dmysql.MySQLError{Number: mysql.ErrNoSuchTable})
		mock.ExpectClose()
		return db, nil
	}
	backupGetDBConn := GetDBConnImpl
	GetDBConnImpl = mockGetDBConn
	defer func() { GetDBConnImpl = backupGetDBConn }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changefeed := "test-changefeed"
	sinkURI, err := url.Parse("mysql://127.0.0.1:4000/?time-zone=UTC&worker-count=1&batch-replace-size=1")
	require.Nil(t, err)
	sink, err := NewMySQLSink(ctx, model.DefaultChangeFeedID(changefeed), sinkURI)
	require.Nil(t, err)

	rows := []*model.RowChangedEvent{
		{
			StartTs:  1,
			CommitTs: 2,
			Table:    &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
			Columns: []*model.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
					Value: 1,
				},
				{
					Name:  "b",
					Type:  mysql.TypeVarchar,
					Flag:  0,
					Value: "test",
				},
			},
		},
		{
			StartTs:  2,
			CommitTs: 3,
			Table:    &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
			Columns: []*model.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
					Value: 2,
				},
				{
					Name:  "b",
					Type:  mysql.TypeVarchar,
					Flag:  0,
					Value: "test",
				},
			},
		},
		{
			StartTs:  3,
			CommitTs: 4,
			Table:    &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
			Columns: []*model.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
					Value: 3,
				},
				{
					Name:  "b",
					Type:  mysql.TypeVarchar,
					Flag:  0,
					Value: "test",
				},
			},
		},
		{
			StartTs:  4,
			CommitTs: 5,
			Table:    &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
			Columns: []*model.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
					Value: 1,
				},
				{
					Name:  "b",
					Type:  mysql.TypeVarchar,
					Flag:  0,
					Value: "test",
				},
			},
		},
		{
			StartTs:  5,
			CommitTs: 6,
			Table:    &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
			Columns: []*model.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
					Value: 2,
				},
				{
					Name:  "b",
					Type:  mysql.TypeVarchar,
					Flag:  0,
					Value: "test",
				},
			},
		},
	}

	_ = sink.OnTxnEvent(&eventsink.TxnCallbackableEvent{
		Event: &model.SingleTableTxn{Rows: rows},
	})
	err = sink.Flush(context.Background())
	require.Regexp(t, ".*ErrMySQLTxnError.*", err)
	require.Nil(t, sink.Close())
}

func TestMysqlSinkSafeModeOff(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		input    []*model.RowChangedEvent
		expected *preparedDMLs
	}{
		{
			name:  "empty",
			input: []*model.RowChangedEvent{},
			expected: &preparedDMLs{
				startTs: []model.Ts{},
				sqls:    []string{},
				values:  [][]interface{}{},
			},
		}, {
			name: "insert without PK",
			input: []*model.RowChangedEvent{
				{
					StartTs:       418658114257813514,
					CommitTs:      418658114257813515,
					ReplicatingTs: 418658114257813513,
					Table:         &model.TableName{Schema: "common_1", Table: "uk_without_pk"},
					Columns: []*model.Column{nil, {
						Name:  "a1",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag,
						Value: 1,
					}, {
						Name:  "a3",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag,
						Value: 1,
					}},
				},
			},
			expected: &preparedDMLs{
				startTs: []model.Ts{418658114257813514},
				sqls: []string{
					"INSERT INTO `common_1`.`uk_without_pk`(`a1`,`a3`) VALUES (?,?);",
				},
				values:   [][]interface{}{{1, 1}},
				rowCount: 1,
			},
		}, {
			name: "insert with PK",
			input: []*model.RowChangedEvent{
				{
					StartTs:       418658114257813514,
					CommitTs:      418658114257813515,
					ReplicatingTs: 418658114257813513,
					Table:         &model.TableName{Schema: "common_1", Table: "pk"},
					Columns: []*model.Column{nil, {
						Name:  "a1",
						Type:  mysql.TypeLong,
						Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
						Value: 1,
					}, {
						Name:  "a3",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag,
						Value: 1,
					}},
				},
			},
			expected: &preparedDMLs{
				startTs:  []model.Ts{418658114257813514},
				sqls:     []string{"INSERT INTO `common_1`.`pk`(`a1`,`a3`) VALUES (?,?);"},
				values:   [][]interface{}{{1, 1}},
				rowCount: 1,
			},
		}, {
			name: "update without PK",
			input: []*model.RowChangedEvent{
				{
					StartTs:       418658114257813516,
					CommitTs:      418658114257813517,
					ReplicatingTs: 418658114257813515,
					Table:         &model.TableName{Schema: "common_1", Table: "uk_without_pk"},
					PreColumns: []*model.Column{nil, {
						Name:  "a1",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag,
						Value: 2,
					}, {
						Name:  "a3",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag,
						Value: 2,
					}},
					Columns: []*model.Column{nil, {
						Name:  "a1",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag,
						Value: 3,
					}, {
						Name:  "a3",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag,
						Value: 3,
					}},
				},
			},
			expected: &preparedDMLs{
				startTs: []model.Ts{418658114257813516},
				sqls: []string{
					"UPDATE `common_1`.`uk_without_pk` SET `a1`=?,`a3`=? " +
						"WHERE `a1`=? AND `a3`=? LIMIT 1;",
				},
				values:   [][]interface{}{{3, 3, 2, 2}},
				rowCount: 1,
			},
		}, {
			name: "update with PK",
			input: []*model.RowChangedEvent{
				{
					StartTs:       418658114257813516,
					CommitTs:      418658114257813517,
					ReplicatingTs: 418658114257813515,
					Table:         &model.TableName{Schema: "common_1", Table: "pk"},
					PreColumns: []*model.Column{nil, {
						Name:  "a1",
						Type:  mysql.TypeLong,
						Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
						Value: 2,
					}, {
						Name:  "a3",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag,
						Value: 2,
					}},
					Columns: []*model.Column{nil, {
						Name:  "a1",
						Type:  mysql.TypeLong,
						Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
						Value: 3,
					}, {
						Name:  "a3",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag,
						Value: 3,
					}},
				},
			},
			expected: &preparedDMLs{
				startTs: []model.Ts{418658114257813516},
				sqls: []string{"UPDATE `common_1`.`pk` SET `a1`=?,`a3`=? " +
					"WHERE `a1`=? AND `a3`=? LIMIT 1;"},
				values:   [][]interface{}{{3, 3, 2, 2}},
				rowCount: 1,
			},
		}, {
			name: "batch insert with PK",
			input: []*model.RowChangedEvent{
				{
					StartTs:       418658114257813516,
					CommitTs:      418658114257813517,
					ReplicatingTs: 418658114257813515,
					Table:         &model.TableName{Schema: "common_1", Table: "pk"},
					Columns: []*model.Column{nil, {
						Name:  "a1",
						Type:  mysql.TypeLong,
						Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
						Value: 3,
					}, {
						Name:  "a3",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag,
						Value: 3,
					}},
				},
				{
					StartTs:       418658114257813516,
					CommitTs:      418658114257813517,
					ReplicatingTs: 418658114257813515,
					Table:         &model.TableName{Schema: "common_1", Table: "pk"},
					Columns: []*model.Column{nil, {
						Name:  "a1",
						Type:  mysql.TypeLong,
						Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
						Value: 5,
					}, {
						Name:  "a3",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag,
						Value: 5,
					}},
				},
			},
			expected: &preparedDMLs{
				startTs: []model.Ts{418658114257813516},
				sqls: []string{
					"INSERT INTO `common_1`.`pk`(`a1`,`a3`) VALUES (?,?);",
					"INSERT INTO `common_1`.`pk`(`a1`,`a3`) VALUES (?,?);",
				},
				values:   [][]interface{}{{3, 3}, {5, 5}},
				rowCount: 2,
			},
		}, {
			name: "safe mode on commit ts < replicating ts",
			input: []*model.RowChangedEvent{
				{
					StartTs:       418658114257813516,
					CommitTs:      418658114257813517,
					ReplicatingTs: 418658114257813518,
					Table:         &model.TableName{Schema: "common_1", Table: "pk"},
					Columns: []*model.Column{nil, {
						Name:  "a1",
						Type:  mysql.TypeLong,
						Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
						Value: 3,
					}, {
						Name:  "a3",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag,
						Value: 3,
					}},
				},
			},
			expected: &preparedDMLs{
				startTs: []model.Ts{418658114257813516},
				sqls: []string{
					"REPLACE INTO `common_1`.`pk`(`a1`,`a3`) VALUES (?,?);",
				},
				values:   [][]interface{}{{3, 3}},
				rowCount: 1,
			},
		}, {
			name: "safe mode on one row commit ts < replicating ts",
			input: []*model.RowChangedEvent{
				{
					StartTs:       418658114257813516,
					CommitTs:      418658114257813517,
					ReplicatingTs: 418658114257813518,
					Table:         &model.TableName{Schema: "common_1", Table: "pk"},
					Columns: []*model.Column{nil, {
						Name:  "a1",
						Type:  mysql.TypeLong,
						Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
						Value: 3,
					}, {
						Name:  "a3",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag,
						Value: 3,
					}},
				},
				{
					StartTs:       418658114257813506,
					CommitTs:      418658114257813507,
					ReplicatingTs: 418658114257813505,
					Table:         &model.TableName{Schema: "common_1", Table: "pk"},
					Columns: []*model.Column{nil, {
						Name:  "a1",
						Type:  mysql.TypeLong,
						Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
						Value: 5,
					}, {
						Name:  "a3",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag,
						Value: 5,
					}},
				},
			},
			expected: &preparedDMLs{
				startTs: []model.Ts{418658114257813516, 418658114257813506},
				sqls: []string{
					"REPLACE INTO `common_1`.`pk`(`a1`,`a3`) VALUES (?,?);",
					"REPLACE INTO `common_1`.`pk`(`a1`,`a3`) VALUES (?,?);",
				},
				values:   [][]interface{}{{3, 3}, {5, 5}},
				rowCount: 2,
			},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ms := newMySQLSink4Test(ctx, t)
	ms.params.safeMode = false
	ms.options.enableOldValue = true
	for _, tc := range testCases {
		ms.events = make([]*eventsink.TxnCallbackableEvent, 1)
		ms.events[0] = &eventsink.TxnCallbackableEvent{
			Event: &model.SingleTableTxn{Rows: tc.input},
		}
		ms.rows = len(tc.input)
		dmls := ms.prepareDMLs()
		require.Equal(t, tc.expected, dmls, tc.name)
	}
}
