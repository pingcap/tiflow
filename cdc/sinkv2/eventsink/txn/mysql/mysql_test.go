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
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink"
	"github.com/pingcap/tiflow/cdc/sinkv2/metrics"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/sink"
	pmysql "github.com/pingcap/tiflow/pkg/sink/mysql"
	"github.com/pingcap/tiflow/pkg/sqlmodel"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func newMySQLBackendWithoutDB(ctx context.Context) *mysqlBackend {
	cfg := pmysql.NewConfig()
	cfg.BatchReplaceEnabled = false
	cfg.BatchDMLEnable = false
	return &mysqlBackend{
		statistics: metrics.NewStatistics(ctx, sink.TxnSink),
		cfg:        cfg,
	}
}

func newMySQLBackend(
	ctx context.Context,
	sinkURI *url.URL,
	replicaConfig *config.ReplicaConfig,
	dbConnFactory pmysql.Factory,
) (*mysqlBackend, error) {
	ctx1, cancel := context.WithCancel(ctx)
	statistics := metrics.NewStatistics(ctx1, sink.TxnSink)
	cancel() // Cancel background goroutines in returned metrics.Statistics.
	raw := sinkURI.Query()
	raw.Set("batch-dml-enable", "true")
	sinkURI.RawQuery = raw.Encode()

	backends, err := NewMySQLBackends(ctx, sinkURI, replicaConfig, dbConnFactory, statistics)
	if err != nil {
		return nil, err
	}
	return backends[0], nil
}

func newTestMockDB(t *testing.T) (db *sql.DB, mock sqlmock.Sqlmock) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	mock.ExpectQuery("select tidb_version()").WillReturnError(&dmysql.MySQLError{
		Number:  1305,
		Message: "FUNCTION test.tidb_version does not exist",
	})
	mock.ExpectQuery("select tidb_version()").WillReturnError(&dmysql.MySQLError{
		Number:  1305,
		Message: "FUNCTION test.tidb_version does not exist",
	})
	require.Nil(t, err)
	return
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
				startTs:         []model.Ts{418658114257813514},
				sqls:            []string{"DELETE FROM `common_1`.`uk_without_pk` WHERE `a1` = ? AND `a3` = ? LIMIT 1"},
				values:          [][]interface{}{{1, 1}},
				rowCount:        1,
				approximateSize: 74,
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
				startTs:         []model.Ts{418658114257813516},
				sqls:            []string{"REPLACE INTO `common_1`.`uk_without_pk` (`a1`,`a3`) VALUES (?,?)"},
				values:          [][]interface{}{{2, 2}},
				rowCount:        1,
				approximateSize: 64,
			},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ms := newMySQLBackendWithoutDB(ctx)
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
			db, err := pmysql.MockTestDB(true)
			require.Nil(t, err)
			return db, nil
		}

		// normal db
		db, mock := newTestMockDB(t)
		mock.ExpectClose()
		return db, nil
	}

	changefeed := "test-changefeed"
	contextutil.PutChangefeedIDInCtx(ctx, model.DefaultChangeFeedID(changefeed))
	sinkURI, err := url.Parse("mysql://127.0.0.1:4000/?time-zone=UTC&worker-count=1")
	require.Nil(t, err)
	sink, err := newMySQLBackend(ctx, sinkURI, config.GetDefaultReplicaConfig(), mockGetDBConn)
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
	contextutil.PutChangefeedIDInCtx(ctx, model.DefaultChangeFeedID(changefeed))
	sinkURI, err := url.Parse(fmt.Sprintf("mysql://%s/?read-timeout=1s&timeout=1s", addr))
	require.Nil(t, err)
	_, err = newMySQLBackend(ctx, sinkURI,
		config.GetDefaultReplicaConfig(), pmysql.CreateMySQLDBConn)
	require.Equal(t, driver.ErrBadConn, errors.Cause(err))
}

// Test OnTxnEvent and Flush interfaces. Event callbacks should be called correctly after flush.
func TestNewMySQLBackendExecDML(t *testing.T) {
	dbIndex := 0
	mockGetDBConn := func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		defer func() { dbIndex++ }()

		if dbIndex == 0 {
			// test db
			db, err := pmysql.MockTestDB(true)
			require.Nil(t, err)
			return db, nil
		}

		// normal db
		db, mock := newTestMockDB(t)
		mock.ExpectBegin()
		mock.ExpectExec("INSERT INTO `s1`.`t1` (`a`,`b`) VALUES (?,?),(?,?)").
			WithArgs(1, "test", 2, "test").
			WillReturnResult(sqlmock.NewResult(2, 2))
		mock.ExpectCommit()
		mock.ExpectClose()
		return db, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changefeed := "test-changefeed"
	contextutil.PutChangefeedIDInCtx(ctx, model.DefaultChangeFeedID(changefeed))
	sinkURI, err := url.Parse("mysql://127.0.0.1:4000/?time-zone=UTC&worker-count=1")
	require.Nil(t, err)
	sink, err := newMySQLBackend(ctx, sinkURI,
		config.GetDefaultReplicaConfig(), mockGetDBConn)
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
	require.Equal(t, uint64(6), flushedTs)

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
			db, err := pmysql.MockTestDB(true)
			require.Nil(t, err)
			return db, nil
		}

		// normal db
		db, mock := newTestMockDB(t)
		mock.ExpectBegin()
		mock.ExpectExec("REPLACE INTO `s1`.`t1` (`a`) VALUES (?),(?)").
			WithArgs(1, 2).
			WillReturnError(errDatabaseNotExists)
		mock.ExpectRollback()
		mock.ExpectClose()
		return db, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changefeed := "test-changefeed"
	contextutil.PutChangefeedIDInCtx(ctx, model.DefaultChangeFeedID(changefeed))
	sinkURI, err := url.Parse("mysql://127.0.0.1:4000/?time-zone=UTC&worker-count=1")
	require.Nil(t, err)
	sink, err := newMySQLBackend(ctx, sinkURI,
		config.GetDefaultReplicaConfig(), mockGetDBConnErrDatabaseNotExists)
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
			db, err := pmysql.MockTestDB(true)
			require.Nil(t, err)
			return db, nil
		}

		// normal db
		db, mock := newTestMockDB(t)
		mock.ExpectBegin()
		mock.ExpectExec("REPLACE INTO `s1`.`t1` (`a`) VALUES (?),(?)").
			WithArgs(1, 2).
			WillReturnError(errTableNotExists)
		mock.ExpectRollback()
		mock.ExpectClose()
		return db, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changefeed := "test-changefeed"
	contextutil.PutChangefeedIDInCtx(ctx, model.DefaultChangeFeedID(changefeed))
	sinkURI, err := url.Parse("mysql://127.0.0.1:4000/?time-zone=UTC&worker-count=1")
	require.Nil(t, err)
	sink, err := newMySQLBackend(ctx, sinkURI,
		config.GetDefaultReplicaConfig(), mockGetDBConnErrDatabaseNotExists)
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
			db, err := pmysql.MockTestDB(true)
			require.Nil(t, err)
			return db, nil
		}

		// normal db
		db, mock := newTestMockDB(t)
		for i := 0; i < 2; i++ {
			mock.ExpectBegin()
			mock.ExpectExec("REPLACE INTO `s1`.`t1` (`a`) VALUES (?),(?)").
				WithArgs(1, 2).
				WillReturnError(errLockDeadlock)
			mock.ExpectRollback()
		}
		mock.ExpectClose()
		return db, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changefeed := "test-changefeed"
	contextutil.PutChangefeedIDInCtx(ctx, model.DefaultChangeFeedID(changefeed))
	sinkURI, err := url.Parse("mysql://127.0.0.1:4000/?time-zone=UTC&worker-count=1")
	require.Nil(t, err)
	sink, err := newMySQLBackend(ctx, sinkURI,
		config.GetDefaultReplicaConfig(), mockGetDBConnErrDatabaseNotExists)
	require.Nil(t, err)
	sink.setDMLMaxRetry(2)

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
			db, err := pmysql.MockTestDB(true)
			require.Nil(t, err)
			return db, nil
		}

		// normal db
		db, mock := newTestMockDB(t)
		mock.ExpectBegin()
		mock.ExpectExec("INSERT INTO `s1`.`t1` (`a`) VALUES (?)").
			WithArgs(1).
			WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectCommit().
			WillReturnError(errDup)
		mock.ExpectClose()
		return db, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changefeed := "test-changefeed"
	contextutil.PutChangefeedIDInCtx(ctx, model.DefaultChangeFeedID(changefeed))
	sinkURI, err := url.Parse("mysql://127.0.0.1:4000/?time-zone=UTC&worker-count=1&safe-mode=false")
	require.Nil(t, err)
	sink, err := newMySQLBackend(ctx, sinkURI,
		config.GetDefaultReplicaConfig(), mockDBInsertDupEntry)
	require.Nil(t, err)
	sink.setDMLMaxRetry(1)
	_ = sink.OnTxnEvent(&eventsink.TxnCallbackableEvent{
		Event: &model.SingleTableTxn{Rows: rows},
	})
	err = sink.Flush(context.Background())
	require.Equal(t, errDup, errors.Cause(err))

	require.Nil(t, sink.Close())
}

func TestNewMySQLBackendExecDDL(t *testing.T) {
	// TODO: fill it.
}

func TestNeedSwitchDB(t *testing.T) {
	// TODO: fill it.
}

func TestNewMySQLBackend(t *testing.T) {
	dbIndex := 0
	mockGetDBConn := func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		defer func() { dbIndex++ }()

		if dbIndex == 0 {
			// test db
			db, err := pmysql.MockTestDB(true)
			require.Nil(t, err)
			return db, nil
		}

		// normal db
		db, mock := newTestMockDB(t)
		mock.ExpectClose()
		return db, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	changefeed := "test-changefeed"
	contextutil.PutChangefeedIDInCtx(ctx, model.DefaultChangeFeedID(changefeed))
	sinkURI, err := url.Parse("mysql://127.0.0.1:4000/?time-zone=UTC&worker-count=1")
	require.Nil(t, err)
	sink, err := newMySQLBackend(ctx, sinkURI,
		config.GetDefaultReplicaConfig(), mockGetDBConn)

	require.Nil(t, err)
	require.Nil(t, sink.Close())
	// Test idempotency of `Close` interface
	require.Nil(t, sink.Close())
}

func TestNewMySQLBackendWithIPv6Address(t *testing.T) {
	dbIndex := 0
	mockGetDBConn := func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		require.Contains(t, dsnStr, "root@tcp([::1]:3306)")
		defer func() { dbIndex++ }()

		if dbIndex == 0 {
			// test db
			db, err := pmysql.MockTestDB(true)
			require.Nil(t, err)
			return db, nil
		}

		// normal db
		db, mock := newTestMockDB(t)
		mock.ExpectClose()
		return db, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changefeed := "test-changefeed"
	contextutil.PutChangefeedIDInCtx(ctx, model.DefaultChangeFeedID(changefeed))
	// See https://www.ietf.org/rfc/rfc2732.txt, we have to use brackets to wrap IPv6 address.
	sinkURI, err := url.Parse("mysql://[::1]:3306/?time-zone=UTC&worker-count=1")
	require.Nil(t, err)
	sink, err := newMySQLBackend(ctx, sinkURI,
		config.GetDefaultReplicaConfig(), mockGetDBConn)
	require.Nil(t, err)
	require.Nil(t, sink.Close())
}

func TestGBKSupported(t *testing.T) {
	dbIndex := 0
	mockGetDBConn := func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		defer func() { dbIndex++ }()

		if dbIndex == 0 {
			// test db
			db, err := pmysql.MockTestDB(true)
			require.Nil(t, err)
			return db, nil
		}

		// normal db
		db, mock := newTestMockDB(t)
		mock.ExpectClose()
		return db, nil
	}

	zapcore, logs := observer.New(zap.WarnLevel)
	conf := &log.Config{Level: "warn", File: log.FileLogConfig{}}
	_, r, _ := log.InitLogger(conf)
	logger := zap.New(zapcore)
	restoreFn := log.ReplaceGlobals(logger, r)
	defer restoreFn()

	ctx := context.Background()
	changefeed := "test-changefeed"
	contextutil.PutChangefeedIDInCtx(ctx, model.DefaultChangeFeedID(changefeed))
	sinkURI, err := url.Parse("mysql://127.0.0.1:4000/?time-zone=UTC&worker-count=1")
	require.Nil(t, err)
	sink, err := newMySQLBackend(ctx, sinkURI,
		config.GetDefaultReplicaConfig(), mockGetDBConn)
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
			db, err := pmysql.MockTestDB(true)
			require.Nil(t, err)
			return db, nil
		}

		// normal db
		db, mock := newTestMockDB(t)
		mock.ExpectBegin()
		mock.ExpectExec("INSERT INTO `s1`.`t1` (`a`,`b`) VALUES (?,?),(?,?)").WillDelayFor(1 * time.Second).
			WillReturnError(&dmysql.MySQLError{Number: mysql.ErrNoSuchTable})
		mock.ExpectClose()
		return db, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changefeed := "test-changefeed"
	contextutil.PutChangefeedIDInCtx(ctx, model.DefaultChangeFeedID(changefeed))
	sinkURI, err := url.Parse("mysql://127.0.0.1:4000/?time-zone=UTC&worker-count=1&batch-replace-size=1")
	require.Nil(t, err)
	sink, err := newMySQLBackend(ctx, sinkURI,
		config.GetDefaultReplicaConfig(), mockGetDBConn)
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
					"INSERT INTO `common_1`.`uk_without_pk` (`a1`,`a3`) VALUES (?,?)",
				},
				values:          [][]interface{}{{1, 1}},
				rowCount:        1,
				approximateSize: 63,
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
				startTs:         []model.Ts{418658114257813514},
				sqls:            []string{"INSERT INTO `common_1`.`pk` (`a1`,`a3`) VALUES (?,?)"},
				values:          [][]interface{}{{1, 1}},
				rowCount:        1,
				approximateSize: 52,
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
						"WHERE `a1`=? AND `a3`=? LIMIT 1",
				},
				values:          [][]interface{}{{3, 3, 2, 2}},
				rowCount:        1,
				approximateSize: 83,
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
					"WHERE `a1`=? AND `a3`=? LIMIT 1"},
				values:          [][]interface{}{{3, 3, 2, 2}},
				rowCount:        1,
				approximateSize: 72,
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
					"INSERT INTO `common_1`.`pk` (`a1`,`a3`) VALUES (?,?)",
					"INSERT INTO `common_1`.`pk` (`a1`,`a3`) VALUES (?,?)",
				},
				values:          [][]interface{}{{3, 3}, {5, 5}},
				rowCount:        2,
				approximateSize: 104,
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
					"REPLACE INTO `common_1`.`pk` (`a1`,`a3`) VALUES (?,?)",
				},
				values:          [][]interface{}{{3, 3}},
				rowCount:        1,
				approximateSize: 53,
			},
		}, {
			name: "safe mode on and txn's commit ts < replicating ts",
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
					StartTs:       418658114257813516,
					CommitTs:      418658114257813517,
					ReplicatingTs: 418658114257813518,
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
					"REPLACE INTO `common_1`.`pk` (`a1`,`a3`) VALUES (?,?)",
					"REPLACE INTO `common_1`.`pk` (`a1`,`a3`) VALUES (?,?)",
				},
				values:          [][]interface{}{{3, 3}, {5, 5}},
				rowCount:        2,
				approximateSize: 106,
			},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ms := newMySQLBackendWithoutDB(ctx)
	ms.cfg.SafeMode = false
	ms.cfg.EnableOldValue = true
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

func TestPrepareBatchDMLs(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		isTiDB   bool
		input    []*model.RowChangedEvent
		expected *preparedDMLs
	}{
		// empty event
		{
			isTiDB: true,
			input:  []*model.RowChangedEvent{},
			expected: &preparedDMLs{
				startTs: []model.Ts{},
				sqls:    []string{},
				values:  [][]interface{}{},
			},
		},
		{ // delete event
			isTiDB: false,
			input: []*model.RowChangedEvent{
				{
					StartTs:  418658114257813514,
					CommitTs: 418658114257813515,
					Table:    &model.TableName{Schema: "common_1", Table: "uk_without_pk"},
					PreColumns: []*model.Column{nil, {
						Name:  "a1",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag | model.UniqueKeyFlag,
						Value: 1,
					}, {
						Name:    "a3",
						Type:    mysql.TypeVarchar,
						Charset: charset.CharsetGBK,
						Flag: model.BinaryFlag | model.MultipleKeyFlag |
							model.HandleKeyFlag | model.UniqueKeyFlag,
						Value: []byte("你好"),
					}},
					IndexColumns: [][]int{{1, 2}},
				},
				{
					StartTs:  418658114257813514,
					CommitTs: 418658114257813515,
					Table:    &model.TableName{Schema: "common_1", Table: "uk_without_pk"},
					PreColumns: []*model.Column{nil, {
						Name:  "a1",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag | model.UniqueKeyFlag,
						Value: 2,
					}, {
						Name:    "a3",
						Type:    mysql.TypeVarchar,
						Charset: charset.CharsetGBK,
						Flag: model.BinaryFlag | model.MultipleKeyFlag |
							model.HandleKeyFlag | model.UniqueKeyFlag,
						Value: []byte("世界"),
					}},
					IndexColumns: [][]int{{1, 2}},
				},
			},
			expected: &preparedDMLs{
				startTs:         []model.Ts{418658114257813514},
				sqls:            []string{"DELETE FROM `common_1`.`uk_without_pk` WHERE (`a1`,`a3`) IN ((?,?),(?,?))"},
				values:          [][]interface{}{{1, "你好", 2, "世界"}},
				rowCount:        2,
				approximateSize: 1,
			},
		},
		{ // insert event
			isTiDB: true,
			input: []*model.RowChangedEvent{
				{
					StartTs:  418658114257813516,
					CommitTs: 418658114257813517,
					Table:    &model.TableName{Schema: "common_1", Table: "uk_without_pk"},
					Columns: []*model.Column{nil, {
						Name:  "a1",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag,
						Value: 1,
					}, {
						Name:    "a3",
						Type:    mysql.TypeVarchar,
						Charset: charset.CharsetGBK,
						Flag:    model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag,
						Value:   "你好",
					}},
					IndexColumns: [][]int{{1, 1}},
				},
				{
					StartTs:  418658114257813516,
					CommitTs: 418658114257813517,
					Table:    &model.TableName{Schema: "common_1", Table: "uk_without_pk"},
					Columns: []*model.Column{nil, {
						Name:  "a1",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag | model.HandleKeyFlag,
						Value: 2,
					}, {
						Name:    "a3",
						Type:    mysql.TypeVarchar,
						Charset: charset.CharsetGBK,
						Flag: model.BinaryFlag | model.MultipleKeyFlag |
							model.HandleKeyFlag | model.HandleKeyFlag,
						Value: "世界",
					}},
					IndexColumns: [][]int{{2, 2}},
				},
			},
			expected: &preparedDMLs{
				startTs:         []model.Ts{418658114257813516},
				sqls:            []string{"INSERT INTO `common_1`.`uk_without_pk` (`a1`,`a3`) VALUES (?,?),(?,?)"},
				values:          [][]interface{}{{1, "你好", 2, "世界"}},
				rowCount:        2,
				approximateSize: 1,
			},
		},
		// update event
		{
			isTiDB: true,
			input: []*model.RowChangedEvent{
				{
					StartTs:  418658114257813516,
					CommitTs: 418658114257813517,
					Table:    &model.TableName{Schema: "common_1", Table: "uk_without_pk"},
					PreColumns: []*model.Column{nil, {
						Name:  "a1",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag | model.UniqueKeyFlag,
						Value: 1,
					}, {
						Name:    "a3",
						Type:    mysql.TypeVarchar,
						Charset: charset.CharsetGBK,
						Flag: model.BinaryFlag | model.MultipleKeyFlag |
							model.HandleKeyFlag | model.UniqueKeyFlag,
						Value: []byte("开发"),
					}},
					Columns: []*model.Column{nil, {
						Name:  "a1",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag | model.UniqueKeyFlag,
						Value: 2,
					}, {
						Name:    "a3",
						Type:    mysql.TypeVarchar,
						Charset: charset.CharsetGBK,
						Flag: model.BinaryFlag | model.MultipleKeyFlag |
							model.HandleKeyFlag | model.UniqueKeyFlag,
						Value: []byte("测试"),
					}},
					IndexColumns: [][]int{{1, 2}},
				},
				{
					StartTs:  418658114257813516,
					CommitTs: 418658114257813517,
					Table:    &model.TableName{Schema: "common_1", Table: "uk_without_pk"},
					PreColumns: []*model.Column{nil, {
						Name:  "a1",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag | model.UniqueKeyFlag,
						Value: 3,
					}, {
						Name:    "a3",
						Type:    mysql.TypeVarchar,
						Charset: charset.CharsetGBK,
						Flag: model.BinaryFlag | model.MultipleKeyFlag |
							model.HandleKeyFlag | model.UniqueKeyFlag,
						Value: []byte("纽约"),
					}},
					Columns: []*model.Column{nil, {
						Name:  "a1",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag | model.UniqueKeyFlag,
						Value: 4,
					}, {
						Name:    "a3",
						Type:    mysql.TypeVarchar,
						Charset: charset.CharsetGBK,
						Flag: model.BinaryFlag | model.MultipleKeyFlag |
							model.HandleKeyFlag | model.UniqueKeyFlag,
						Value: []byte("北京"),
					}},
					IndexColumns: [][]int{{1, 2}},
				},
			},
			expected: &preparedDMLs{
				startTs: []model.Ts{418658114257813516},
				sqls: []string{"UPDATE `common_1`.`uk_without_pk` SET `a1`=CASE " +
					"WHEN ROW(`a1`,`a3`)=ROW(?,?) THEN ? WHEN ROW(`a1`,`a3`)=ROW(?,?) " +
					"THEN ? END, `a3`=CASE WHEN ROW(`a1`,`a3`)=ROW(?,?) " +
					"THEN ? WHEN ROW(`a1`,`a3`)=ROW(?,?) THEN ? END WHERE " +
					"ROW(`a1`,`a3`) IN (ROW(?,?),ROW(?,?))"},
				values: [][]interface{}{{
					1, "开发", 2, 3, "纽约", 4, 1, "开发", "测试", 3,
					"纽约", "北京", 1, "开发", 3, "纽约",
				}},
				rowCount:        2,
				approximateSize: 1,
			},
		},
		// mixed event, and test delete， update, insert are ordered
		{
			isTiDB: true,
			input: []*model.RowChangedEvent{
				{
					StartTs:  418658114257813514,
					CommitTs: 418658114257813515,
					Table:    &model.TableName{Schema: "common_1", Table: "uk_without_pk"},
					Columns: []*model.Column{nil, {
						Name:  "a1",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag | model.UniqueKeyFlag,
						Value: 2,
					}, {
						Name:    "a3",
						Type:    mysql.TypeVarchar,
						Charset: charset.CharsetGBK,
						Flag: model.BinaryFlag | model.MultipleKeyFlag |
							model.HandleKeyFlag | model.UniqueKeyFlag,
						Value: []byte("你好"),
					}},

					IndexColumns: [][]int{{1, 2}},
				},
				{
					StartTs:  418658114257813514,
					CommitTs: 418658114257813515,
					Table:    &model.TableName{Schema: "common_1", Table: "uk_without_pk"},
					PreColumns: []*model.Column{nil, {
						Name:  "a1",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag | model.UniqueKeyFlag,
						Value: 1,
					}, {
						Name:    "a3",
						Type:    mysql.TypeVarchar,
						Charset: charset.CharsetGBK,
						Flag: model.BinaryFlag | model.MultipleKeyFlag |
							model.HandleKeyFlag | model.UniqueKeyFlag,
						Value: []byte("世界"),
					}},
					IndexColumns: [][]int{{1, 2}},
				},
				{
					StartTs:  418658114257813514,
					CommitTs: 418658114257813515,
					Table:    &model.TableName{Schema: "common_1", Table: "uk_without_pk"},
					PreColumns: []*model.Column{nil, {
						Name:  "a1",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag | model.UniqueKeyFlag,
						Value: 2,
					}, {
						Name:    "a3",
						Type:    mysql.TypeVarchar,
						Charset: charset.CharsetGBK,
						Flag: model.BinaryFlag | model.MultipleKeyFlag |
							model.HandleKeyFlag | model.UniqueKeyFlag,
						Value: "你好",
					}},
					IndexColumns: [][]int{{1, 2}},
				},
				{
					StartTs:  418658114257813516,
					CommitTs: 418658114257813517,
					Table:    &model.TableName{Schema: "common_1", Table: "uk_without_pk"},
					PreColumns: []*model.Column{nil, {
						Name:  "a1",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag | model.UniqueKeyFlag,
						Value: 1,
					}, {
						Name:    "a3",
						Type:    mysql.TypeVarchar,
						Charset: charset.CharsetGBK,
						Flag: model.BinaryFlag | model.MultipleKeyFlag |
							model.HandleKeyFlag | model.UniqueKeyFlag,
						Value: []byte("开发"),
					}},
					Columns: []*model.Column{nil, {
						Name:  "a1",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag | model.UniqueKeyFlag,
						Value: 2,
					}, {
						Name:    "a3",
						Type:    mysql.TypeVarchar,
						Charset: charset.CharsetGBK,
						Flag: model.BinaryFlag | model.MultipleKeyFlag |
							model.HandleKeyFlag | model.UniqueKeyFlag,
						Value: []byte("测试"),
					}},
					IndexColumns: [][]int{{1, 2}},
				},
				{
					StartTs:  418658114257813516,
					CommitTs: 418658114257813517,
					Table:    &model.TableName{Schema: "common_1", Table: "uk_without_pk"},
					PreColumns: []*model.Column{nil, {
						Name:  "a1",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag | model.UniqueKeyFlag,
						Value: 3,
					}, {
						Name:    "a3",
						Type:    mysql.TypeVarchar,
						Charset: charset.CharsetGBK,
						Flag: model.BinaryFlag | model.MultipleKeyFlag |
							model.HandleKeyFlag | model.UniqueKeyFlag,
						Value: []byte("纽约"),
					}},
					Columns: []*model.Column{nil, {
						Name:  "a1",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag | model.UniqueKeyFlag,
						Value: 4,
					}, {
						Name:    "a3",
						Type:    mysql.TypeVarchar,
						Charset: charset.CharsetGBK,
						Flag: model.BinaryFlag | model.MultipleKeyFlag |
							model.HandleKeyFlag | model.UniqueKeyFlag,
						Value: []byte("北京"),
					}},
					IndexColumns: [][]int{{1, 2}},
				},
			},
			expected: &preparedDMLs{
				startTs: []model.Ts{418658114257813514},
				sqls: []string{
					"DELETE FROM `common_1`.`uk_without_pk` WHERE (`a1`,`a3`) IN ((?,?),(?,?))",
					"UPDATE `common_1`.`uk_without_pk` SET `a1`=CASE " +
						"WHEN ROW(`a1`,`a3`)=ROW(?,?) THEN ? WHEN ROW(`a1`,`a3`)=ROW(?,?) " +
						"THEN ? END, `a3`=CASE WHEN ROW(`a1`,`a3`)=ROW(?,?) " +
						"THEN ? WHEN ROW(`a1`,`a3`)=ROW(?,?) THEN ? END WHERE " +
						"ROW(`a1`,`a3`) IN (ROW(?,?),ROW(?,?))",
					"INSERT INTO `common_1`.`uk_without_pk` (`a1`,`a3`) VALUES (?,?)",
				},
				values: [][]interface{}{
					{1, "世界", 2, "你好"},
					{
						1, "开发", 2, 3, "纽约", 4, 1, "开发", "测试", 3,
						"纽约", "北京", 1, "开发", 3, "纽约",
					},
					{2, "你好"},
				},
				rowCount:        5,
				approximateSize: 3,
			},
		},
		// update event and downstream is mysql and without pk
		{
			isTiDB: false,
			input: []*model.RowChangedEvent{
				{
					StartTs:  418658114257813516,
					CommitTs: 418658114257813517,
					Table:    &model.TableName{Schema: "common_1", Table: "uk_without_pk"},
					PreColumns: []*model.Column{nil, {
						Name: "a1",
						Type: mysql.TypeLong,
						Flag: model.BinaryFlag |
							model.MultipleKeyFlag |
							model.HandleKeyFlag |
							model.UniqueKeyFlag,
						Value: 1,
					}, {
						Name:    "a3",
						Type:    mysql.TypeVarchar,
						Charset: charset.CharsetGBK,
						Flag: model.BinaryFlag | model.MultipleKeyFlag |
							model.HandleKeyFlag | model.UniqueKeyFlag,
						Value: []byte("开发"),
					}},
					Columns: []*model.Column{nil, {
						Name: "a1",
						Type: mysql.TypeLong,
						Flag: model.BinaryFlag |
							model.MultipleKeyFlag |
							model.HandleKeyFlag |
							model.UniqueKeyFlag,
						Value: 2,
					}, {
						Name:    "a3",
						Type:    mysql.TypeVarchar,
						Charset: charset.CharsetGBK,
						Flag: model.BinaryFlag | model.MultipleKeyFlag |
							model.HandleKeyFlag | model.UniqueKeyFlag,
						Value: []byte("测试"),
					}},
					IndexColumns: [][]int{{1, 2}},
				},
				{
					StartTs:  418658114257813516,
					CommitTs: 418658114257813517,
					Table:    &model.TableName{Schema: "common_1", Table: "uk_without_pk"},
					PreColumns: []*model.Column{nil, {
						Name: "a1",
						Type: mysql.TypeLong,
						Flag: model.BinaryFlag |
							model.MultipleKeyFlag |
							model.HandleKeyFlag |
							model.UniqueKeyFlag,
						Value: 3,
					}, {
						Name:    "a3",
						Type:    mysql.TypeVarchar,
						Charset: charset.CharsetGBK,
						Flag: model.BinaryFlag | model.MultipleKeyFlag |
							model.HandleKeyFlag | model.UniqueKeyFlag,
						Value: []byte("纽约"),
					}},
					Columns: []*model.Column{nil, {
						Name: "a1",
						Type: mysql.TypeLong,
						Flag: model.BinaryFlag |
							model.MultipleKeyFlag |
							model.HandleKeyFlag | model.UniqueKeyFlag,
						Value: 4,
					}, {
						Name:    "a3",
						Type:    mysql.TypeVarchar,
						Charset: charset.CharsetGBK,
						Flag: model.BinaryFlag | model.MultipleKeyFlag |
							model.HandleKeyFlag | model.UniqueKeyFlag,
						Value: []byte("北京"),
					}},
					IndexColumns: [][]int{{1, 2}},
				},
			},
			expected: &preparedDMLs{
				startTs: []model.Ts{418658114257813516},
				sqls: []string{
					"UPDATE `common_1`.`uk_without_pk` SET `a1` = ?, " +
						"`a3` = ? WHERE `a1` = ? AND `a3` = ? LIMIT 1",
					"UPDATE `common_1`.`uk_without_pk` SET `a1` = ?, " +
						"`a3` = ? WHERE `a1` = ? AND `a3` = ? LIMIT 1",
				},
				values:          [][]interface{}{{2, "测试", 1, "开发"}, {4, "北京", 3, "纽约"}},
				rowCount:        2,
				approximateSize: 2,
			},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ms := newMySQLBackendWithoutDB(ctx)
	ms.cfg.SafeMode = false
	ms.cfg.EnableOldValue = true
	ms.cfg.BatchDMLEnable = true
	for _, tc := range testCases {
		ms.cfg.IsTiDB = tc.isTiDB
		ms.events = make([]*eventsink.TxnCallbackableEvent, 1)
		ms.events[0] = &eventsink.TxnCallbackableEvent{
			Event: &model.SingleTableTxn{Rows: tc.input},
		}
		ms.rows = len(tc.input)
		dmls := ms.prepareDMLs()
		require.Equal(t, tc.expected, dmls)
	}
}

func TestGroupRowsByType(t *testing.T) {
	ctx := context.Background()
	ms := newMySQLBackendWithoutDB(ctx)
	testCases := []struct {
		name      string
		input     []*model.RowChangedEvent
		maxTxnRow int
	}{
		{
			name: "delete",
			input: []*model.RowChangedEvent{
				{
					StartTs:  418658114257813514,
					CommitTs: 418658114257813515,
					Table:    &model.TableName{Schema: "common_1", Table: "uk_without_pk"},
					PreColumns: []*model.Column{nil, {
						Name: "a1",
						Type: mysql.TypeLong,
						Flag: model.BinaryFlag | model.MultipleKeyFlag |
							model.HandleKeyFlag | model.UniqueKeyFlag,
						Value: 1,
					}, {
						Name: "a3",
						Type: mysql.TypeLong,
						Flag: model.BinaryFlag | model.MultipleKeyFlag |
							model.HandleKeyFlag | model.UniqueKeyFlag,
						Value: 1,
					}},
					IndexColumns: [][]int{{1, 2}},
				},
				{
					StartTs:  418658114257813514,
					CommitTs: 418658114257813515,
					Table:    &model.TableName{Schema: "common_1", Table: "uk_without_pk"},
					PreColumns: []*model.Column{nil, {
						Name: "a1",
						Type: mysql.TypeLong,
						Flag: model.BinaryFlag | model.MultipleKeyFlag |
							model.HandleKeyFlag | model.UniqueKeyFlag,
						Value: 2,
					}, {
						Name: "a3",
						Type: mysql.TypeLong,
						Flag: model.BinaryFlag | model.MultipleKeyFlag |
							model.HandleKeyFlag | model.UniqueKeyFlag,
						Value: 2,
					}},
					IndexColumns: [][]int{{1, 2}},
				},
				{
					StartTs:  418658114257813514,
					CommitTs: 418658114257813515,
					Table:    &model.TableName{Schema: "common_1", Table: "uk_without_pk"},
					PreColumns: []*model.Column{nil, {
						Name: "a1",
						Type: mysql.TypeLong,
						Flag: model.BinaryFlag | model.MultipleKeyFlag |
							model.HandleKeyFlag | model.UniqueKeyFlag,
						Value: 2,
					}, {
						Name: "a3",
						Type: mysql.TypeLong,
						Flag: model.BinaryFlag | model.MultipleKeyFlag |
							model.HandleKeyFlag | model.UniqueKeyFlag,
						Value: 2,
					}},
					IndexColumns: [][]int{{1, 2}},
				},
				{
					StartTs:  418658114257813514,
					CommitTs: 418658114257813515,
					Table:    &model.TableName{Schema: "common_1", Table: "uk_without_pk"},
					PreColumns: []*model.Column{nil, {
						Name: "a1",
						Type: mysql.TypeLong,
						Flag: model.BinaryFlag | model.MultipleKeyFlag |
							model.HandleKeyFlag | model.UniqueKeyFlag,
						Value: 2,
					}, {
						Name: "a3",
						Type: mysql.TypeLong,
						Flag: model.BinaryFlag | model.MultipleKeyFlag |
							model.HandleKeyFlag | model.UniqueKeyFlag,
						Value: 2,
					}},
					IndexColumns: [][]int{{1, 2}},
				},
			},
			maxTxnRow: 2,
		},
		{
			name: "insert",
			input: []*model.RowChangedEvent{
				{
					StartTs:  418658114257813516,
					CommitTs: 418658114257813517,
					Table:    &model.TableName{Schema: "common_1", Table: "uk_without_pk"},
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
					IndexColumns: [][]int{{1, 1}},
				},
				{
					StartTs:  418658114257813516,
					CommitTs: 418658114257813517,
					Table:    &model.TableName{Schema: "common_1", Table: "uk_without_pk"},
					Columns: []*model.Column{nil, {
						Name: "a1",
						Type: mysql.TypeLong,
						Flag: model.BinaryFlag | model.MultipleKeyFlag |
							model.HandleKeyFlag | model.HandleKeyFlag,
						Value: 2,
					}, {
						Name: "a3",
						Type: mysql.TypeLong,
						Flag: model.BinaryFlag | model.MultipleKeyFlag |
							model.HandleKeyFlag | model.HandleKeyFlag,
						Value: 2,
					}},
					IndexColumns: [][]int{{2, 2}},
				},
				{
					StartTs:  418658114257813516,
					CommitTs: 418658114257813517,
					Table:    &model.TableName{Schema: "common_1", Table: "uk_without_pk"},
					Columns: []*model.Column{nil, {
						Name: "a1",
						Type: mysql.TypeLong,
						Flag: model.BinaryFlag | model.MultipleKeyFlag |
							model.HandleKeyFlag | model.HandleKeyFlag,
						Value: 2,
					}, {
						Name: "a3",
						Type: mysql.TypeLong,
						Flag: model.BinaryFlag | model.MultipleKeyFlag |
							model.HandleKeyFlag | model.HandleKeyFlag,
						Value: 2,
					}},
					IndexColumns: [][]int{{2, 2}},
				},
				{
					StartTs:  418658114257813516,
					CommitTs: 418658114257813517,
					Table:    &model.TableName{Schema: "common_1", Table: "uk_without_pk"},
					Columns: []*model.Column{nil, {
						Name: "a1",
						Type: mysql.TypeLong,
						Flag: model.BinaryFlag | model.MultipleKeyFlag |
							model.HandleKeyFlag | model.HandleKeyFlag,
						Value: 2,
					}, {
						Name: "a3",
						Type: mysql.TypeLong,
						Flag: model.BinaryFlag | model.MultipleKeyFlag |
							model.HandleKeyFlag | model.HandleKeyFlag,
						Value: 2,
					}},
					IndexColumns: [][]int{{2, 2}},
				},

				{
					StartTs:  418658114257813516,
					CommitTs: 418658114257813517,
					Table:    &model.TableName{Schema: "common_1", Table: "uk_without_pk"},
					Columns: []*model.Column{nil, {
						Name: "a1",
						Type: mysql.TypeLong,
						Flag: model.BinaryFlag | model.MultipleKeyFlag |
							model.HandleKeyFlag | model.HandleKeyFlag,
						Value: 2,
					}, {
						Name: "a3",
						Type: mysql.TypeLong,
						Flag: model.BinaryFlag | model.MultipleKeyFlag |
							model.HandleKeyFlag | model.HandleKeyFlag,
						Value: 2,
					}},
					IndexColumns: [][]int{{2, 2}},
				},

				{
					StartTs:  418658114257813516,
					CommitTs: 418658114257813517,
					Table:    &model.TableName{Schema: "common_1", Table: "uk_without_pk"},
					Columns: []*model.Column{nil, {
						Name: "a1",
						Type: mysql.TypeLong,
						Flag: model.BinaryFlag | model.MultipleKeyFlag |
							model.HandleKeyFlag | model.HandleKeyFlag,
						Value: 2,
					}, {
						Name: "a3",
						Type: mysql.TypeLong,
						Flag: model.BinaryFlag | model.MultipleKeyFlag |
							model.HandleKeyFlag | model.HandleKeyFlag,
						Value: 2,
					}},
					IndexColumns: [][]int{{2, 2}},
				},
			},
			maxTxnRow: 4,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			event := &eventsink.TxnCallbackableEvent{
				Event: &model.SingleTableTxn{Rows: testCases[0].input},
			}
			colums := tc.input[0].Columns
			if len(colums) == 0 {
				colums = tc.input[0].PreColumns
			}
			tableInfo := model.BuildTiDBTableInfo(colums, tc.input[0].IndexColumns)
			ms.cfg.MaxTxnRow = tc.maxTxnRow
			inserts, updates, deletes := ms.groupRowsByType(event, tableInfo, false)
			for _, rows := range inserts {
				require.LessOrEqual(t, len(rows), tc.maxTxnRow)
			}
			for _, rows := range updates {
				require.LessOrEqual(t, len(rows), tc.maxTxnRow)
			}
			for _, rows := range deletes {
				require.LessOrEqual(t, len(rows), tc.maxTxnRow)
			}
		})
	}
}

func TestBackendGenUpdateSQL(t *testing.T) {
	ctx := context.Background()
	ms := newMySQLBackendWithoutDB(ctx)
	table := &model.TableName{Schema: "db", Table: "tb1"}

	createSQL := "CREATE TABLE tb1 (id INT PRIMARY KEY, name varchar(20))"
	stmt, err := parser.New().ParseOneStmt(createSQL, "", "")
	require.NoError(t, err)
	ti, err := ddl.BuildTableInfoFromAST(stmt.(*ast.CreateTableStmt))
	require.NoError(t, err)

	row1 := sqlmodel.NewRowChange(table, table, []any{1, "a"}, []any{1, "aa"}, ti, ti, nil)
	row1.SetApproximateDataSize(6)
	row2 := sqlmodel.NewRowChange(table, table, []any{2, "b"}, []any{2, "bb"}, ti, ti, nil)
	row2.SetApproximateDataSize(6)

	testCases := []struct {
		rows                  []*sqlmodel.RowChange
		maxMultiUpdateRowSize int
		expectedSQLs          []string
		expectedValues        [][]interface{}
	}{
		{
			[]*sqlmodel.RowChange{row1, row2},
			ms.cfg.MaxMultiUpdateRowCount,
			[]string{
				"UPDATE `db`.`tb1` SET " +
					"`id`=CASE WHEN `id`=? THEN ? WHEN `id`=? THEN ? END, " +
					"`name`=CASE WHEN `id`=? THEN ? WHEN `id`=? THEN ? END " +
					"WHERE `id` IN (?,?)",
			},
			[][]interface{}{
				{1, 1, 2, 2, 1, "aa", 2, "bb", 1, 2},
			},
		},
		{
			[]*sqlmodel.RowChange{row1, row2},
			0,
			[]string{
				"UPDATE `db`.`tb1` SET `id` = ?, `name` = ? WHERE `id` = ? LIMIT 1",
				"UPDATE `db`.`tb1` SET `id` = ?, `name` = ? WHERE `id` = ? LIMIT 1",
			},
			[][]interface{}{
				{1, "aa", 1},
				{2, "bb", 2},
			},
		},
	}
	for _, tc := range testCases {
		ms.cfg.MaxMultiUpdateRowSize = tc.maxMultiUpdateRowSize
		sqls, values := ms.genUpdateSQL(tc.rows...)
		require.Equal(t, tc.expectedSQLs, sqls)
		require.Equal(t, tc.expectedValues, values)
	}
}
