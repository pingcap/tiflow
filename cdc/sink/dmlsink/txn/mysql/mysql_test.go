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
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/metabuild"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink"
	"github.com/pingcap/tiflow/cdc/sink/metrics"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/sink"
	pmysql "github.com/pingcap/tiflow/pkg/sink/mysql"
	"github.com/pingcap/tiflow/pkg/sqlmodel"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func init() {
	serverConfig := config.GetGlobalServerConfig().Clone()
	serverConfig.TZ = "UTC"
	config.StoreGlobalServerConfig(serverConfig)
}

func newMySQLBackendWithoutDB() *mysqlBackend {
	cfg := pmysql.NewConfig()
	cfg.BatchDMLEnable = false
	return &mysqlBackend{
		statistics: metrics.NewStatistics(model.DefaultChangeFeedID("test"), sink.TxnSink),
		cfg:        cfg,
	}
}

func newMySQLBackend(
	ctx context.Context,
	changefeedID model.ChangeFeedID,
	sinkURI *url.URL,
	replicaConfig *config.ReplicaConfig,
	dbConnFactory pmysql.IDBConnectionFactory,
) (*mysqlBackend, error) {
	statistics := metrics.NewStatistics(changefeedID, sink.TxnSink)
	raw := sinkURI.Query()
	raw.Set("batch-dml-enable", "true")
	sinkURI.RawQuery = raw.Encode()

	backends, err := NewMySQLBackends(ctx, changefeedID,
		sinkURI, replicaConfig, dbConnFactory, statistics)
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
	// mock a different possible error for the second query
	mock.ExpectQuery("select tidb_version()").WillReturnError(&dmysql.MySQLError{
		Number:  1044,
		Message: "Access denied for user 'cdc'@'%' to database 'information_schema'",
	})
	require.Nil(t, err)
	return
}

func TestPrepareDML(t *testing.T) {
	t.Parallel()
	tableInfo := model.BuildTableInfo("common_1", "uk_without_pk", []*model.Column{
		nil,
		{
			Name: "a1",
			Type: mysql.TypeLong,
			Flag: model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag | model.UniqueKeyFlag,
		},
		{
			Name: "a3",
			Type: mysql.TypeLong,
			Flag: model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag | model.UniqueKeyFlag,
		},
	}, [][]int{{1, 2}})

	tableInfoVector := model.BuildTableInfo("common_1", "uk_without_pk", []*model.Column{
		nil, {
			Name: "a1",
			Type: mysql.TypeLong,
			Flag: model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag | model.UniqueKeyFlag,
		}, {
			Name: "a3",
			Type: mysql.TypeTiDBVectorFloat32,
		},
	}, [][]int{{1, 2}})

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
		},
		// delete event
		{
			input: []*model.RowChangedEvent{
				{
					StartTs:   418658114257813514,
					CommitTs:  418658114257813515,
					TableInfo: tableInfo,
					PreColumns: model.Columns2ColumnDatas([]*model.Column{
						nil,
						{
							Name:  "a1",
							Value: 1,
						},
						{
							Name:  "a3",
							Value: 1,
						},
					}, tableInfo),
				},
			},
			expected: &preparedDMLs{
				startTs:         []model.Ts{418658114257813514},
				sqls:            []string{"DELETE FROM `common_1`.`uk_without_pk` WHERE `a1` = ? AND `a3` = ? LIMIT 1"},
				values:          [][]interface{}{{1, 1}},
				rowCount:        1,
				approximateSize: 74,
			},
		},
		// insert event.
		{
			input: []*model.RowChangedEvent{
				{
					StartTs:   418658114257813516,
					CommitTs:  418658114257813517,
					TableInfo: tableInfo,
					Columns: model.Columns2ColumnDatas([]*model.Column{
						nil,
						{
							Name:  "a1",
							Value: 2,
						},
						{
							Name:  "a3",
							Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag,
							Value: 2,
						},
					}, tableInfo),
				},
			},
			expected: &preparedDMLs{
				startTs:         []model.Ts{418658114257813516},
				sqls:            []string{"INSERT INTO `common_1`.`uk_without_pk` (`a1`,`a3`) VALUES (?,?)"},
				values:          [][]interface{}{{2, 2}},
				rowCount:        1,
				approximateSize: 63,
			},
		},
		// vector type
		{
			input: []*model.RowChangedEvent{
				{
					StartTs:   418658114257813518,
					CommitTs:  418658114257813519,
					TableInfo: tableInfoVector,
					Columns: model.Columns2ColumnDatas(
						[]*model.Column{
							nil, {
								Name:  "a1",
								Type:  mysql.TypeLong,
								Value: 1,
							}, {
								Name:  "a3",
								Type:  mysql.TypeTiDBVectorFloat32,
								Value: util.Must(types.ParseVectorFloat32("[1.1,-2,3.33,-4.12,-5]")),
							},
						}, tableInfoVector),
				},
			},
			expected: &preparedDMLs{
				startTs:         []model.Ts{418658114257813518},
				sqls:            []string{"INSERT INTO `common_1`.`uk_without_pk` (`a1`,`a3`) VALUES (?,?)"},
				values:          [][]interface{}{{1, "[1.1,-2,3.33,-4.12,-5]"}},
				rowCount:        1,
				approximateSize: 63,
			},
		},
	}

	ms := newMySQLBackendWithoutDB()
	for _, tc := range testCases {
		ms.events = make([]*dmlsink.TxnCallbackableEvent, 1)
		ms.events[0] = &dmlsink.TxnCallbackableEvent{
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

	dbConnFactory := pmysql.NewDBConnectionFactoryForTest()
	dbConnFactory.SetStandardConnectionFactory(func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		db, mock := newTestMockDB(t)
		mock.ExpectClose()
		return db, nil
	})

	changefeed := "test-changefeed"
	sinkURI, err := url.Parse("mysql://127.0.0.1:4000/?time-zone=UTC&worker-count=1" +
		"&cache-prep-stmts=false")
	require.Nil(t, err)
	sink, err := newMySQLBackend(ctx, model.DefaultChangeFeedID(changefeed),
		sinkURI, config.GetDefaultReplicaConfig(), dbConnFactory)
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
	_, err = newMySQLBackend(ctx, model.DefaultChangeFeedID(changefeed), sinkURI,
		config.GetDefaultReplicaConfig(), &pmysql.DBConnectionFactory{})
	require.Equal(t, driver.ErrBadConn, errors.Cause(err))
}

// Test OnTxnEvent and Flush interfaces. Event callbacks should be called correctly after flush.
func TestNewMySQLBackendExecDML(t *testing.T) {
	dbConnFactory := pmysql.NewDBConnectionFactoryForTest()
	dbConnFactory.SetStandardConnectionFactory(func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		db, mock := newTestMockDB(t)
		mock.ExpectBegin()
		mock.ExpectExec("INSERT INTO `s1`.`t1` (`a`,`b`) VALUES (?,?),(?,?)").
			WithArgs(1, "test", 2, "test").
			WillReturnResult(sqlmock.NewResult(2, 2))
		mock.ExpectCommit()
		mock.ExpectClose()
		return db, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changefeed := "test-changefeed"
	// TODO: Need to test txn sink behavior when cache-prep-stmts is true
	// I did some attempts to write tests when cache-prep-stmts is true, but failed.
	// The reason is that I can't find a way to prepare a statement in sqlmock connection,
	// and execute it in another sqlmock connection.
	sinkURI, err := url.Parse(
		"mysql://127.0.0.1:4000/?time-zone=UTC&worker-count=1&cache-prep-stmts=false")
	require.Nil(t, err)
	sink, err := newMySQLBackend(ctx, model.DefaultChangeFeedID(changefeed), sinkURI,
		config.GetDefaultReplicaConfig(), dbConnFactory)
	require.Nil(t, err)

	tableInfo := model.BuildTableInfo("s1", "t1", []*model.Column{
		{
			Name: "a",
			Type: mysql.TypeLong,
			Flag: model.HandleKeyFlag | model.PrimaryKeyFlag,
		},
		{
			Name: "b",
			Type: mysql.TypeVarchar,
			Flag: 0,
		},
	}, [][]int{{0}})
	rows := []*model.RowChangedEvent{
		{
			StartTs:         1,
			CommitTs:        2,
			TableInfo:       tableInfo,
			PhysicalTableID: 1,
			Columns: model.Columns2ColumnDatas([]*model.Column{
				{
					Name:  "a",
					Value: 1,
				},
				{
					Name:  "b",
					Value: "test",
				},
			}, tableInfo),
		},
		{
			StartTs:         5,
			CommitTs:        6,
			TableInfo:       tableInfo,
			PhysicalTableID: 1,
			Columns: model.Columns2ColumnDatas([]*model.Column{
				{
					Name:  "a",
					Value: 2,
				},
				{
					Name:  "b",
					Value: "test",
				},
			}, tableInfo),
		},
	}

	var flushedTs uint64 = 0
	_ = sink.OnTxnEvent(&dmlsink.TxnCallbackableEvent{
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
	tableInfo := model.BuildTableInfo("s1", "t1", []*model.Column{
		{
			Name: "a",
			Type: mysql.TypeLong,
			Flag: model.HandleKeyFlag | model.PrimaryKeyFlag,
		},
	}, [][]int{{0}})
	rows := []*model.RowChangedEvent{
		{
			TableInfo:       tableInfo,
			PhysicalTableID: 1,
			Columns: model.Columns2ColumnDatas([]*model.Column{
				{
					Name:  "a",
					Value: 1,
				},
			}, tableInfo),
		},
		{
			TableInfo:       tableInfo,
			PhysicalTableID: 1,
			Columns: model.Columns2ColumnDatas([]*model.Column{
				{
					Name:  "a",
					Value: 2,
				},
			}, tableInfo),
		},
	}

	errDatabaseNotExists := &dmysql.MySQLError{
		Number: uint16(infoschema.ErrDatabaseNotExists.Code()),
	}

	dbConnFactory := pmysql.NewDBConnectionFactoryForTest()
	dbConnFactory.SetStandardConnectionFactory(func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		db, mock := newTestMockDB(t)
		mock.ExpectBegin()
		mock.ExpectExec("REPLACE INTO `s1`.`t1` (`a`) VALUES (?),(?)").
			WithArgs(1, 2).
			WillReturnError(errDatabaseNotExists)
		mock.ExpectRollback()
		mock.ExpectClose()
		return db, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changefeed := "test-changefeed"
	sinkURI, err := url.Parse(
		"mysql://127.0.0.1:4000/?time-zone=UTC&worker-count=1&cache-prep-stmts=false")
	require.Nil(t, err)
	sink, err := newMySQLBackend(ctx, model.DefaultChangeFeedID(changefeed), sinkURI,
		config.GetDefaultReplicaConfig(), dbConnFactory)
	require.Nil(t, err)

	_ = sink.OnTxnEvent(&dmlsink.TxnCallbackableEvent{
		Event: &model.SingleTableTxn{Rows: rows},
	})
	err = sink.Flush(context.Background())
	require.Equal(t, errDatabaseNotExists, errors.Cause(err))

	require.Nil(t, sink.Close())
}

func TestExecDMLRollbackErrTableNotExists(t *testing.T) {
	tableInfo := model.BuildTableInfo("s1", "t1", []*model.Column{
		{
			Name:  "a",
			Type:  mysql.TypeLong,
			Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
			Value: 1,
		},
	}, [][]int{{0}})
	rows := []*model.RowChangedEvent{
		{
			TableInfo:       tableInfo,
			PhysicalTableID: 1,
			Columns: model.Columns2ColumnDatas([]*model.Column{
				{
					Name:  "a",
					Value: 1,
				},
			}, tableInfo),
		},
		{
			TableInfo:       tableInfo,
			PhysicalTableID: 1,
			Columns: model.Columns2ColumnDatas([]*model.Column{
				{
					Name:  "a",
					Value: 2,
				},
			}, tableInfo),
		},
	}

	errTableNotExists := &dmysql.MySQLError{
		Number: uint16(infoschema.ErrTableNotExists.Code()),
	}

	dbConnFactory := pmysql.NewDBConnectionFactoryForTest()
	dbConnFactory.SetStandardConnectionFactory(func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		db, mock := newTestMockDB(t)
		mock.ExpectBegin()
		mock.ExpectExec("REPLACE INTO `s1`.`t1` (`a`) VALUES (?),(?)").
			WithArgs(1, 2).
			WillReturnError(errTableNotExists)
		mock.ExpectRollback()
		mock.ExpectClose()
		return db, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changefeed := "test-changefeed"
	sinkURI, err := url.Parse(
		"mysql://127.0.0.1:4000/?time-zone=UTC&worker-count=1&cache-prep-stmts=false")
	require.Nil(t, err)
	sink, err := newMySQLBackend(ctx, model.DefaultChangeFeedID(changefeed), sinkURI,
		config.GetDefaultReplicaConfig(), dbConnFactory)
	require.Nil(t, err)

	_ = sink.OnTxnEvent(&dmlsink.TxnCallbackableEvent{
		Event: &model.SingleTableTxn{Rows: rows},
	})
	err = sink.Flush(context.Background())
	require.Equal(t, errTableNotExists, errors.Cause(err))

	require.Nil(t, sink.Close())
}

func TestExecDMLRollbackErrRetryable(t *testing.T) {
	tableInfo := model.BuildTableInfo("s1", "t1", []*model.Column{
		{
			Name:  "a",
			Type:  mysql.TypeLong,
			Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
			Value: 1,
		},
	}, [][]int{{0}})
	rows := []*model.RowChangedEvent{
		{
			TableInfo:       tableInfo,
			PhysicalTableID: 1,
			Columns: model.Columns2ColumnDatas([]*model.Column{
				{
					Name:  "a",
					Value: 1,
				},
			}, tableInfo),
		},
		{
			TableInfo:       tableInfo,
			PhysicalTableID: 1,
			Columns: model.Columns2ColumnDatas([]*model.Column{
				{
					Name:  "a",
					Value: 2,
				},
			}, tableInfo),
		},
	}

	errLockDeadlock := &dmysql.MySQLError{
		Number: mysql.ErrLockDeadlock,
	}

	dbConnFactory := pmysql.NewDBConnectionFactoryForTest()
	dbConnFactory.SetStandardConnectionFactory(func(ctx context.Context, dsnStr string) (*sql.DB, error) {
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
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changefeed := "test-changefeed"
	sinkURI, err := url.Parse(
		"mysql://127.0.0.1:4000/?time-zone=UTC&worker-count=1&cache-prep-stmts=false")
	require.Nil(t, err)
	sink, err := newMySQLBackend(ctx, model.DefaultChangeFeedID(changefeed), sinkURI,
		config.GetDefaultReplicaConfig(), dbConnFactory)
	require.Nil(t, err)
	sink.setDMLMaxRetry(2)

	_ = sink.OnTxnEvent(&dmlsink.TxnCallbackableEvent{
		Event: &model.SingleTableTxn{Rows: rows},
	})
	err = sink.Flush(context.Background())
	require.Equal(t, errLockDeadlock, errors.Cause(err))

	require.Nil(t, sink.Close())
}

func TestMysqlSinkNotRetryErrDupEntry(t *testing.T) {
	errDup := mysql.NewErr(mysql.ErrDupEntry)
	tableInfo := model.BuildTableInfo("s1", "t1", []*model.Column{
		{
			Name: "a",
			Type: mysql.TypeLong,
			Flag: model.HandleKeyFlag | model.PrimaryKeyFlag,
		},
	}, [][]int{{0}})
	rows := []*model.RowChangedEvent{
		{
			StartTs:         2,
			CommitTs:        3,
			ReplicatingTs:   1,
			TableInfo:       tableInfo,
			PhysicalTableID: 1,
			Columns: model.Columns2ColumnDatas([]*model.Column{
				{
					Name:  "a",
					Value: 1,
				},
			}, tableInfo),
		},
	}

	dbConnFactory := pmysql.NewDBConnectionFactoryForTest()
	dbConnFactory.SetStandardConnectionFactory(func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		db, mock := newTestMockDB(t)
		mock.ExpectBegin()
		mock.ExpectExec("INSERT INTO `s1`.`t1` (`a`) VALUES (?)").
			WithArgs(1).
			WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectCommit().
			WillReturnError(errDup)
		mock.ExpectClose()
		return db, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changefeed := "test-changefeed"
	sinkURI, err := url.Parse(
		"mysql://127.0.0.1:4000/?time-zone=UTC&worker-count=1&safe-mode=false" +
			"&cache-prep-stmts=false")
	require.Nil(t, err)
	sink, err := newMySQLBackend(ctx, model.DefaultChangeFeedID(changefeed), sinkURI,
		config.GetDefaultReplicaConfig(), dbConnFactory)
	require.Nil(t, err)
	sink.setDMLMaxRetry(1)
	_ = sink.OnTxnEvent(&dmlsink.TxnCallbackableEvent{
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
	dbConnFactory := pmysql.NewDBConnectionFactoryForTest()
	dbConnFactory.SetStandardConnectionFactory(func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		db, mock := newTestMockDB(t)
		mock.ExpectClose()
		return db, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	changefeed := "test-changefeed"
	sinkURI, err := url.Parse("mysql://127.0.0.1:4000/?time-zone=UTC&worker-count=1" +
		"&cache-prep-stmts=false")
	require.Nil(t, err)
	sink, err := newMySQLBackend(ctx, model.DefaultChangeFeedID(changefeed), sinkURI,
		config.GetDefaultReplicaConfig(), dbConnFactory)

	require.Nil(t, err)
	require.Nil(t, sink.Close())
	// Test idempotency of `Close` interface
	require.Nil(t, sink.Close())
}

func TestNewMySQLBackendWithIPv6Address(t *testing.T) {
	dbConnFactory := pmysql.NewDBConnectionFactoryForTest()
	dbConnFactory.SetStandardConnectionFactory(func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		db, mock := newTestMockDB(t)
		mock.ExpectClose()
		return db, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changefeed := "test-changefeed"
	// See https://www.ietf.org/rfc/rfc2732.txt, we have to use brackets to wrap IPv6 address.
	sinkURI, err := url.Parse("mysql://[::1]:3306/?time-zone=UTC&worker-count=1" +
		"&cache-prep-stmts=false")
	require.Nil(t, err)
	sink, err := newMySQLBackend(ctx, model.DefaultChangeFeedID(changefeed), sinkURI,
		config.GetDefaultReplicaConfig(), dbConnFactory)
	require.Nil(t, err)
	require.Nil(t, sink.Close())
}

func TestGBKSupported(t *testing.T) {
	dbConnFactory := pmysql.NewDBConnectionFactoryForTest()
	dbConnFactory.SetStandardConnectionFactory(func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		db, mock := newTestMockDB(t)
		mock.ExpectClose()
		return db, nil
	})

	zapcore, logs := observer.New(zap.WarnLevel)
	conf := &log.Config{Level: "warn", File: log.FileLogConfig{}}
	_, r, _ := log.InitLogger(conf)
	logger := zap.New(zapcore)
	restoreFn := log.ReplaceGlobals(logger, r)
	defer restoreFn()

	ctx := context.Background()
	changefeed := "test-changefeed"
	sinkURI, err := url.Parse("mysql://127.0.0.1:4000/?time-zone=UTC&worker-count=1" +
		"&cache-prep-stmts=false")
	require.Nil(t, err)
	sink, err := newMySQLBackend(ctx, model.DefaultChangeFeedID(changefeed), sinkURI,
		config.GetDefaultReplicaConfig(), dbConnFactory)
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
	dbConnFactory := pmysql.NewDBConnectionFactoryForTest()
	dbConnFactory.SetStandardConnectionFactory(func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		db, mock := newTestMockDB(t)
		mock.ExpectBegin()
		mock.ExpectExec("INSERT INTO `s1`.`t1` (`a`,`b`) VALUES (?,?),(?,?)").WillDelayFor(1 * time.Second).
			WillReturnError(&dmysql.MySQLError{Number: mysql.ErrNoSuchTable})
		mock.ExpectClose()
		return db, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changefeed := "test-changefeed"
	sinkURI, err := url.Parse(
		"mysql://127.0.0.1:4000/?time-zone=UTC&worker-count=1&cache-prep-stmts=false")
	require.Nil(t, err)
	sink, err := newMySQLBackend(ctx, model.DefaultChangeFeedID(changefeed), sinkURI,
		config.GetDefaultReplicaConfig(), dbConnFactory)
	require.Nil(t, err)

	tableInfo := model.BuildTableInfo("s1", "t1", []*model.Column{
		{
			Name: "a",
			Type: mysql.TypeLong,
			Flag: model.HandleKeyFlag | model.PrimaryKeyFlag,
		},
		{
			Name: "b",
			Type: mysql.TypeVarchar,
			Flag: 0,
		},
	}, [][]int{{0}})
	rows := []*model.RowChangedEvent{
		{
			StartTs:         1,
			CommitTs:        2,
			TableInfo:       tableInfo,
			PhysicalTableID: 1,
			Columns: model.Columns2ColumnDatas([]*model.Column{
				{
					Name:  "a",
					Value: 1,
				},
				{
					Name:  "b",
					Value: "test",
				},
			}, tableInfo),
		},
		{
			StartTs:         2,
			CommitTs:        3,
			TableInfo:       tableInfo,
			PhysicalTableID: 1,
			Columns: model.Columns2ColumnDatas([]*model.Column{
				{
					Name:  "a",
					Value: 2,
				},
				{
					Name:  "b",
					Value: "test",
				},
			}, tableInfo),
		},
	}

	_ = sink.OnTxnEvent(&dmlsink.TxnCallbackableEvent{
		Event: &model.SingleTableTxn{Rows: rows},
	})
	err = sink.Flush(context.Background())
	require.Regexp(t, ".*ErrMySQLTxnError.*", err)
	require.Nil(t, sink.Close())
}

func TestMysqlSinkSafeModeOff(t *testing.T) {
	t.Parallel()

	tableInfoWithoutPK := model.BuildTableInfo("common_1", "uk_without_pk", []*model.Column{nil, {
		Name:  "a1",
		Type:  mysql.TypeLong,
		Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag | model.UniqueKeyFlag,
		Value: 1,
	}, {
		Name:  "a3",
		Type:  mysql.TypeLong,
		Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag | model.UniqueKeyFlag,
		Value: 1,
	}}, [][]int{{1, 2}})

	tableInfoWithPK := model.BuildTableInfo("common_1", "pk", []*model.Column{nil, {
		Name: "a1",
		Type: mysql.TypeLong,
		Flag: model.HandleKeyFlag | model.MultipleKeyFlag | model.PrimaryKeyFlag,
	}, {
		Name: "a3",
		Type: mysql.TypeLong,
		Flag: model.BinaryFlag | model.HandleKeyFlag | model.MultipleKeyFlag | model.PrimaryKeyFlag,
	}}, [][]int{{1, 2}})

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
					TableInfo:     tableInfoWithoutPK,
					Columns: model.Columns2ColumnDatas([]*model.Column{nil, {
						Name:  "a1",
						Value: 1,
					}, {
						Name:  "a3",
						Value: 1,
					}}, tableInfoWithoutPK),
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
					TableInfo:     tableInfoWithPK,
					Columns: model.Columns2ColumnDatas([]*model.Column{nil, {
						Name:  "a1",
						Value: 1,
					}, {
						Name:  "a3",
						Value: 1,
					}}, tableInfoWithPK),
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
					TableInfo:     tableInfoWithoutPK,
					PreColumns: model.Columns2ColumnDatas([]*model.Column{nil, {
						Name:  "a1",
						Value: 2,
					}, {
						Name:  "a3",
						Value: 2,
					}}, tableInfoWithoutPK),
					Columns: model.Columns2ColumnDatas([]*model.Column{nil, {
						Name:  "a1",
						Value: 3,
					}, {
						Name:  "a3",
						Value: 3,
					}}, tableInfoWithoutPK),
				},
			},
			expected: &preparedDMLs{
				startTs: []model.Ts{418658114257813516},
				sqls: []string{
					"UPDATE `common_1`.`uk_without_pk` SET `a1` = ?, `a3` = ? " +
						"WHERE `a1` = ? AND `a3` = ? LIMIT 1",
				},
				values:          [][]interface{}{{3, 3, 2, 2}},
				rowCount:        1,
				approximateSize: 92,
			},
		}, {
			name: "update with PK",
			input: []*model.RowChangedEvent{
				{
					StartTs:       418658114257813516,
					CommitTs:      418658114257813517,
					ReplicatingTs: 418658114257813515,
					TableInfo:     tableInfoWithPK,
					PreColumns: model.Columns2ColumnDatas([]*model.Column{nil, {
						Name:  "a1",
						Value: 2,
					}, {
						Name:  "a3",
						Value: 2,
					}}, tableInfoWithPK),
					Columns: model.Columns2ColumnDatas([]*model.Column{nil, {
						Name:  "a1",
						Value: 3,
					}, {
						Name:  "a3",
						Value: 3,
					}}, tableInfoWithPK),
				},
			},
			expected: &preparedDMLs{
				startTs: []model.Ts{418658114257813516},
				sqls: []string{"UPDATE `common_1`.`pk` SET `a1` = ?, `a3` = ? " +
					"WHERE `a1` = ? AND `a3` = ? LIMIT 1"},
				values:          [][]interface{}{{3, 3, 2, 2}},
				rowCount:        1,
				approximateSize: 81,
			},
		}, {
			name: "batch insert with PK",
			input: []*model.RowChangedEvent{
				{
					StartTs:       418658114257813516,
					CommitTs:      418658114257813517,
					ReplicatingTs: 418658114257813515,
					TableInfo:     tableInfoWithPK,
					Columns: model.Columns2ColumnDatas([]*model.Column{nil, {
						Name:  "a1",
						Value: 3,
					}, {
						Name:  "a3",
						Value: 3,
					}}, tableInfoWithPK),
				},
				{
					StartTs:       418658114257813516,
					CommitTs:      418658114257813517,
					ReplicatingTs: 418658114257813515,
					TableInfo:     tableInfoWithPK,
					Columns: model.Columns2ColumnDatas([]*model.Column{nil, {
						Name:  "a1",
						Value: 5,
					}, {
						Name:  "a3",
						Value: 5,
					}}, tableInfoWithPK),
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
					TableInfo:     tableInfoWithPK,
					Columns: model.Columns2ColumnDatas([]*model.Column{nil, {
						Name:  "a1",
						Value: 3,
					}, {
						Name:  "a3",
						Value: 3,
					}}, tableInfoWithPK),
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
					TableInfo:     tableInfoWithPK,
					Columns: model.Columns2ColumnDatas([]*model.Column{nil, {
						Name:  "a1",
						Value: 3,
					}, {
						Name:  "a3",
						Value: 3,
					}}, tableInfoWithPK),
				},
				{
					StartTs:       418658114257813516,
					CommitTs:      418658114257813517,
					ReplicatingTs: 418658114257813518,
					TableInfo:     tableInfoWithPK,
					Columns: model.Columns2ColumnDatas([]*model.Column{nil, {
						Name:  "a1",
						Type:  mysql.TypeLong,
						Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
						Value: 5,
					}, {
						Name:  "a3",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag,
						Value: 5,
					}}, tableInfoWithPK),
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

	ms := newMySQLBackendWithoutDB()
	ms.cfg.SafeMode = false
	for _, tc := range testCases {
		ms.events = make([]*dmlsink.TxnCallbackableEvent, 1)
		ms.events[0] = &dmlsink.TxnCallbackableEvent{
			Event: &model.SingleTableTxn{Rows: tc.input},
		}
		ms.rows = len(tc.input)
		dmls := ms.prepareDMLs()
		require.Equal(t, tc.expected, dmls, tc.name)
	}
}

func TestPrepareBatchDMLs(t *testing.T) {
	t.Parallel()
	tableInfoWithoutPK := model.BuildTableInfo("common_1", "uk_without_pk", []*model.Column{{
		Name: "a1",
		Type: mysql.TypeLong,
		Flag: model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag | model.UniqueKeyFlag,
	}, {
		Name:    "a3",
		Type:    mysql.TypeVarchar,
		Charset: charset.CharsetGBK,
		Flag:    model.MultipleKeyFlag | model.HandleKeyFlag | model.UniqueKeyFlag,
	}}, [][]int{{0, 1}})
	tableInfoWithVector := model.BuildTableInfo("common_1", "uk_without_pk", []*model.Column{{
		Name: "a1",
		Type: mysql.TypeLong,
		Flag: model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag | model.UniqueKeyFlag,
	}, {
		Name: "a3",
		Type: mysql.TypeTiDBVectorFloat32,
	}}, [][]int{{0, 1}})
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
					StartTs:   418658114257813514,
					CommitTs:  418658114257813515,
					TableInfo: tableInfoWithoutPK,
					PreColumns: model.Columns2ColumnDatas([]*model.Column{{
						Name:  "a1",
						Value: 1,
					}, {
						Name:  "a3",
						Value: []byte("你好"),
					}}, tableInfoWithoutPK),
					ApproximateDataSize: 10,
				},
				{
					StartTs:   418658114257813514,
					CommitTs:  418658114257813515,
					TableInfo: tableInfoWithoutPK,
					PreColumns: model.Columns2ColumnDatas([]*model.Column{{
						Name:  "a1",
						Value: 2,
					}, {
						Name:  "a3",
						Value: []byte("世界"),
					}}, tableInfoWithoutPK),
					ApproximateDataSize: 10,
				},
			},
			expected: &preparedDMLs{
				startTs:         []model.Ts{418658114257813514},
				sqls:            []string{"DELETE FROM `common_1`.`uk_without_pk` WHERE (`a1` = ? AND `a3` = ?) OR (`a1` = ? AND `a3` = ?)"},
				values:          [][]interface{}{{1, "你好", 2, "世界"}},
				rowCount:        2,
				approximateSize: 115,
			},
		},
		{ // insert event
			isTiDB: true,
			input: []*model.RowChangedEvent{
				{
					StartTs:   418658114257813516,
					CommitTs:  418658114257813517,
					TableInfo: tableInfoWithoutPK,
					Columns: model.Columns2ColumnDatas([]*model.Column{{
						Name:  "a1",
						Value: 1,
					}, {
						Name:  "a3",
						Value: "你好",
					}}, tableInfoWithoutPK),
					ApproximateDataSize: 10,
				},
				{
					StartTs:   418658114257813516,
					CommitTs:  418658114257813517,
					TableInfo: tableInfoWithoutPK,
					Columns: model.Columns2ColumnDatas([]*model.Column{{
						Name:  "a1",
						Value: 2,
					}, {
						Name:  "a3",
						Value: "世界",
					}}, tableInfoWithoutPK),
					ApproximateDataSize: 10,
				},
			},
			expected: &preparedDMLs{
				startTs:         []model.Ts{418658114257813516},
				sqls:            []string{"INSERT INTO `common_1`.`uk_without_pk` (`a1`,`a3`) VALUES (?,?),(?,?)"},
				values:          [][]interface{}{{1, "你好", 2, "世界"}},
				rowCount:        2,
				approximateSize: 89,
			},
		},
		// update event
		{
			isTiDB: true,
			input: []*model.RowChangedEvent{
				{
					StartTs:   418658114257813516,
					CommitTs:  418658114257813517,
					TableInfo: tableInfoWithoutPK,
					PreColumns: model.Columns2ColumnDatas([]*model.Column{{
						Name:  "a1",
						Value: 1,
					}, {
						Name:  "a3",
						Value: []byte("开发"),
					}}, tableInfoWithoutPK),
					Columns: model.Columns2ColumnDatas([]*model.Column{{
						Name:  "a1",
						Value: 2,
					}, {
						Name:  "a3",
						Value: []byte("测试"),
					}}, tableInfoWithoutPK),
					ApproximateDataSize: 12,
				},
				{
					StartTs:   418658114257813516,
					CommitTs:  418658114257813517,
					TableInfo: tableInfoWithoutPK,
					PreColumns: model.Columns2ColumnDatas([]*model.Column{{
						Name:  "a1",
						Value: 3,
					}, {
						Name:  "a3",
						Value: []byte("纽约"),
					}}, tableInfoWithoutPK),
					Columns: model.Columns2ColumnDatas([]*model.Column{{
						Name:  "a1",
						Value: 4,
					}, {
						Name:  "a3",
						Value: []byte("北京"),
					}}, tableInfoWithoutPK),
					ApproximateDataSize: 12,
				},
			},
			expected: &preparedDMLs{
				startTs: []model.Ts{418658114257813516},
				sqls: []string{"UPDATE `common_1`.`uk_without_pk` " +
					"SET `a1`=CASE WHEN `a1` = ? AND `a3` = ? THEN ? WHEN `a1` = ? AND `a3` = ? THEN ? END, " +
					"`a3`=CASE WHEN `a1` = ? AND `a3` = ? THEN ? WHEN `a1` = ? AND `a3` = ? THEN ? END " +
					"WHERE (`a1` = ? AND `a3` = ?) OR (`a1` = ? AND `a3` = ?)"},
				values: [][]interface{}{{
					1, "开发", 2, 3, "纽约", 4, 1, "开发", "测试", 3,
					"纽约", "北京", 1, "开发", 3, "纽约",
				}},
				rowCount:        2,
				approximateSize: 283,
			},
		},
		// mixed event, and test delete， update, insert are ordered
		{
			isTiDB: true,
			input: []*model.RowChangedEvent{
				{
					StartTs:   418658114257813514,
					CommitTs:  418658114257813515,
					TableInfo: tableInfoWithoutPK,
					Columns: model.Columns2ColumnDatas([]*model.Column{{
						Name:  "a1",
						Value: 2,
					}, {
						Name:  "a3",
						Value: []byte("你好"),
					}}, tableInfoWithoutPK),
					ApproximateDataSize: 10,
				},
				{
					StartTs:   418658114257813514,
					CommitTs:  418658114257813515,
					TableInfo: tableInfoWithoutPK,
					PreColumns: model.Columns2ColumnDatas([]*model.Column{{
						Name:  "a1",
						Value: 1,
					}, {
						Name:  "a3",
						Value: []byte("世界"),
					}}, tableInfoWithoutPK),
					ApproximateDataSize: 10,
				},
				{
					StartTs:   418658114257813514,
					CommitTs:  418658114257813515,
					TableInfo: tableInfoWithoutPK,
					PreColumns: model.Columns2ColumnDatas([]*model.Column{{
						Name:  "a1",
						Value: 2,
					}, {
						Name:  "a3",
						Value: "你好",
					}}, tableInfoWithoutPK),
					ApproximateDataSize: 10,
				},
				{
					StartTs:   418658114257813516,
					CommitTs:  418658114257813517,
					TableInfo: tableInfoWithoutPK,
					PreColumns: model.Columns2ColumnDatas([]*model.Column{{
						Name:  "a1",
						Value: 1,
					}, {
						Name:  "a3",
						Value: []byte("开发"),
					}}, tableInfoWithoutPK),
					Columns: model.Columns2ColumnDatas([]*model.Column{{
						Name:  "a1",
						Value: 2,
					}, {
						Name:  "a3",
						Value: []byte("测试"),
					}}, tableInfoWithoutPK),
					ApproximateDataSize: 10,
				},
				{
					StartTs:   418658114257813516,
					CommitTs:  418658114257813517,
					TableInfo: tableInfoWithoutPK,
					PreColumns: model.Columns2ColumnDatas([]*model.Column{{
						Name:  "a1",
						Value: 3,
					}, {
						Name:  "a3",
						Value: []byte("纽约"),
					}}, tableInfoWithoutPK),
					Columns: model.Columns2ColumnDatas([]*model.Column{{
						Name:  "a1",
						Value: 4,
					}, {
						Name:  "a3",
						Value: []byte("北京"),
					}}, tableInfoWithoutPK),
					ApproximateDataSize: 10,
				},
			},
			expected: &preparedDMLs{
				startTs: []model.Ts{418658114257813514},
				sqls: []string{
					"DELETE FROM `common_1`.`uk_without_pk` WHERE (`a1` = ? AND `a3` = ?) OR (`a1` = ? AND `a3` = ?)",
					"UPDATE `common_1`.`uk_without_pk` " +
						"SET `a1`=CASE WHEN `a1` = ? AND `a3` = ? THEN ? WHEN `a1` = ? AND `a3` = ? THEN ? END, " +
						"`a3`=CASE WHEN `a1` = ? AND `a3` = ? THEN ? WHEN `a1` = ? AND `a3` = ? THEN ? END " +
						"WHERE (`a1` = ? AND `a3` = ?) OR (`a1` = ? AND `a3` = ?)",
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
				approximateSize: 467,
			},
		},
		// update event and downstream is mysql and without pk
		{
			isTiDB: false,
			input: []*model.RowChangedEvent{
				{
					StartTs:   418658114257813516,
					CommitTs:  418658114257813517,
					TableInfo: tableInfoWithoutPK,
					PreColumns: model.Columns2ColumnDatas([]*model.Column{{
						Name:  "a1",
						Value: 1,
					}, {
						Name:  "a3",
						Value: []byte("开发"),
					}}, tableInfoWithoutPK),
					Columns: model.Columns2ColumnDatas([]*model.Column{{
						Name:  "a1",
						Value: 2,
					}, {
						Name:  "a3",
						Value: []byte("测试"),
					}}, tableInfoWithoutPK),
					ApproximateDataSize: 10,
				},
				{
					StartTs:   418658114257813516,
					CommitTs:  418658114257813517,
					TableInfo: tableInfoWithoutPK,
					PreColumns: model.Columns2ColumnDatas([]*model.Column{{
						Name:  "a1",
						Value: 3,
					}, {
						Name:  "a3",
						Value: []byte("纽约"),
					}}, tableInfoWithoutPK),
					Columns: model.Columns2ColumnDatas([]*model.Column{{
						Name:  "a1",
						Value: 4,
					}, {
						Name:  "a3",
						Value: []byte("北京"),
					}}, tableInfoWithoutPK),
					ApproximateDataSize: 10,
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
				approximateSize: 204,
			},
		},

		// inser vector data
		{
			isTiDB: true,
			input: []*model.RowChangedEvent{
				{
					StartTs:   418658114257813516,
					CommitTs:  418658114257813517,
					TableInfo: tableInfoWithVector,
					Columns: model.Columns2ColumnDatas([]*model.Column{{
						Name:  "a1",
						Value: 1,
					}, {
						Name:  "a3",
						Value: util.Must(types.ParseVectorFloat32("[1,2,3,4,5]")),
					}}, tableInfoWithVector),
					ApproximateDataSize: 10,
				},
			},
			expected: &preparedDMLs{
				startTs: []model.Ts{418658114257813516},
				sqls: []string{
					"INSERT INTO `common_1`.`uk_without_pk` (`a1`,`a3`) VALUES (?,?)",
				},
				values: [][]interface{}{
					{1, "[1,2,3,4,5]"},
				},
				rowCount:        1,
				approximateSize: 73,
			},
		},
	}

	ms := newMySQLBackendWithoutDB()
	ms.cfg.BatchDMLEnable = true
	ms.cfg.SafeMode = false
	for _, tc := range testCases {
		ms.cfg.IsTiDB = tc.isTiDB
		ms.events = make([]*dmlsink.TxnCallbackableEvent, 1)
		ms.events[0] = &dmlsink.TxnCallbackableEvent{
			Event: &model.SingleTableTxn{Rows: tc.input},
		}
		ms.rows = len(tc.input)
		dmls := ms.prepareDMLs()
		require.Equal(t, tc.expected, dmls)
	}
}

func TestGroupRowsByType(t *testing.T) {
	ms := newMySQLBackendWithoutDB()
	tableInfoWithoutPK := model.BuildTableInfo("common_1", "uk_without_pk", []*model.Column{{
		Name: "a1",
		Type: mysql.TypeLong,
		Flag: model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag | model.UniqueKeyFlag,
	}, {
		Name: "a3",
		Type: mysql.TypeLong,
		Flag: model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag | model.UniqueKeyFlag,
	}}, [][]int{{0, 1}})
	testCases := []struct {
		name      string
		input     []*model.RowChangedEvent
		maxTxnRow int
	}{
		{
			name: "delete",
			input: []*model.RowChangedEvent{
				{
					StartTs:   418658114257813514,
					CommitTs:  418658114257813515,
					TableInfo: tableInfoWithoutPK,
					PreColumns: model.Columns2ColumnDatas([]*model.Column{{
						Name:  "a1",
						Value: 1,
					}, {
						Name:  "a3",
						Value: 1,
					}}, tableInfoWithoutPK),
				},
				{
					StartTs:   418658114257813514,
					CommitTs:  418658114257813515,
					TableInfo: tableInfoWithoutPK,
					PreColumns: model.Columns2ColumnDatas([]*model.Column{{
						Name:  "a1",
						Value: 2,
					}, {
						Name:  "a3",
						Value: 2,
					}}, tableInfoWithoutPK),
				},
				{
					StartTs:   418658114257813514,
					CommitTs:  418658114257813515,
					TableInfo: tableInfoWithoutPK,
					PreColumns: model.Columns2ColumnDatas([]*model.Column{{
						Name:  "a1",
						Value: 2,
					}, {
						Name:  "a3",
						Value: 2,
					}}, tableInfoWithoutPK),
				},
				{
					StartTs:   418658114257813514,
					CommitTs:  418658114257813515,
					TableInfo: tableInfoWithoutPK,
					PreColumns: model.Columns2ColumnDatas([]*model.Column{{
						Name:  "a1",
						Value: 2,
					}, {
						Name:  "a3",
						Value: 2,
					}}, tableInfoWithoutPK),
				},
			},
			maxTxnRow: 2,
		},
		{
			name: "insert",
			input: []*model.RowChangedEvent{
				{
					StartTs:   418658114257813516,
					CommitTs:  418658114257813517,
					TableInfo: tableInfoWithoutPK,
					Columns: model.Columns2ColumnDatas([]*model.Column{{
						Name:  "a1",
						Value: 1,
					}, {
						Name:  "a3",
						Value: 1,
					}}, tableInfoWithoutPK),
				},
				{
					StartTs:   418658114257813516,
					CommitTs:  418658114257813517,
					TableInfo: tableInfoWithoutPK,
					Columns: model.Columns2ColumnDatas([]*model.Column{{
						Name:  "a1",
						Value: 2,
					}, {
						Name:  "a3",
						Value: 2,
					}}, tableInfoWithoutPK),
				},
				{
					StartTs:   418658114257813516,
					CommitTs:  418658114257813517,
					TableInfo: tableInfoWithoutPK,
					Columns: model.Columns2ColumnDatas([]*model.Column{{
						Name:  "a1",
						Value: 2,
					}, {
						Name:  "a3",
						Value: 2,
					}}, tableInfoWithoutPK),
				},
				{
					StartTs:   418658114257813516,
					CommitTs:  418658114257813517,
					TableInfo: tableInfoWithoutPK,
					Columns: model.Columns2ColumnDatas([]*model.Column{{
						Name:  "a1",
						Value: 2,
					}, {
						Name:  "a3",
						Value: 2,
					}}, tableInfoWithoutPK),
				},

				{
					StartTs:   418658114257813516,
					CommitTs:  418658114257813517,
					TableInfo: tableInfoWithoutPK,
					Columns: model.Columns2ColumnDatas([]*model.Column{{
						Name:  "a1",
						Value: 2,
					}, {
						Name:  "a3",
						Value: 2,
					}}, tableInfoWithoutPK),
				},

				{
					StartTs:   418658114257813516,
					CommitTs:  418658114257813517,
					TableInfo: tableInfoWithoutPK,
					Columns: model.Columns2ColumnDatas([]*model.Column{{
						Name:  "a1",
						Value: 2,
					}, {
						Name:  "a3",
						Value: 2,
					}}, tableInfoWithoutPK),
				},
			},
			maxTxnRow: 4,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			event := &dmlsink.TxnCallbackableEvent{
				Event: &model.SingleTableTxn{
					TableInfo: testCases[0].input[0].TableInfo,
					Rows:      testCases[0].input,
				},
			}
			ms.cfg.MaxTxnRow = tc.maxTxnRow
			inserts, updates, deletes := ms.groupRowsByType(event, event.Event.TableInfo)
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
	ms := newMySQLBackendWithoutDB()
	table := &model.TableName{Schema: "db", Table: "tb1"}

	createSQL := "CREATE TABLE tb1 (id INT PRIMARY KEY, name varchar(20))"
	stmt, err := parser.New().ParseOneStmt(createSQL, "", "")
	require.NoError(t, err)
	ti, err := ddl.BuildTableInfoFromAST(metabuild.NewContext(), stmt.(*ast.CreateTableStmt))
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
					"`id`=CASE WHEN `id` = ? THEN ? WHEN `id` = ? THEN ? END, " +
					"`name`=CASE WHEN `id` = ? THEN ? WHEN `id` = ? THEN ? END " +
					"WHERE (`id` = ?) OR (`id` = ?)",
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
