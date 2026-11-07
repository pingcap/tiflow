// Copyright 2019 PingCAP, Inc.
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
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/retry"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/dm/syncer/dbconn"
	"github.com/pingcap/tiflow/dm/syncer/metrics"
	"github.com/stretchr/testify/require"
)

func newMysqlErr(number uint16, message string) *mysql.MySQLError {
	return &mysql.MySQLError{
		Number:  number,
		Message: message,
	}
}

func TestHandleSpecialDDLError(t *testing.T) {
	var (
		cfg                 = genDefaultSubTaskConfig4Test()
		syncer              = NewSyncer(cfg, nil, nil)
		tctx                = tcontext.Background()
		conn2               = dbconn.NewDBConn(cfg, nil)
		customErr           = errors.New("custom error")
		invalidDDL          = "SQL CAN NOT BE PARSED"
		insertDML           = "INSERT INTO tbl VALUES (1)"
		createTable         = "CREATE TABLE tbl (col INT)"
		addUK               = "ALTER TABLE tbl ADD UNIQUE INDEX idx(col)"
		addFK               = "ALTER TABLE tbl ADD CONSTRAINT fk FOREIGN KEY (col) REFERENCES tbl2 (col)"
		addColumn           = "ALTER TABLE tbl ADD COLUMN col INT"
		addIndexMulti       = "ALTER TABLE tbl ADD INDEX idx1(col1), ADD INDEX idx2(col2)"
		addIndex1           = "ALTER TABLE tbl ADD INDEX idx(col)"
		addIndex2           = "CREATE INDEX idx ON tbl(col)"
		dropColumnWithIndex = "ALTER TABLE tbl DROP c1"
		cases               = []struct {
			err     error
			ddls    []string
			index   int
			handled bool
		}{
			{
				err: mysql.ErrInvalidConn, // empty DDLs
			},
			{
				err:  mysql.ErrInvalidConn,
				ddls: []string{addColumn, addIndex1}, // error happen not on the last
			},
			{
				err:  mysql.ErrInvalidConn,
				ddls: []string{addIndex1, addColumn}, // error happen not on the last
			},
			{
				err:  mysql.ErrInvalidConn,
				ddls: []string{addIndex1, addIndex2}, // error happen not on the last
			},
			{
				err:  customErr, // not `invalid connection`
				ddls: []string{addIndex1},
			},
			{
				err:  mysql.ErrInvalidConn,
				ddls: []string{invalidDDL}, // invalid DDL
			},
			{
				err:  mysql.ErrInvalidConn,
				ddls: []string{insertDML}, // invalid DDL
			},
			{
				err:  mysql.ErrInvalidConn,
				ddls: []string{createTable}, // not `ADD INDEX`
			},
			{
				err:  mysql.ErrInvalidConn,
				ddls: []string{addColumn}, // not `ADD INDEX`
			},
			{
				err:  mysql.ErrInvalidConn,
				ddls: []string{addUK}, // not `ADD INDEX`, but `ADD UNIQUE INDEX`
			},
			{
				err:  mysql.ErrInvalidConn,
				ddls: []string{addFK}, // not `ADD INDEX`, but `ADD * FOREIGN KEY`
			},
			{
				err:  mysql.ErrInvalidConn,
				ddls: []string{addIndexMulti}, // multi `ADD INDEX` in one statement
			},
			{
				err:     mysql.ErrInvalidConn,
				ddls:    []string{addIndex1},
				handled: true,
			},
			{
				err:     mysql.ErrInvalidConn,
				ddls:    []string{addIndex2},
				handled: true,
			},
			{
				err:     mysql.ErrInvalidConn,
				ddls:    []string{addColumn, addIndex1},
				index:   1,
				handled: true,
			},
			{
				err:     mysql.ErrInvalidConn,
				ddls:    []string{addColumn, addIndex2},
				index:   1,
				handled: true,
			},
			{
				err:     mysql.ErrInvalidConn,
				ddls:    []string{addIndex1, addIndex2},
				index:   1,
				handled: true,
			},
			{
				err:   newMysqlErr(errno.ErrUnsupportedDDLOperation, "drop column xx with index"),
				ddls:  []string{addIndex1, dropColumnWithIndex},
				index: 0, // wrong index
			},
		}
	)
	conn2.ResetBaseConnFn = func(*tcontext.Context, *conn.BaseConn) (*conn.BaseConn, error) {
		return nil, nil
	}

	syncer.metricsProxies = metrics.DefaultMetricsProxies.CacheForOneTask("task", "worker", "source")

	for _, cs := range cases {
		err2 := syncer.handleSpecialDDLError(
			tctx, cs.err, cs.ddls, cs.index, conn2, -1, binlog.Location{}, binlog.Location{})
		if cs.handled {
			require.NoError(t, err2)
		} else {
			require.Equal(t, cs.err, err2)
		}
	}

	var (
		execErr = newMysqlErr(errno.ErrUnsupportedDDLOperation, "drop column xx with index")
		ddls    = []string{dropColumnWithIndex}
	)

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	conn1, err := db.Conn(context.Background())
	require.NoError(t, err)
	conn2.ResetBaseConnFn = func(_ *tcontext.Context, _ *conn.BaseConn) (*conn.BaseConn, error) {
		return conn.NewBaseConnForTest(conn1, nil), nil
	}
	err = conn2.ResetConn(tctx)
	require.NoError(t, err)

	// dropColumnF test successful
	mock.ExpectQuery("SELECT INDEX_NAME FROM information_schema.statistics WHERE.*").WillReturnRows(
		sqlmock.NewRows([]string{"INDEX_NAME"}).AddRow("gen_idx"))
	mock.ExpectQuery("SELECT count\\(\\*\\) FROM information_schema.statistics WHERE.*").WillReturnRows(
		sqlmock.NewRows([]string{"count(*)"}).AddRow(1))
	mock.ExpectBegin()
	mock.ExpectExec("ALTER TABLE ``.`tbl` DROP INDEX `gen_idx`").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	mock.ExpectBegin()
	mock.ExpectExec(dropColumnWithIndex).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	handledErr := syncer.handleSpecialDDLError(
		tctx, execErr, ddls, 0, conn2, -1, binlog.Location{}, binlog.Location{})
	require.NoError(t, mock.ExpectationsWereMet())
	require.NoError(t, handledErr)

	// dropColumnF test failed because multi-column index
	mock.ExpectQuery("SELECT INDEX_NAME FROM information_schema.statistics WHERE.*").WillReturnRows(
		sqlmock.NewRows([]string{"INDEX_NAME"}).AddRow("gen_idx"))
	mock.ExpectQuery("SELECT count\\(\\*\\) FROM information_schema.statistics WHERE.*").WillReturnRows(
		sqlmock.NewRows([]string{"count(*)"}).AddRow(2))

	handledErr = syncer.handleSpecialDDLError(
		tctx, execErr, ddls, 0, conn2, -1, binlog.Location{}, binlog.Location{})
	require.NoError(t, mock.ExpectationsWereMet())
	require.Error(t, execErr, handledErr)
}

func TestWaitAsyncDDLCanceled(t *testing.T) {
	const ddlSQL = "ALTER TABLE `test`.`t` ADD COLUMN `c` INT"

	cfg := genDefaultSubTaskConfig4Test()
	syncer := NewSyncer(cfg, nil, nil)
	syncer.metricsProxies = metrics.DefaultMetricsProxies.CacheForOneTask("task", "worker", "source")

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	sqlConn, err := db.Conn(context.Background())
	require.NoError(t, err)
	dbConn := dbconn.NewDBConn(cfg, conn.NewBaseConnForTest(sqlConn, &retry.FiniteRetryStrategy{}))
	mock.ExpectQuery("ADMIN SHOW DDL JOBS 10").WillReturnRows(
		sqlmock.NewRows([]string{"JOB_ID", "CREATE_TIME", "STATE"}).
			AddRow(1, "2026-08-03 18:00:00", "running"))
	mock.ExpectQuery("ADMIN SHOW DDL JOB QUERIES LIMIT 10 OFFSET 0").WillReturnRows(
		sqlmock.NewRows([]string{"JOB_ID", "QUERY"}).AddRow(1, ddlSQL))

	ctx, cancel := context.WithCancel(context.Background())
	tctx := tcontext.Background().WithContext(ctx)
	errCh := make(chan error, 1)
	go func() {
		errCh <- syncer.handleSpecialDDLError(
			tctx,
			mysql.ErrInvalidConn,
			[]string{ddlSQL},
			0,
			dbConn,
			1,
			binlog.Location{},
			binlog.Location{},
		)
	}()

	require.Eventually(t, func() bool {
		return mock.ExpectationsWereMet() == nil
	}, time.Second, 10*time.Millisecond)
	cancel()

	select {
	case err = <-errCh:
		require.True(t, utils.IsContextCanceledError(err), err)
		require.NotEqual(t, mysql.ErrInvalidConn, errors.Cause(err))
	case <-time.After(time.Second):
		t.Fatal("waitAsyncDDL did not stop after its context was canceled")
	}
}

func TestReconcileAsyncDDLMismatchIsBarrier(t *testing.T) {
	cfg := genDefaultSubTaskConfig4Test()
	syncer := NewSyncer(cfg, nil, nil)
	startLocation := binlog.MustZeroLocation(cfg.Flavor)
	currentLocation := startLocation.Clone()
	currentLocation.Position.Pos++
	info := syncer.setAsyncDDLReconcileInfo(
		[]string{"ALTER TABLE `old_target`.`t` ADD COLUMN `c` INT"},
		0,
		1,
		startLocation,
		currentLocation,
	)

	reconciled, err := syncer.reconcileAsyncDDL(
		tcontext.Background(),
		[]string{"ALTER TABLE `new_target`.`t` ADD COLUMN `c` INT"},
		startLocation,
		currentLocation,
		nil,
	)
	require.True(t, reconciled)
	require.True(t, terror.ErrDBUnExpect.Equal(err), err)
	require.Same(t, info, syncer.getAsyncDDLReconcileInfo())
	require.False(t, syncer.asyncDDLResolved(info))
}

func TestIsConnectionRefusedError(t *testing.T) {
	isConnRefusedErr := isConnectionRefusedError(nil)
	require.False(t, isConnRefusedErr)

	isConnRefusedErr = isConnectionRefusedError(errors.New("timeout"))
	require.False(t, isConnRefusedErr)

	isConnRefusedErr = isConnectionRefusedError(errors.New("connect: connection refused"))
	require.True(t, isConnRefusedErr)
}
