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

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/syncer/dbconn"
	"github.com/pingcap/tiflow/dm/syncer/metrics"
)

func newMysqlErr(number uint16, message string) *mysql.MySQLError {
	return &mysql.MySQLError{
		Number:  number,
		Message: message,
	}
}

func (s *testSyncerSuite) TestHandleSpecialDDLError(c *C) {
	var (
		syncer              = NewSyncer(s.cfg, nil, nil)
		tctx                = tcontext.Background()
		conn2               = dbconn.NewDBConn(s.cfg, nil)
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
		err2 := syncer.handleSpecialDDLError(tctx, cs.err, cs.ddls, cs.index, conn2)
		if cs.handled {
			c.Assert(err2, IsNil)
		} else {
			c.Assert(err2, Equals, cs.err)
		}
	}

	var (
		execErr = newMysqlErr(errno.ErrUnsupportedDDLOperation, "drop column xx with index")
		ddls    = []string{dropColumnWithIndex}
	)

	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	conn1, err := db.Conn(context.Background())
	c.Assert(err, IsNil)
	conn2.ResetBaseConnFn = func(_ *tcontext.Context, _ *conn.BaseConn) (*conn.BaseConn, error) {
		return conn.NewBaseConn(conn1, nil), nil
	}
	err = conn2.ResetConn(tctx)
	c.Assert(err, IsNil)

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

	handledErr := syncer.handleSpecialDDLError(tctx, execErr, ddls, 0, conn2)
	c.Assert(mock.ExpectationsWereMet(), IsNil)
	c.Assert(handledErr, IsNil)

	// dropColumnF test failed because multi-column index
	mock.ExpectQuery("SELECT INDEX_NAME FROM information_schema.statistics WHERE.*").WillReturnRows(
		sqlmock.NewRows([]string{"INDEX_NAME"}).AddRow("gen_idx"))
	mock.ExpectQuery("SELECT count\\(\\*\\) FROM information_schema.statistics WHERE.*").WillReturnRows(
		sqlmock.NewRows([]string{"count(*)"}).AddRow(2))

	handledErr = syncer.handleSpecialDDLError(tctx, execErr, ddls, 0, conn2)
	c.Assert(mock.ExpectationsWereMet(), IsNil)
	c.Assert(handledErr, Equals, execErr)
}
