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

package conn

import (
	"errors"
	"strings"
	"testing"

	tcontext "github.com/pingcap/dm/pkg/context"
	"github.com/pingcap/dm/pkg/metricsproxy"
	"github.com/pingcap/dm/pkg/retry"
	"github.com/pingcap/dm/pkg/terror"

	"github.com/DATA-DOG/go-sqlmock"
	. "github.com/pingcap/check"
	"github.com/prometheus/client_golang/prometheus"
)

func TestSuite(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testBaseConnSuite{})

type testBaseConnSuite struct{}

var testStmtHistogram = metricsproxy.NewHistogramVec(
	prometheus.HistogramOpts{
		Namespace: "dm",
		Subsystem: "conn",
		Name:      "stmt_duration_time",
		Help:      "Bucketed histogram of every statement query time (s).",
		Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 18),
	}, []string{"type", "task"})

func (t *testBaseConnSuite) TestBaseConn(c *C) {
	baseConn := NewBaseConn(nil, nil)

	tctx := tcontext.Background()
	err := baseConn.SetRetryStrategy(nil)
	c.Assert(err, IsNil)

	// nolint:sqlclosecheck
	_, err = baseConn.QuerySQL(tctx, "select 1")
	c.Assert(terror.ErrDBUnExpect.Equal(err), IsTrue)

	_, err = baseConn.ExecuteSQL(tctx, testStmtHistogram, "test", []string{""})
	c.Assert(terror.ErrDBUnExpect.Equal(err), IsTrue)

	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	dbConn, err := db.Conn(tctx.Context())
	c.Assert(err, IsNil)

	baseConn = &BaseConn{dbConn, nil}

	err = baseConn.SetRetryStrategy(&retry.FiniteRetryStrategy{})
	c.Assert(err, IsNil)

	mock.ExpectQuery("select 1").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
	// nolint:sqlclosecheck
	rows, err := baseConn.QuerySQL(tctx, "select 1")
	c.Assert(err, IsNil)
	ids := make([]int, 0, 1)
	for rows.Next() {
		var id int
		err = rows.Scan(&id)
		c.Assert(err, IsNil)
		ids = append(ids, id)
	}
	c.Assert(len(ids), Equals, 1)
	c.Assert(ids[0], Equals, 1)

	mock.ExpectQuery("select 1").WillReturnError(errors.New("invalid connection"))
	// nolint:sqlclosecheck
	_, err = baseConn.QuerySQL(tctx, "select 1")
	c.Assert(terror.ErrDBQueryFailed.Equal(err), IsTrue)

	affected, err := baseConn.ExecuteSQL(tctx, testStmtHistogram, "test", []string{})
	c.Assert(affected, Equals, 0)
	c.Assert(err, IsNil)

	mock.ExpectBegin()
	mock.ExpectExec("create database test").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	affected, err = baseConn.ExecuteSQL(tctx, testStmtHistogram, "test", []string{"create database test"})
	c.Assert(err, IsNil)
	c.Assert(affected, Equals, 1)

	mock.ExpectBegin().WillReturnError(errors.New("begin error"))
	_, err = baseConn.ExecuteSQL(tctx, testStmtHistogram, "test", []string{"create database test"})
	c.Assert(terror.ErrDBExecuteFailed.Equal(err), IsTrue)

	mock.ExpectBegin()
	mock.ExpectExec("create database test").WillReturnError(errors.New("invalid connection"))
	mock.ExpectRollback()
	_, err = baseConn.ExecuteSQL(tctx, testStmtHistogram, "test", []string{"create database test"})
	c.Assert(terror.ErrDBExecuteFailed.Equal(err), IsTrue)

	mock.ExpectBegin()
	mock.ExpectExec("create database test").WillReturnError(errors.New("ignore me"))
	mock.ExpectExec("create database test").WillReturnError(errors.New("don't ignore me"))
	mock.ExpectRollback()
	ignoreF := func(err error) bool {
		return err.Error() == "ignore me"
	}
	affected, err = baseConn.ExecuteSQLWithIgnoreError(tctx, testStmtHistogram, "test", ignoreF, []string{"create database test", "create database test"})
	c.Assert(strings.Contains(err.Error(), "don't ignore me"), IsTrue)
	c.Assert(affected, Equals, 1)

	if err = mock.ExpectationsWereMet(); err != nil {
		c.Fatal("there were unexpected:", err)
	}
	c.Assert(baseConn.close(), IsNil)
}
