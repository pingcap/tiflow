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

package sink

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net"
	"net/url"
	"sort"
	"sync"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	dmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/sink/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/cyclic/mark"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/retry"
	"github.com/pingcap/ticdc/pkg/util/testleak"
	"github.com/pingcap/tidb/infoschema"
	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
)

type MySQLSinkSuite struct{}

func Test(t *testing.T) { check.TestingT(t) }

var _ = check.Suite(&MySQLSinkSuite{})

func newMySQLSink4Test(ctx context.Context, c *check.C) *mysqlSink {
	f, err := filter.NewFilter(config.GetDefaultReplicaConfig())
	c.Assert(err, check.IsNil)
	params := defaultParams.Clone()
	params.batchReplaceEnabled = false
	return &mysqlSink{
		txnCache:   common.NewUnresolvedTxnCache(),
		filter:     f,
		statistics: NewStatistics(ctx, "test", make(map[string]string)),
		params:     params,
	}
}

func (s MySQLSinkSuite) TestPrepareDML(c *check.C) {
	defer testleak.AfterTest(c)()
	testCases := []struct {
		input    []*model.RowChangedEvent
		expected *preparedDMLs
	}{{
		input:    []*model.RowChangedEvent{},
		expected: &preparedDMLs{sqls: []string{}, values: [][]interface{}{}},
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
			sqls:     []string{"DELETE FROM `common_1`.`uk_without_pk` WHERE `a1` = ? AND `a3` = ? LIMIT 1;"},
			values:   [][]interface{}{{1, 1}},
			rowCount: 1,
		},
	}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ms := newMySQLSink4Test(ctx, c)
	for i, tc := range testCases {
		dmls := ms.prepareDMLs(tc.input, 0, 0)
		c.Assert(dmls, check.DeepEquals, tc.expected, check.Commentf("%d", i))
	}
}

func (s MySQLSinkSuite) TestPrepareUpdate(c *check.C) {
	defer testleak.AfterTest(c)()
	testCases := []struct {
		quoteTable   string
		preCols      []*model.Column
		cols         []*model.Column
		expectedSQL  string
		expectedArgs []interface{}
	}{
		{
			quoteTable:   "`test`.`t1`",
			preCols:      []*model.Column{},
			cols:         []*model.Column{},
			expectedSQL:  "",
			expectedArgs: nil,
		},
		{
			quoteTable: "`test`.`t1`",
			preCols: []*model.Column{
				{Name: "a", Type: mysql.TypeLong, Flag: model.HandleKeyFlag | model.PrimaryKeyFlag, Value: 1},
				{Name: "b", Type: mysql.TypeVarchar, Flag: 0, Value: "test"},
			},
			cols: []*model.Column{
				{Name: "a", Type: mysql.TypeLong, Flag: model.HandleKeyFlag | model.PrimaryKeyFlag, Value: 1},
				{Name: "b", Type: mysql.TypeVarchar, Flag: 0, Value: "test2"},
			},
			expectedSQL:  "UPDATE `test`.`t1` SET `a`=?,`b`=? WHERE `a`=? LIMIT 1;",
			expectedArgs: []interface{}{1, "test2", 1},
		},
		{
			quoteTable: "`test`.`t1`",
			preCols: []*model.Column{
				{Name: "a", Type: mysql.TypeLong, Flag: model.MultipleKeyFlag | model.HandleKeyFlag, Value: 1},
				{Name: "b", Type: mysql.TypeVarString, Flag: model.MultipleKeyFlag | model.HandleKeyFlag, Value: "test"},
				{Name: "c", Type: mysql.TypeLong, Flag: model.GeneratedColumnFlag, Value: 100},
			},
			cols: []*model.Column{
				{Name: "a", Type: mysql.TypeLong, Flag: model.MultipleKeyFlag | model.HandleKeyFlag, Value: 2},
				{Name: "b", Type: mysql.TypeVarString, Flag: model.MultipleKeyFlag | model.HandleKeyFlag, Value: "test2"},
				{Name: "c", Type: mysql.TypeLong, Flag: model.GeneratedColumnFlag, Value: 100},
			},
			expectedSQL:  "UPDATE `test`.`t1` SET `a`=?,`b`=? WHERE `a`=? AND `b`=? LIMIT 1;",
			expectedArgs: []interface{}{2, "test2", 1, "test"},
		},
	}
	for _, tc := range testCases {
		query, args := prepareUpdate(tc.quoteTable, tc.preCols, tc.cols, false)
		c.Assert(query, check.Equals, tc.expectedSQL)
		c.Assert(args, check.DeepEquals, tc.expectedArgs)
	}
}

func (s MySQLSinkSuite) TestPrepareDelete(c *check.C) {
	defer testleak.AfterTest(c)()
	testCases := []struct {
		quoteTable   string
		preCols      []*model.Column
		expectedSQL  string
		expectedArgs []interface{}
	}{
		{
			quoteTable:   "`test`.`t1`",
			preCols:      []*model.Column{},
			expectedSQL:  "",
			expectedArgs: nil,
		},
		{
			quoteTable: "`test`.`t1`",
			preCols: []*model.Column{
				{Name: "a", Type: mysql.TypeLong, Flag: model.HandleKeyFlag | model.PrimaryKeyFlag, Value: 1},
				{Name: "b", Type: mysql.TypeVarchar, Flag: 0, Value: "test"},
			},
			expectedSQL:  "DELETE FROM `test`.`t1` WHERE `a` = ? LIMIT 1;",
			expectedArgs: []interface{}{1},
		},
		{
			quoteTable: "`test`.`t1`",
			preCols: []*model.Column{
				{Name: "a", Type: mysql.TypeLong, Flag: model.MultipleKeyFlag | model.HandleKeyFlag, Value: 1},
				{Name: "b", Type: mysql.TypeVarString, Flag: model.MultipleKeyFlag | model.HandleKeyFlag, Value: "test"},
				{Name: "c", Type: mysql.TypeLong, Flag: model.GeneratedColumnFlag, Value: 100},
			},
			expectedSQL:  "DELETE FROM `test`.`t1` WHERE `a` = ? AND `b` = ? LIMIT 1;",
			expectedArgs: []interface{}{1, "test"},
		},
	}
	for _, tc := range testCases {
		query, args := prepareDelete(tc.quoteTable, tc.preCols, false)
		c.Assert(query, check.Equals, tc.expectedSQL)
		c.Assert(args, check.DeepEquals, tc.expectedArgs)
	}
}

func (s MySQLSinkSuite) TestWhereSlice(c *check.C) {
	defer testleak.AfterTest(c)()
	testCases := []struct {
		cols             []*model.Column
		forceReplicate   bool
		expectedColNames []string
		expectedArgs     []interface{}
	}{
		{
			cols:             []*model.Column{},
			forceReplicate:   false,
			expectedColNames: nil,
			expectedArgs:     nil,
		},
		{
			cols: []*model.Column{
				{Name: "a", Type: mysql.TypeLong, Flag: model.HandleKeyFlag | model.PrimaryKeyFlag, Value: 1},
				{Name: "b", Type: mysql.TypeVarchar, Flag: 0, Value: "test"},
			},
			forceReplicate:   false,
			expectedColNames: []string{"a"},
			expectedArgs:     []interface{}{1},
		},
		{
			cols: []*model.Column{
				{Name: "a", Type: mysql.TypeLong, Flag: model.MultipleKeyFlag | model.HandleKeyFlag, Value: 1},
				{Name: "b", Type: mysql.TypeVarString, Flag: model.MultipleKeyFlag | model.HandleKeyFlag, Value: "test"},
				{Name: "c", Type: mysql.TypeLong, Flag: model.GeneratedColumnFlag, Value: 100},
			},
			forceReplicate:   false,
			expectedColNames: []string{"a", "b"},
			expectedArgs:     []interface{}{1, "test"},
		},
		{
			cols:             []*model.Column{},
			forceReplicate:   true,
			expectedColNames: []string{},
			expectedArgs:     []interface{}{},
		},
		{
			cols: []*model.Column{
				{Name: "a", Type: mysql.TypeLong, Flag: model.HandleKeyFlag | model.PrimaryKeyFlag, Value: 1},
				{Name: "b", Type: mysql.TypeVarchar, Flag: 0, Value: "test"},
			},
			forceReplicate:   true,
			expectedColNames: []string{"a"},
			expectedArgs:     []interface{}{1},
		},
		{
			cols: []*model.Column{
				{Name: "a", Type: mysql.TypeLong, Flag: model.MultipleKeyFlag | model.HandleKeyFlag, Value: 1},
				{Name: "b", Type: mysql.TypeVarString, Flag: model.MultipleKeyFlag | model.HandleKeyFlag, Value: "test"},
				{Name: "c", Type: mysql.TypeLong, Flag: model.GeneratedColumnFlag, Value: 100},
			},
			forceReplicate:   true,
			expectedColNames: []string{"a", "b"},
			expectedArgs:     []interface{}{1, "test"},
		},
		{
			cols: []*model.Column{
				{Name: "a", Type: mysql.TypeLong, Flag: model.UniqueKeyFlag, Value: 1},
				{Name: "b", Type: mysql.TypeVarchar, Flag: 0, Value: "test"},
			},
			forceReplicate:   true,
			expectedColNames: []string{"a", "b"},
			expectedArgs:     []interface{}{1, "test"},
		},
		{
			cols: []*model.Column{
				{Name: "a", Type: mysql.TypeLong, Flag: model.MultipleKeyFlag, Value: 1},
				{Name: "b", Type: mysql.TypeVarString, Flag: model.MultipleKeyFlag, Value: "test"},
				{Name: "c", Type: mysql.TypeLong, Flag: model.GeneratedColumnFlag, Value: 100},
			},
			forceReplicate:   true,
			expectedColNames: []string{"a", "b", "c"},
			expectedArgs:     []interface{}{1, "test", 100},
		},
	}
	for _, tc := range testCases {
		colNames, args := whereSlice(tc.cols, tc.forceReplicate)
		c.Assert(colNames, check.DeepEquals, tc.expectedColNames)
		c.Assert(args, check.DeepEquals, tc.expectedArgs)
	}
}

func (s MySQLSinkSuite) TestMapReplace(c *check.C) {
	defer testleak.AfterTest(c)()
	testCases := []struct {
		quoteTable    string
		cols          []*model.Column
		expectedQuery string
		expectedArgs  []interface{}
	}{
		{
			quoteTable: "`test`.`t1`",
			cols: []*model.Column{
				{Name: "a", Type: mysql.TypeLong, Value: 1},
				{Name: "b", Type: mysql.TypeVarchar, Value: "varchar"},
				{Name: "c", Type: mysql.TypeLong, Value: 1, Flag: model.GeneratedColumnFlag},
				{Name: "d", Type: mysql.TypeTiny, Value: uint8(255)},
			},
			expectedQuery: "REPLACE INTO `test`.`t1`(`a`,`b`,`d`) VALUES ",
			expectedArgs:  []interface{}{1, "varchar", uint8(255)},
		},
		{
			quoteTable: "`test`.`t1`",
			cols: []*model.Column{
				{Name: "a", Type: mysql.TypeLong, Value: 1},
				{Name: "b", Type: mysql.TypeVarchar, Value: "varchar"},
				{Name: "c", Type: mysql.TypeLong, Value: 1},
				{Name: "d", Type: mysql.TypeTiny, Value: uint8(255)},
			},
			expectedQuery: "REPLACE INTO `test`.`t1`(`a`,`b`,`c`,`d`) VALUES ",
			expectedArgs:  []interface{}{1, "varchar", 1, uint8(255)},
		},
	}
	for _, tc := range testCases {
		// multiple times to verify the stability of column sequence in query string
		for i := 0; i < 10; i++ {
			query, args := prepareReplace(tc.quoteTable, tc.cols, false, false)
			c.Assert(query, check.Equals, tc.expectedQuery)
			c.Assert(args, check.DeepEquals, tc.expectedArgs)
		}
	}
}

type sqlArgs [][]interface{}

func (a sqlArgs) Len() int           { return len(a) }
func (a sqlArgs) Less(i, j int) bool { return fmt.Sprintf("%s", a[i]) < fmt.Sprintf("%s", a[j]) }
func (a sqlArgs) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

func (s MySQLSinkSuite) TestReduceReplace(c *check.C) {
	defer testleak.AfterTest(c)()
	testCases := []struct {
		replaces   map[string][][]interface{}
		batchSize  int
		sort       bool
		expectSQLs []string
		expectArgs [][]interface{}
	}{
		{
			replaces: map[string][][]interface{}{
				"REPLACE INTO `test`.`t1`(`a`,`b`) VALUES ": {
					[]interface{}{1, "1"},
					[]interface{}{2, "2"},
					[]interface{}{3, "3"},
				},
			},
			batchSize: 1,
			sort:      false,
			expectSQLs: []string{
				"REPLACE INTO `test`.`t1`(`a`,`b`) VALUES (?,?)",
				"REPLACE INTO `test`.`t1`(`a`,`b`) VALUES (?,?)",
				"REPLACE INTO `test`.`t1`(`a`,`b`) VALUES (?,?)",
			},
			expectArgs: [][]interface{}{
				{1, "1"},
				{2, "2"},
				{3, "3"},
			},
		},
		{
			replaces: map[string][][]interface{}{
				"REPLACE INTO `test`.`t1`(`a`,`b`) VALUES ": {
					[]interface{}{1, "1"},
					[]interface{}{2, "2"},
					[]interface{}{3, "3"},
					[]interface{}{4, "3"},
					[]interface{}{5, "5"},
				},
			},
			batchSize: 3,
			sort:      false,
			expectSQLs: []string{
				"REPLACE INTO `test`.`t1`(`a`,`b`) VALUES (?,?),(?,?),(?,?)",
				"REPLACE INTO `test`.`t1`(`a`,`b`) VALUES (?,?),(?,?)",
			},
			expectArgs: [][]interface{}{
				{1, "1", 2, "2", 3, "3"},
				{4, "3", 5, "5"},
			},
		},
		{
			replaces: map[string][][]interface{}{
				"REPLACE INTO `test`.`t1`(`a`,`b`) VALUES ": {
					[]interface{}{1, "1"},
					[]interface{}{2, "2"},
					[]interface{}{3, "3"},
					[]interface{}{4, "3"},
					[]interface{}{5, "5"},
				},
			},
			batchSize: 10,
			sort:      false,
			expectSQLs: []string{
				"REPLACE INTO `test`.`t1`(`a`,`b`) VALUES (?,?),(?,?),(?,?),(?,?),(?,?)",
			},
			expectArgs: [][]interface{}{
				{1, "1", 2, "2", 3, "3", 4, "3", 5, "5"},
			},
		},
		{
			replaces: map[string][][]interface{}{
				"REPLACE INTO `test`.`t1`(`a`,`b`) VALUES ": {
					[]interface{}{1, "1"},
					[]interface{}{2, "2"},
					[]interface{}{3, "3"},
					[]interface{}{4, "3"},
					[]interface{}{5, "5"},
					[]interface{}{6, "6"},
				},
				"REPLACE INTO `test`.`t2`(`a`,`b`) VALUES ": {
					[]interface{}{7, ""},
					[]interface{}{8, ""},
					[]interface{}{9, ""},
				},
			},
			batchSize: 3,
			sort:      true,
			expectSQLs: []string{
				"REPLACE INTO `test`.`t1`(`a`,`b`) VALUES (?,?),(?,?),(?,?)",
				"REPLACE INTO `test`.`t1`(`a`,`b`) VALUES (?,?),(?,?),(?,?)",
				"REPLACE INTO `test`.`t2`(`a`,`b`) VALUES (?,?),(?,?),(?,?)",
			},
			expectArgs: [][]interface{}{
				{1, "1", 2, "2", 3, "3"},
				{4, "3", 5, "5", 6, "6"},
				{7, "", 8, "", 9, ""},
			},
		},
	}
	for _, tc := range testCases {
		sqls, args := reduceReplace(tc.replaces, tc.batchSize)
		if tc.sort {
			sort.Strings(sqls)
			sort.Sort(sqlArgs(args))
		}
		c.Assert(sqls, check.DeepEquals, tc.expectSQLs)
		c.Assert(args, check.DeepEquals, tc.expectArgs)
	}
}

func mockTestDB() (*sql.DB, error) {
	// mock for test db, which is used querying TiDB session variable
	db, mock, err := sqlmock.New()
	if err != nil {
		return nil, err
	}
	columns := []string{"Variable_name", "Value"}
	mock.ExpectQuery("show session variables like 'allow_auto_random_explicit_insert';").WillReturnRows(
		sqlmock.NewRows(columns).AddRow("allow_auto_random_explicit_insert", "0"),
	)
	mock.ExpectQuery("show session variables like 'tidb_txn_mode';").WillReturnRows(
		sqlmock.NewRows(columns).AddRow("tidb_txn_mode", "pessimistic"),
	)
	mock.ExpectClose()
	return db, nil
}

func (s MySQLSinkSuite) TestAdjustSQLMode(c *check.C) {
	defer testleak.AfterTest(c)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dbIndex := 0
	mockGetDBConn := func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		defer func() {
			dbIndex++
		}()
		if dbIndex == 0 {
			// test db
			db, err := mockTestDB()
			c.Assert(err, check.IsNil)
			return db, nil
		}
		// normal db
		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		c.Assert(err, check.IsNil)
		mock.ExpectQuery("SELECT @@SESSION.sql_mode;").
			WillReturnRows(sqlmock.NewRows([]string{"@@SESSION.sql_mode"}).
				AddRow("ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE"))
		mock.ExpectExec("SET sql_mode = 'ONLY_FULL_GROUP_BY,NO_ZERO_IN_DATE,NO_ZERO_DATE';").
			WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectClose()
		return db, nil
	}
	backupGetDBConn := GetDBConnImpl
	GetDBConnImpl = mockGetDBConn
	defer func() {
		GetDBConnImpl = backupGetDBConn
	}()

	changefeed := "test-changefeed"
	sinkURI, err := url.Parse("mysql://127.0.0.1:4000/?time-zone=UTC&worker-count=4")
	c.Assert(err, check.IsNil)
	rc := config.GetDefaultReplicaConfig()
	rc.Cyclic = &config.CyclicConfig{
		Enable:          true,
		ReplicaID:       1,
		FilterReplicaID: []uint64{2},
	}
	f, err := filter.NewFilter(rc)
	c.Assert(err, check.IsNil)
	cyclicConfig, err := rc.Cyclic.Marshal()
	c.Assert(err, check.IsNil)
	opts := map[string]string{
		mark.OptCyclicConfig: cyclicConfig,
	}
	sink, err := newMySQLSink(ctx, changefeed, sinkURI, f, rc, opts)
	c.Assert(err, check.IsNil)

	err = sink.Close(ctx)
	c.Assert(err, check.IsNil)
}

type mockUnavailableMySQL struct {
	listener net.Listener
	quit     chan interface{}
	wg       sync.WaitGroup
}

func newMockUnavailableMySQL(addr string, c *check.C) *mockUnavailableMySQL {
	s := &mockUnavailableMySQL{
		quit: make(chan interface{}),
	}
	l, err := net.Listen("tcp", addr)
	c.Assert(err, check.IsNil)
	s.listener = l
	s.wg.Add(1)
	go s.serve(c)
	return s
}

func (s *mockUnavailableMySQL) serve(c *check.C) {
	defer s.wg.Done()

	for {
		_, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.quit:
				return
			default:
				c.Error(err)
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

func (s MySQLSinkSuite) TestNewMySQLTimeout(c *check.C) {
	defer testleak.AfterTest(c)()

	addr := "127.0.0.1:33333"
	mockMySQL := newMockUnavailableMySQL(addr, c)
	defer mockMySQL.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changefeed := "test-changefeed"
	sinkURI, err := url.Parse(fmt.Sprintf("mysql://%s/?read-timeout=2s&timeout=2s", addr))
	c.Assert(err, check.IsNil)
	rc := config.GetDefaultReplicaConfig()
	f, err := filter.NewFilter(rc)
	c.Assert(err, check.IsNil)
	_, err = newMySQLSink(ctx, changefeed, sinkURI, f, rc, map[string]string{})
	c.Assert(errors.Cause(err), check.Equals, driver.ErrBadConn)
}

func (s MySQLSinkSuite) TestNewMySQLSinkExecDML(c *check.C) {
	defer testleak.AfterTest(c)()

	dbIndex := 0
	mockGetDBConn := func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		defer func() {
			dbIndex++
		}()
		if dbIndex == 0 {
			// test db
			db, err := mockTestDB()
			c.Assert(err, check.IsNil)
			return db, nil
		}
		// normal db
		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		c.Assert(err, check.IsNil)
		mock.ExpectBegin()
		mock.ExpectExec("REPLACE INTO `s1`.`t1`(`a`,`b`) VALUES (?,?),(?,?)").
			WithArgs(1, "test", 2, "test").
			WillReturnResult(sqlmock.NewResult(2, 2))
		mock.ExpectCommit()
		mock.ExpectBegin()
		mock.ExpectExec("REPLACE INTO `s1`.`t2`(`a`,`b`) VALUES (?,?),(?,?)").
			WithArgs(1, "test", 2, "test").
			WillReturnResult(sqlmock.NewResult(2, 2))
		mock.ExpectCommit()
		mock.ExpectClose()
		return db, nil
	}
	backupGetDBConn := GetDBConnImpl
	GetDBConnImpl = mockGetDBConn
	defer func() {
		GetDBConnImpl = backupGetDBConn
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changefeed := "test-changefeed"
	sinkURI, err := url.Parse("mysql://127.0.0.1:4000/?time-zone=UTC&worker-count=4")
	c.Assert(err, check.IsNil)
	rc := config.GetDefaultReplicaConfig()
	f, err := filter.NewFilter(rc)
	c.Assert(err, check.IsNil)
	sink, err := newMySQLSink(ctx, changefeed, sinkURI, f, rc, map[string]string{})
	c.Assert(err, check.IsNil)

	rows := []*model.RowChangedEvent{
		{
			StartTs:  1,
			CommitTs: 2,
			Table:    &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
			Columns: []*model.Column{
				{Name: "a", Type: mysql.TypeLong, Flag: model.HandleKeyFlag | model.PrimaryKeyFlag, Value: 1},
				{Name: "b", Type: mysql.TypeVarchar, Flag: 0, Value: "test"},
			},
		},
		{
			StartTs:  1,
			CommitTs: 2,
			Table:    &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
			Columns: []*model.Column{
				{Name: "a", Type: mysql.TypeLong, Flag: model.HandleKeyFlag | model.PrimaryKeyFlag, Value: 2},
				{Name: "b", Type: mysql.TypeVarchar, Flag: 0, Value: "test"},
			},
		},
		{
			StartTs:  5,
			CommitTs: 6,
			Table:    &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
			Columns: []*model.Column{
				{Name: "a", Type: mysql.TypeLong, Flag: model.HandleKeyFlag | model.PrimaryKeyFlag, Value: 3},
				{Name: "b", Type: mysql.TypeVarchar, Flag: 0, Value: "test"},
			},
		},
		{
			StartTs:  3,
			CommitTs: 4,
			Table:    &model.TableName{Schema: "s1", Table: "t2", TableID: 2},
			Columns: []*model.Column{
				{Name: "a", Type: mysql.TypeLong, Flag: model.HandleKeyFlag | model.PrimaryKeyFlag, Value: 1},
				{Name: "b", Type: mysql.TypeVarchar, Flag: 0, Value: "test"},
			},
		},
		{
			StartTs:  3,
			CommitTs: 4,
			Table:    &model.TableName{Schema: "s1", Table: "t2", TableID: 2},
			Columns: []*model.Column{
				{Name: "a", Type: mysql.TypeLong, Flag: model.HandleKeyFlag | model.PrimaryKeyFlag, Value: 2},
				{Name: "b", Type: mysql.TypeVarchar, Flag: 0, Value: "test"},
			},
		},
	}

	err = sink.EmitRowChangedEvents(ctx, rows...)
	c.Assert(err, check.IsNil)

	err = retry.Do(context.Background(), func() error {
		ts, err := sink.FlushRowChangedEvents(ctx, uint64(2))
		c.Assert(err, check.IsNil)
		if ts < uint64(2) {
			return errors.Errorf("checkpoint ts %d less than resolved ts %d", ts, 2)
		}
		return nil
	}, retry.WithBackoffBaseDelay(20), retry.WithMaxTries(10), retry.WithIsRetryableErr(cerror.IsRetryableError))

	c.Assert(err, check.IsNil)

	err = retry.Do(context.Background(), func() error {
		ts, err := sink.FlushRowChangedEvents(ctx, uint64(4))
		c.Assert(err, check.IsNil)
		if ts < uint64(4) {
			return errors.Errorf("checkpoint ts %d less than resolved ts %d", ts, 4)
		}
		return nil
	}, retry.WithBackoffBaseDelay(20), retry.WithMaxTries(10), retry.WithIsRetryableErr(cerror.IsRetryableError))
	c.Assert(err, check.IsNil)

	err = sink.Barrier(ctx)
	c.Assert(err, check.IsNil)

	err = sink.Close(ctx)
	c.Assert(err, check.IsNil)
}

func (s MySQLSinkSuite) TestExecDMLRollbackErrDatabaseNotExists(c *check.C) {
	defer testleak.AfterTest(c)()

	rows := []*model.RowChangedEvent{
		{
			Table: &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
			Columns: []*model.Column{
				{Name: "a", Type: mysql.TypeLong, Flag: model.HandleKeyFlag | model.PrimaryKeyFlag, Value: 1},
			},
		},
		{
			Table: &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
			Columns: []*model.Column{
				{Name: "a", Type: mysql.TypeLong, Flag: model.HandleKeyFlag | model.PrimaryKeyFlag, Value: 2},
			},
		},
	}

	errDatabaseNotExists := &dmysql.MySQLError{
		Number: uint16(infoschema.ErrDatabaseNotExists.Code()),
	}

	dbIndex := 0
	mockGetDBConnErrDatabaseNotExists := func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		defer func() {
			dbIndex++
		}()
		if dbIndex == 0 {
			// test db
			db, err := mockTestDB()
			c.Assert(err, check.IsNil)
			return db, nil
		}
		// normal db
		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		c.Assert(err, check.IsNil)
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
	defer func() {
		GetDBConnImpl = backupGetDBConn
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changefeed := "test-changefeed"
	sinkURI, err := url.Parse("mysql://127.0.0.1:4000/?time-zone=UTC&worker-count=1")
	c.Assert(err, check.IsNil)
	rc := config.GetDefaultReplicaConfig()
	f, err := filter.NewFilter(rc)
	c.Assert(err, check.IsNil)
	sink, err := newMySQLSink(ctx, changefeed, sinkURI, f, rc, map[string]string{})
	c.Assert(err, check.IsNil)

	err = sink.(*mysqlSink).execDMLs(ctx, rows, 1 /* replicaID */, 1 /* bucket */)
	c.Assert(errors.Cause(err), check.Equals, errDatabaseNotExists)

	err = sink.Close(ctx)
	c.Assert(err, check.IsNil)
}

func (s MySQLSinkSuite) TestExecDMLRollbackErrTableNotExists(c *check.C) {
	defer testleak.AfterTest(c)()

	rows := []*model.RowChangedEvent{
		{
			Table: &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
			Columns: []*model.Column{
				{Name: "a", Type: mysql.TypeLong, Flag: model.HandleKeyFlag | model.PrimaryKeyFlag, Value: 1},
			},
		},
		{
			Table: &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
			Columns: []*model.Column{
				{Name: "a", Type: mysql.TypeLong, Flag: model.HandleKeyFlag | model.PrimaryKeyFlag, Value: 2},
			},
		},
	}

	errTableNotExists := &dmysql.MySQLError{
		Number: uint16(infoschema.ErrTableNotExists.Code()),
	}

	dbIndex := 0
	mockGetDBConnErrDatabaseNotExists := func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		defer func() {
			dbIndex++
		}()
		if dbIndex == 0 {
			// test db
			db, err := mockTestDB()
			c.Assert(err, check.IsNil)
			return db, nil
		}
		// normal db
		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		c.Assert(err, check.IsNil)
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
	defer func() {
		GetDBConnImpl = backupGetDBConn
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changefeed := "test-changefeed"
	sinkURI, err := url.Parse("mysql://127.0.0.1:4000/?time-zone=UTC&worker-count=1")
	c.Assert(err, check.IsNil)
	rc := config.GetDefaultReplicaConfig()
	f, err := filter.NewFilter(rc)
	c.Assert(err, check.IsNil)
	sink, err := newMySQLSink(ctx, changefeed, sinkURI, f, rc, map[string]string{})
	c.Assert(err, check.IsNil)

	err = sink.(*mysqlSink).execDMLs(ctx, rows, 1 /* replicaID */, 1 /* bucket */)
	c.Assert(errors.Cause(err), check.Equals, errTableNotExists)

	err = sink.Close(ctx)
	c.Assert(err, check.IsNil)
}

func (s MySQLSinkSuite) TestExecDMLRollbackErrRetryable(c *check.C) {
	defer testleak.AfterTest(c)()

	rows := []*model.RowChangedEvent{
		{
			Table: &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
			Columns: []*model.Column{
				{Name: "a", Type: mysql.TypeLong, Flag: model.HandleKeyFlag | model.PrimaryKeyFlag, Value: 1},
			},
		},
		{
			Table: &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
			Columns: []*model.Column{
				{Name: "a", Type: mysql.TypeLong, Flag: model.HandleKeyFlag | model.PrimaryKeyFlag, Value: 2},
			},
		},
	}

	errLockDeadlock := &dmysql.MySQLError{
		Number: mysql.ErrLockDeadlock,
	}

	dbIndex := 0
	mockGetDBConnErrDatabaseNotExists := func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		defer func() {
			dbIndex++
		}()
		if dbIndex == 0 {
			// test db
			db, err := mockTestDB()
			c.Assert(err, check.IsNil)
			return db, nil
		}
		// normal db
		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		c.Assert(err, check.IsNil)
		for i := 0; i < defaultDMLMaxRetryTime; i++ {
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
	defer func() {
		GetDBConnImpl = backupGetDBConn
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changefeed := "test-changefeed"
	sinkURI, err := url.Parse("mysql://127.0.0.1:4000/?time-zone=UTC&worker-count=1")
	c.Assert(err, check.IsNil)
	rc := config.GetDefaultReplicaConfig()
	f, err := filter.NewFilter(rc)
	c.Assert(err, check.IsNil)
	sink, err := newMySQLSink(ctx, changefeed, sinkURI, f, rc, map[string]string{})
	c.Assert(err, check.IsNil)

	err = sink.(*mysqlSink).execDMLs(ctx, rows, 1 /* replicaID */, 1 /* bucket */)
	c.Assert(errors.Cause(err), check.Equals, errLockDeadlock)

	err = sink.Close(ctx)
	c.Assert(err, check.IsNil)
}

func (s MySQLSinkSuite) TestNewMySQLSinkExecDDL(c *check.C) {
	defer testleak.AfterTest(c)()

	dbIndex := 0
	mockGetDBConn := func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		defer func() {
			dbIndex++
		}()
		if dbIndex == 0 {
			// test db
			db, err := mockTestDB()
			c.Assert(err, check.IsNil)
			return db, nil
		}
		// normal db
		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		c.Assert(err, check.IsNil)
		mock.ExpectBegin()
		mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectExec("ALTER TABLE test.t1 ADD COLUMN a int").WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectCommit()
		mock.ExpectBegin()
		mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectExec("ALTER TABLE test.t1 ADD COLUMN a int").
			WillReturnError(&dmysql.MySQLError{
				Number: uint16(infoschema.ErrColumnExists.Code()),
			})
		mock.ExpectRollback()
		mock.ExpectClose()
		return db, nil
	}
	backupGetDBConn := GetDBConnImpl
	GetDBConnImpl = mockGetDBConn
	defer func() {
		GetDBConnImpl = backupGetDBConn
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changefeed := "test-changefeed"
	sinkURI, err := url.Parse("mysql://127.0.0.1:4000/?time-zone=UTC&worker-count=4")
	c.Assert(err, check.IsNil)
	rc := config.GetDefaultReplicaConfig()
	rc.Filter = &config.FilterConfig{
		Rules: []string{"test.t1"},
	}
	f, err := filter.NewFilter(rc)
	c.Assert(err, check.IsNil)
	sink, err := newMySQLSink(ctx, changefeed, sinkURI, f, rc, map[string]string{})
	c.Assert(err, check.IsNil)

	ddl1 := &model.DDLEvent{
		StartTs:  1000,
		CommitTs: 1010,
		TableInfo: &model.SimpleTableInfo{
			Schema: "test",
			Table:  "t1",
		},
		Type:  timodel.ActionAddColumn,
		Query: "ALTER TABLE test.t1 ADD COLUMN a int",
	}
	ddl2 := &model.DDLEvent{
		StartTs:  1020,
		CommitTs: 1030,
		TableInfo: &model.SimpleTableInfo{
			Schema: "test",
			Table:  "t2",
		},
		Type:  timodel.ActionAddColumn,
		Query: "ALTER TABLE test.t1 ADD COLUMN a int",
	}

	err = sink.EmitDDLEvent(ctx, ddl1)
	c.Assert(err, check.IsNil)
	err = sink.EmitDDLEvent(ctx, ddl2)
	c.Assert(cerror.ErrDDLEventIgnored.Equal(err), check.IsTrue)
	// DDL execute failed, but error can be ignored
	err = sink.EmitDDLEvent(ctx, ddl1)
	c.Assert(err, check.IsNil)

	err = sink.Close(ctx)
	c.Assert(err, check.IsNil)
}

func (s MySQLSinkSuite) TestNeedSwitchDB(c *check.C) {
	defer testleak.AfterTest(c)()
	testCases := []struct {
		ddl        *model.DDLEvent
		needSwitch bool
	}{
		{
			&model.DDLEvent{
				TableInfo: &model.SimpleTableInfo{
					Schema: "",
				},
				Type: timodel.ActionCreateTable,
			},
			false,
		},
		{
			&model.DDLEvent{
				TableInfo: &model.SimpleTableInfo{
					Schema: "golang",
				},
				Type: timodel.ActionCreateSchema,
			},
			false,
		},
		{
			&model.DDLEvent{
				TableInfo: &model.SimpleTableInfo{
					Schema: "golang",
				},
				Type: timodel.ActionDropSchema,
			},
			false,
		},
		{
			&model.DDLEvent{
				TableInfo: &model.SimpleTableInfo{
					Schema: "golang",
				},
				Type: timodel.ActionCreateTable,
			},
			true,
		},
	}

	for _, tc := range testCases {
		c.Assert(needSwitchDB(tc.ddl), check.Equals, tc.needSwitch)
	}
}

func (s MySQLSinkSuite) TestNewMySQLSink(c *check.C) {
	defer testleak.AfterTest(c)()

	dbIndex := 0
	mockGetDBConn := func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		defer func() {
			dbIndex++
		}()
		if dbIndex == 0 {
			// test db
			db, err := mockTestDB()
			c.Assert(err, check.IsNil)
			return db, nil
		}
		// normal db
		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		mock.ExpectClose()
		c.Assert(err, check.IsNil)
		return db, nil
	}
	backupGetDBConn := GetDBConnImpl
	GetDBConnImpl = mockGetDBConn
	defer func() {
		GetDBConnImpl = backupGetDBConn
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	changefeed := "test-changefeed"
	sinkURI, err := url.Parse("mysql://127.0.0.1:4000/?time-zone=UTC&worker-count=4")
	c.Assert(err, check.IsNil)
	rc := config.GetDefaultReplicaConfig()
	f, err := filter.NewFilter(rc)
	c.Assert(err, check.IsNil)
	sink, err := newMySQLSink(ctx, changefeed, sinkURI, f, rc, map[string]string{})
	c.Assert(err, check.IsNil)
	err = sink.Close(ctx)
	c.Assert(err, check.IsNil)
}

func (s MySQLSinkSuite) TestMySQLSinkClose(c *check.C) {
	defer testleak.AfterTest(c)()

	dbIndex := 0
	mockGetDBConn := func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		defer func() {
			dbIndex++
		}()
		if dbIndex == 0 {
			// test db
			db, err := mockTestDB()
			c.Assert(err, check.IsNil)
			return db, nil
		}
		// normal db
		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		mock.ExpectClose()
		c.Assert(err, check.IsNil)
		return db, nil
	}
	backupGetDBConn := GetDBConnImpl
	GetDBConnImpl = mockGetDBConn
	defer func() {
		GetDBConnImpl = backupGetDBConn
	}()

	ctx := context.Background()

	changefeed := "test-changefeed"
	sinkURI, err := url.Parse("mysql://127.0.0.1:4000/?time-zone=UTC&worker-count=4")
	c.Assert(err, check.IsNil)
	rc := config.GetDefaultReplicaConfig()
	f, err := filter.NewFilter(rc)
	c.Assert(err, check.IsNil)

	// test sink.Close will work correctly even if the ctx pass in has not been cancel
	sink, err := newMySQLSink(ctx, changefeed, sinkURI, f, rc, map[string]string{})
	c.Assert(err, check.IsNil)
	err = sink.Close(ctx)
	c.Assert(err, check.IsNil)
}
