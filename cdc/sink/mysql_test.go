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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	dmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/common"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/cyclic/mark"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/filter"
	"github.com/pingcap/tiflow/pkg/retry"
	"github.com/stretchr/testify/require"
)

func newMySQLSink4Test(ctx context.Context, t *testing.T) *mysqlSink {
	f, err := filter.NewFilter(config.GetDefaultReplicaConfig())
	require.Nil(t, err)
	params := defaultParams.Clone()
	params.batchReplaceEnabled = false
	return &mysqlSink{
		txnCache:   common.NewUnresolvedTxnCache(),
		filter:     f,
		statistics: NewStatistics(ctx, "test", make(map[string]string)),
		params:     params,
	}
}

func TestPrepareDML(t *testing.T) {
	testCases := []struct {
		input    []*model.RowChangedEvent
		expected *preparedDMLs
	}{
		{
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
		dmls := ms.prepareDMLs(tc.input, 0, 0)
		require.Equal(t, tc.expected, dmls)
	}
}

func TestPrepareUpdate(t *testing.T) {
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
		require.Equal(t, tc.expectedSQL, query)
		require.Equal(t, tc.expectedArgs, args)
	}
}

func TestPrepareDelete(t *testing.T) {
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
		require.Equal(t, tc.expectedSQL, query)
		require.Equal(t, tc.expectedArgs, args)
	}
}

func TestWhereSlice(t *testing.T) {
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
		require.Equal(t, tc.expectedColNames, colNames)
		require.Equal(t, tc.expectedArgs, args)
	}
}

func TestMapReplace(t *testing.T) {
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
			require.Equal(t, tc.expectedQuery, query)
			require.Equal(t, tc.expectedArgs, args)
		}
	}
}

type sqlArgs [][]interface{}

func (a sqlArgs) Len() int           { return len(a) }
func (a sqlArgs) Less(i, j int) bool { return fmt.Sprintf("%s", a[i]) < fmt.Sprintf("%s", a[j]) }
func (a sqlArgs) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

func TestReduceReplace(t *testing.T) {
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
		require.Equal(t, tc.expectSQLs, sqls)
		require.Equal(t, tc.expectArgs, args)
	}
}

func TestSinkParamsClone(t *testing.T) {
	param1 := defaultParams.Clone()
	param2 := param1.Clone()
	param2.changefeedID = "123"
	param2.batchReplaceEnabled = false
	param2.maxTxnRow = 1
	require.Equal(t, &sinkParams{
		workerCount:         defaultWorkerCount,
		maxTxnRow:           defaultMaxTxnRow,
		tidbTxnMode:         defaultTiDBTxnMode,
		batchReplaceEnabled: defaultBatchReplaceEnabled,
		batchReplaceSize:    defaultBatchReplaceSize,
		readTimeout:         defaultReadTimeout,
		writeTimeout:        defaultWriteTimeout,
		dialTimeout:         defaultDialTimeout,
		safeMode:            defaultSafeMode,
	}, param1)
	require.Equal(t, &sinkParams{
		changefeedID:        "123",
		workerCount:         defaultWorkerCount,
		maxTxnRow:           1,
		tidbTxnMode:         defaultTiDBTxnMode,
		batchReplaceEnabled: false,
		batchReplaceSize:    defaultBatchReplaceSize,
		readTimeout:         defaultReadTimeout,
		writeTimeout:        defaultWriteTimeout,
		dialTimeout:         defaultDialTimeout,
		safeMode:            defaultSafeMode,
	}, param2)
}

func TestConfigureSinkURI(t *testing.T) {
	testDefaultParams := func() {
		db, err := mockTestDB()
		require.Nil(t, err)
		defer db.Close()

		dsn, err := dmysql.ParseDSN("root:123456@tcp(127.0.0.1:4000)/")
		require.Nil(t, err)
		params := defaultParams.Clone()
		dsnStr, err := configureSinkURI(context.TODO(), dsn, params, db)
		require.Nil(t, err)
		expectedParams := []string{
			"tidb_txn_mode=optimistic",
			"readTimeout=2m",
			"writeTimeout=2m",
			"allow_auto_random_explicit_insert=1",
		}
		for _, param := range expectedParams {
			require.True(t, strings.Contains(dsnStr, param))
		}
		require.False(t, strings.Contains(dsnStr, "time_zone"))
	}

	testTimezoneParam := func() {
		db, err := mockTestDB()
		require.Nil(t, err)
		defer db.Close()

		dsn, err := dmysql.ParseDSN("root:123456@tcp(127.0.0.1:4000)/")
		require.Nil(t, err)
		params := defaultParams.Clone()
		params.timezone = `"UTC"`
		dsnStr, err := configureSinkURI(context.TODO(), dsn, params, db)
		require.Nil(t, err)
		require.True(t, strings.Contains(dsnStr, "time_zone=%22UTC%22"))
	}

	testTimeoutParams := func() {
		db, err := mockTestDB()
		require.Nil(t, err)
		defer db.Close()

		dsn, err := dmysql.ParseDSN("root:123456@tcp(127.0.0.1:4000)/")
		require.Nil(t, err)
		uri, err := url.Parse("mysql://127.0.0.1:3306/?read-timeout=4m&write-timeout=5m&timeout=3m")
		require.Nil(t, err)
		params, err := parseSinkURI(context.TODO(), uri, map[string]string{})
		require.Nil(t, err)
		dsnStr, err := configureSinkURI(context.TODO(), dsn, params, db)
		require.Nil(t, err)
		expectedParams := []string{
			"readTimeout=4m",
			"writeTimeout=5m",
			"timeout=3m",
		}
		for _, param := range expectedParams {
			require.True(t, strings.Contains(dsnStr, param))
		}
	}

	testDefaultParams()
	testTimezoneParam()
	testTimeoutParams()
}

func TestParseSinkURI(t *testing.T) {
	expected := defaultParams.Clone()
	expected.workerCount = 64
	expected.maxTxnRow = 20
	expected.batchReplaceEnabled = true
	expected.batchReplaceSize = 50
	expected.safeMode = true
	expected.timezone = `"UTC"`
	expected.changefeedID = "cf-id"
	expected.captureAddr = "127.0.0.1:8300"
	expected.tidbTxnMode = "pessimistic"
	uriStr := "mysql://127.0.0.1:3306/?worker-count=64&max-txn-row=20" +
		"&batch-replace-enable=true&batch-replace-size=50&safe-mode=true" +
		"&tidb-txn-mode=pessimistic"
	opts := map[string]string{
		OptChangefeedID: expected.changefeedID,
		OptCaptureAddr:  expected.captureAddr,
	}
	uri, err := url.Parse(uriStr)
	require.Nil(t, err)
	params, err := parseSinkURI(context.TODO(), uri, opts)
	require.Nil(t, err)
	require.Equal(t, expected, params)
}

func TestParseSinkURITimezone(t *testing.T) {
	uris := []string{
		"mysql://127.0.0.1:3306/?time-zone=Asia/Shanghai&worker-count=32",
		"mysql://127.0.0.1:3306/?time-zone=&worker-count=32",
		"mysql://127.0.0.1:3306/?worker-count=32",
	}
	expected := []string{
		"\"Asia/Shanghai\"",
		"",
		"\"UTC\"",
	}
	ctx := context.TODO()
	opts := map[string]string{}
	for i, uriStr := range uris {
		uri, err := url.Parse(uriStr)
		require.Nil(t, err)
		params, err := parseSinkURI(ctx, uri, opts)
		require.Nil(t, err)
		require.Equal(t, expected[i], params.timezone)
	}
}

func TestParseSinkURIBadQueryString(t *testing.T) {
	uris := []string{
		"",
		"postgre://127.0.0.1:3306",
		"mysql://127.0.0.1:3306/?worker-count=not-number",
		"mysql://127.0.0.1:3306/?max-txn-row=not-number",
		"mysql://127.0.0.1:3306/?ssl-ca=only-ca-exists",
		"mysql://127.0.0.1:3306/?batch-replace-enable=not-bool",
		"mysql://127.0.0.1:3306/?batch-replace-enable=true&batch-replace-size=not-number",
		"mysql://127.0.0.1:3306/?safe-mode=not-bool",
	}
	ctx := context.TODO()
	opts := map[string]string{OptChangefeedID: "changefeed-01"}
	var uri *url.URL
	var err error
	for _, uriStr := range uris {
		if uriStr != "" {
			uri, err = url.Parse(uriStr)
			require.Nil(t, err)
		} else {
			uri = nil
		}
		_, err = parseSinkURI(ctx, uri, opts)
		require.NotNil(t, err)
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

func TestAdjustSQLMode(t *testing.T) {
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
			require.Nil(t, err)
			return db, nil
		}
		// normal db
		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		require.Nil(t, err)
		mock.ExpectQuery("SELECT @@SESSION.sql_mode;").
			WillReturnRows(sqlmock.NewRows([]string{"@@SESSION.sql_mode"}).
				AddRow("ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE"))
		mock.ExpectExec("SET sql_mode = 'ONLY_FULL_GROUP_BY,NO_ZERO_IN_DATE,NO_ZERO_DATE';").
			WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectClose()
		return db, nil
	}
	backupGetDBConn := getDBConnImpl
	getDBConnImpl = mockGetDBConn
	defer func() {
		getDBConnImpl = backupGetDBConn
	}()

	changefeed := "test-changefeed"
	sinkURI, err := url.Parse("mysql://127.0.0.1:4000/?time-zone=UTC&worker-count=4")
	require.Nil(t, err)
	require.Nil(t, err)
	rc := config.GetDefaultReplicaConfig()
	rc.Cyclic = &config.CyclicConfig{
		Enable:          true,
		ReplicaID:       1,
		FilterReplicaID: []uint64{2},
	}
	f, err := filter.NewFilter(rc)
	require.Nil(t, err)
	cyclicConfig, err := rc.Cyclic.Marshal()
	require.Nil(t, err)
	opts := map[string]string{
		mark.OptCyclicConfig: cyclicConfig,
	}
	sink, err := newMySQLSink(ctx, changefeed, sinkURI, f, rc, opts)
	require.Nil(t, err)

	err = sink.Close(ctx)
	require.Nil(t, err)
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
	sinkURI, err := url.Parse(fmt.Sprintf("mysql://%s/?read-timeout=2s&timeout=2s", addr))
	require.Nil(t, err)
	rc := config.GetDefaultReplicaConfig()
	f, err := filter.NewFilter(rc)
	require.Nil(t, err)
	_, err = newMySQLSink(ctx, changefeed, sinkURI, f, rc, map[string]string{})
	require.Equal(t, driver.ErrBadConn, errors.Cause(err))
}

func TestNewMySQLSinkExecDML(t *testing.T) {
	dbIndex := 0
	mockGetDBConn := func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		defer func() {
			dbIndex++
		}()
		if dbIndex == 0 {
			// test db
			db, err := mockTestDB()
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
		mock.ExpectCommit()
		mock.ExpectBegin()
		mock.ExpectExec("REPLACE INTO `s1`.`t2`(`a`,`b`) VALUES (?,?),(?,?)").
			WithArgs(1, "test", 2, "test").
			WillReturnResult(sqlmock.NewResult(2, 2))
		mock.ExpectCommit()
		mock.ExpectClose()
		return db, nil
	}
	backupGetDBConn := getDBConnImpl
	getDBConnImpl = mockGetDBConn
	defer func() {
		getDBConnImpl = backupGetDBConn
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changefeed := "test-changefeed"
	sinkURI, err := url.Parse("mysql://127.0.0.1:4000/?time-zone=UTC&worker-count=4")
	require.Nil(t, err)
	rc := config.GetDefaultReplicaConfig()
	f, err := filter.NewFilter(rc)
	require.Nil(t, err)
	sink, err := newMySQLSink(ctx, changefeed, sinkURI, f, rc, map[string]string{})
	require.Nil(t, err)

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
	require.Nil(t, err)

	// retry to make sure event is flushed
	err = retry.Do(context.Background(), func() error {
		ts, err := sink.FlushRowChangedEvents(ctx, 1, uint64(2))
		require.Nil(t, err)
		if ts < uint64(2) {
			return errors.Errorf("checkpoint ts %d less than resolved ts %d", ts, 2)
		}
		return nil
	}, retry.WithBackoffBaseDelay(20), retry.WithMaxTries(10), retry.WithIsRetryableErr(cerror.IsRetryableError))

	require.Nil(t, err)

	err = retry.Do(context.Background(), func() error {
		ts, err := sink.FlushRowChangedEvents(ctx, 2, uint64(4))
		require.Nil(t, err)
		if ts < uint64(4) {
			return errors.Errorf("checkpoint ts %d less than resolved ts %d", ts, 4)
		}
		return nil
	}, retry.WithBackoffBaseDelay(20), retry.WithMaxTries(10), retry.WithIsRetryableErr(cerror.IsRetryableError))
	require.Nil(t, err)

	err = sink.Barrier(ctx, 2)
	require.Nil(t, err)

	err = sink.Close(ctx)
	require.Nil(t, err)
}

func TestExecDMLRollbackErrDatabaseNotExists(t *testing.T) {
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
	backupGetDBConn := getDBConnImpl
	getDBConnImpl = mockGetDBConnErrDatabaseNotExists
	defer func() {
		getDBConnImpl = backupGetDBConn
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changefeed := "test-changefeed"
	sinkURI, err := url.Parse("mysql://127.0.0.1:4000/?time-zone=UTC&worker-count=1")
	require.Nil(t, err)
	rc := config.GetDefaultReplicaConfig()
	f, err := filter.NewFilter(rc)
	require.Nil(t, err)
	sink, err := newMySQLSink(ctx, changefeed, sinkURI, f, rc, map[string]string{})
	require.Nil(t, err)

	err = sink.(*mysqlSink).execDMLs(ctx, rows, 1 /* replicaID */, 1 /* bucket */)
	require.Equal(t, errDatabaseNotExists, errors.Cause(err))

	err = sink.Close(ctx)
	require.Nil(t, err)
}

func TestExecDMLRollbackErrTableNotExists(t *testing.T) {
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
	backupGetDBConn := getDBConnImpl
	getDBConnImpl = mockGetDBConnErrDatabaseNotExists
	defer func() {
		getDBConnImpl = backupGetDBConn
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changefeed := "test-changefeed"
	sinkURI, err := url.Parse("mysql://127.0.0.1:4000/?time-zone=UTC&worker-count=1")
	require.Nil(t, err)
	rc := config.GetDefaultReplicaConfig()
	f, err := filter.NewFilter(rc)
	require.Nil(t, err)
	sink, err := newMySQLSink(ctx, changefeed, sinkURI, f, rc, map[string]string{})
	require.Nil(t, err)

	err = sink.(*mysqlSink).execDMLs(ctx, rows, 1 /* replicaID */, 1 /* bucket */)
	require.Equal(t, errTableNotExists, errors.Cause(err))

	err = sink.Close(ctx)
	require.Nil(t, err)
}

func TestExecDMLRollbackErrRetryable(t *testing.T) {
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
			require.Nil(t, err)
			return db, nil
		}
		// normal db
		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		require.Nil(t, err)
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
	backupGetDBConn := getDBConnImpl
	getDBConnImpl = mockGetDBConnErrDatabaseNotExists
	defer func() {
		getDBConnImpl = backupGetDBConn
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changefeed := "test-changefeed"
	sinkURI, err := url.Parse("mysql://127.0.0.1:4000/?time-zone=UTC&worker-count=1")
	require.Nil(t, err)
	rc := config.GetDefaultReplicaConfig()
	f, err := filter.NewFilter(rc)
	require.Nil(t, err)
	sink, err := newMySQLSink(ctx, changefeed, sinkURI, f, rc, map[string]string{})
	require.Nil(t, err)

	err = sink.(*mysqlSink).execDMLs(ctx, rows, 1 /* replicaID */, 1 /* bucket */)
	require.Equal(t, errLockDeadlock, errors.Cause(err))

	err = sink.Close(ctx)
	require.Nil(t, err)
}

func TestNewMySQLSinkExecDDL(t *testing.T) {
	dbIndex := 0
	mockGetDBConn := func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		defer func() {
			dbIndex++
		}()
		if dbIndex == 0 {
			// test db
			db, err := mockTestDB()
			require.Nil(t, err)
			return db, nil
		}
		// normal db
		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		require.Nil(t, err)
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
	backupGetDBConn := getDBConnImpl
	getDBConnImpl = mockGetDBConn
	defer func() {
		getDBConnImpl = backupGetDBConn
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changefeed := "test-changefeed"
	sinkURI, err := url.Parse("mysql://127.0.0.1:4000/?time-zone=UTC&worker-count=4")
	require.Nil(t, err)
	rc := config.GetDefaultReplicaConfig()
	rc.Filter = &config.FilterConfig{
		Rules: []string{"test.t1"},
	}
	f, err := filter.NewFilter(rc)
	require.Nil(t, err)
	sink, err := newMySQLSink(ctx, changefeed, sinkURI, f, rc, map[string]string{})
	require.Nil(t, err)

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
	require.Nil(t, err)
	err = sink.EmitDDLEvent(ctx, ddl2)
	require.True(t, cerror.ErrDDLEventIgnored.Equal(err))
	// DDL execute failed, but error can be ignored
	err = sink.EmitDDLEvent(ctx, ddl1)
	require.Nil(t, err)

	err = sink.Close(ctx)
	require.Nil(t, err)
}

func TestNewMySQLSink(t *testing.T) {
	dbIndex := 0
	mockGetDBConn := func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		defer func() {
			dbIndex++
		}()
		if dbIndex == 0 {
			// test db
			db, err := mockTestDB()
			require.Nil(t, err)
			return db, nil
		}
		// normal db
		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		mock.ExpectClose()
		require.Nil(t, err)
		return db, nil
	}
	backupGetDBConn := getDBConnImpl
	getDBConnImpl = mockGetDBConn
	defer func() {
		getDBConnImpl = backupGetDBConn
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changefeed := "test-changefeed"
	sinkURI, err := url.Parse("mysql://127.0.0.1:4000/?time-zone=UTC&worker-count=4")
	require.Nil(t, err)
	rc := config.GetDefaultReplicaConfig()
	f, err := filter.NewFilter(rc)
	require.Nil(t, err)
	sink, err := newMySQLSink(ctx, changefeed, sinkURI, f, rc, map[string]string{})
	require.Nil(t, err)
	err = sink.Close(ctx)
	require.Nil(t, err)
}

func TestMySQLSinkClose(t *testing.T) {
	dbIndex := 0
	mockGetDBConn := func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		defer func() {
			dbIndex++
		}()
		if dbIndex == 0 {
			// test db
			db, err := mockTestDB()
			require.Nil(t, err)
			return db, nil
		}
		// normal db
		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		mock.ExpectClose()
		require.Nil(t, err)
		return db, nil
	}
	backupGetDBConn := getDBConnImpl
	getDBConnImpl = mockGetDBConn
	defer func() {
		getDBConnImpl = backupGetDBConn
	}()

	ctx := context.Background()

	changefeed := "test-changefeed"
	sinkURI, err := url.Parse("mysql://127.0.0.1:4000/?time-zone=UTC&worker-count=4")
	require.Nil(t, err)
	rc := config.GetDefaultReplicaConfig()
	f, err := filter.NewFilter(rc)
	require.Nil(t, err)

	// test sink.Close will work correctly even if the ctx pass in has not been cancel
	sink, err := newMySQLSink(ctx, changefeed, sinkURI, f, rc, map[string]string{})
	require.Nil(t, err)
	err = sink.Close(ctx)
	require.Nil(t, err)
}

func TestMySQLSinkFlushResovledTs(t *testing.T) {
	dbIndex := 0
	mockGetDBConn := func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		defer func() {
			dbIndex++
		}()
		if dbIndex == 0 {
			// test db
			db, err := mockTestDB()
			require.Nil(t, err)
			return db, nil
		}
		// normal db
		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		mock.ExpectBegin()
		mock.ExpectExec("REPLACE INTO `s1`.`t1`(`a`) VALUES (?)").
			WithArgs(1).
			WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectCommit()
		mock.ExpectBegin()
		mock.ExpectExec("REPLACE INTO `s1`.`t2`(`a`) VALUES (?)").
			WithArgs(1).
			WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectCommit()
		mock.ExpectClose()
		require.Nil(t, err)
		return db, nil
	}
	backupGetDBConn := getDBConnImpl
	getDBConnImpl = mockGetDBConn
	defer func() {
		getDBConnImpl = backupGetDBConn
	}()

	ctx := context.Background()

	changefeed := "test-changefeed"
	sinkURI, err := url.Parse("mysql://127.0.0.1:4000/?time-zone=UTC&worker-count=4")
	require.Nil(t, err)
	rc := config.GetDefaultReplicaConfig()
	f, err := filter.NewFilter(rc)
	require.Nil(t, err)

	// test sink.Close will work correctly even if the ctx pass in has not been cancel
	si, err := newMySQLSink(ctx, changefeed, sinkURI, f, rc, map[string]string{})
	sink := si.(*mysqlSink)
	require.Nil(t, err)
	checkpoint, err := sink.FlushRowChangedEvents(ctx, model.TableID(1), 1)
	require.Nil(t, err)
	require.Equal(t, uint64(0), checkpoint)
	rows := []*model.RowChangedEvent{
		{
			Table:    &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
			CommitTs: 5,
			Columns: []*model.Column{
				{Name: "a", Type: mysql.TypeLong, Flag: model.HandleKeyFlag | model.PrimaryKeyFlag, Value: 1},
			},
		},
	}
	err = sink.EmitRowChangedEvents(ctx, rows...)
	require.Nil(t, err)
	checkpoint, err = sink.FlushRowChangedEvents(ctx, model.TableID(1), 6)
	require.True(t, checkpoint <= 5)
	time.Sleep(500 * time.Millisecond)
	require.Nil(t, err)
	require.Equal(t, uint64(6), sink.getTableCheckpointTs(model.TableID(1)))
	rows = []*model.RowChangedEvent{
		{
			Table:    &model.TableName{Schema: "s1", Table: "t2", TableID: 2},
			CommitTs: 4,
			Columns: []*model.Column{
				{Name: "a", Type: mysql.TypeLong, Flag: model.HandleKeyFlag | model.PrimaryKeyFlag, Value: 1},
			},
		},
	}
	err = sink.EmitRowChangedEvents(ctx, rows...)
	require.Nil(t, err)
	checkpoint, err = sink.FlushRowChangedEvents(ctx, model.TableID(2), 5)
	require.True(t, checkpoint <= 5)
	time.Sleep(500 * time.Millisecond)
	require.Nil(t, err)
	require.Equal(t, uint64(5), sink.getTableCheckpointTs(model.TableID(2)))
	err = sink.Close(ctx)
	require.Nil(t, err)
}
