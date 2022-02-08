// Copyright 2021 PingCAP, Inc.
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
	"net/url"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	dmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/tiflow/pkg/util/testleak"
	"github.com/stretchr/testify/require"
)

func TestSinkParamsClone(t *testing.T) {
	param1 := defaultParams.Clone()
	param2 := param1.Clone()
	param2.changefeedID = "123"
	param2.batchReplaceEnabled = false
	param2.maxTxnRow = 1
	require.Equal(t, &sinkParams{
		workerCount:         DefaultWorkerCount,
		maxTxnRow:           DefaultMaxTxnRow,
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
		workerCount:         DefaultWorkerCount,
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

func TestGenerateDSNByParams(t *testing.T) {
	testDefaultParams := func() {
		db, err := mockTestDB(false)
		require.Nil(t, err)
		defer db.Close()

		dsn, err := dmysql.ParseDSN("root:123456@tcp(127.0.0.1:4000)/")
		require.Nil(t, err)
		params := defaultParams.Clone()
		dsnStr, err := generateDSNByParams(context.TODO(), dsn, params, db)
		require.Nil(t, err)
		expectedParams := []string{
			"tidb_txn_mode=optimistic",
			"readTimeout=2m",
			"writeTimeout=2m",
			"allow_auto_random_explicit_insert=1",
			"transaction_isolation=%22READ-COMMITTED%22",
		}
		for _, param := range expectedParams {
			require.True(t, strings.Contains(dsnStr, param))
		}
		require.False(t, strings.Contains(dsnStr, "time_zone"))
	}

	testTimezoneParam := func() {
		db, err := mockTestDB(false)
		require.Nil(t, err)
		defer db.Close()

		dsn, err := dmysql.ParseDSN("root:123456@tcp(127.0.0.1:4000)/")
		require.Nil(t, err)
		params := defaultParams.Clone()
		params.timezone = `"UTC"`
		dsnStr, err := generateDSNByParams(context.TODO(), dsn, params, db)
		require.Nil(t, err)
		require.True(t, strings.Contains(dsnStr, "time_zone=%22UTC%22"))
	}

	testTimeoutParams := func() {
		db, err := mockTestDB(false)
		require.Nil(t, err)
		defer db.Close()

		dsn, err := dmysql.ParseDSN("root:123456@tcp(127.0.0.1:4000)/")
		require.Nil(t, err)
		uri, err := url.Parse("mysql://127.0.0.1:3306/?read-timeout=4m&write-timeout=5m&timeout=3m")
		require.Nil(t, err)
		params, err := parseSinkURIToParams(context.TODO(), uri, map[string]string{})
		require.Nil(t, err)
		dsnStr, err := generateDSNByParams(context.TODO(), dsn, params, db)
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

func TestParseSinkURIToParams(t *testing.T) {
	defer testleak.AfterTestT(t)()
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
	params, err := parseSinkURIToParams(context.TODO(), uri, opts)
	require.Nil(t, err)
	require.Equal(t, expected, params)
}

func TestParseSinkURITimezone(t *testing.T) {
	defer testleak.AfterTestT(t)()
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
		params, err := parseSinkURIToParams(ctx, uri, opts)
		require.Nil(t, err)
		require.Equal(t, expected[i], params.timezone)
	}
}

func TestParseSinkURIOverride(t *testing.T) {
	defer testleak.AfterTestT(t)()
	cases := []struct {
		uri     string
		checker func(*sinkParams)
	}{{
		uri: "mysql://127.0.0.1:3306/?worker-count=2147483648", // int32 max
		checker: func(sp *sinkParams) {
			require.EqualValues(t, sp.workerCount, maxWorkerCount)
		},
	}, {
		uri: "mysql://127.0.0.1:3306/?max-txn-row=2147483648", // int32 max
		checker: func(sp *sinkParams) {
			require.EqualValues(t, sp.maxTxnRow, maxMaxTxnRow)
		},
	}, {
		uri: "mysql://127.0.0.1:3306/?tidb-txn-mode=badmode",
		checker: func(sp *sinkParams) {
			require.EqualValues(t, sp.tidbTxnMode, defaultTiDBTxnMode)
		},
	}}
	ctx := context.TODO()
	opts := map[string]string{OptChangefeedID: "changefeed-01"}
	var uri *url.URL
	var err error
	for _, cs := range cases {
		if cs.uri != "" {
			uri, err = url.Parse(cs.uri)
			require.Nil(t, err)
		} else {
			uri = nil
		}
		p, err := parseSinkURIToParams(ctx, uri, opts)
		require.Nil(t, err)
		cs.checker(p)
	}
}

func TestParseSinkURIBadQueryString(t *testing.T) {
	defer testleak.AfterTestT(t)()
	uris := []string{
		"",
		"postgre://127.0.0.1:3306",
		"mysql://127.0.0.1:3306/?worker-count=not-number",
		"mysql://127.0.0.1:3306/?worker-count=-1",
		"mysql://127.0.0.1:3306/?worker-count=0",
		"mysql://127.0.0.1:3306/?max-txn-row=not-number",
		"mysql://127.0.0.1:3306/?max-txn-row=-1",
		"mysql://127.0.0.1:3306/?max-txn-row=0",
		"mysql://127.0.0.1:3306/?ssl-ca=only-ca-exists",
		"mysql://127.0.0.1:3306/?batch-replace-enable=not-bool",
		"mysql://127.0.0.1:3306/?batch-replace-enable=true&batch-replace-size=not-number",
		"mysql://127.0.0.1:3306/?safe-mode=not-bool",
		"mysql://127.0.0.1:3306/?time-zone=badtz",
		"mysql://127.0.0.1:3306/?write-timeout=badduration",
		"mysql://127.0.0.1:3306/?read-timeout=badduration",
		"mysql://127.0.0.1:3306/?timeout=badduration",
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
		_, err = parseSinkURIToParams(ctx, uri, opts)
		require.Error(t, err)
	}
}

func TestCheckTiDBVariable(t *testing.T) {
	defer testleak.AfterTestT(t)()
	db, mock, err := sqlmock.New()
	require.Nil(t, err)
	defer db.Close() //nolint:errcheck
	columns := []string{"Variable_name", "Value"}

	mock.ExpectQuery("show session variables like 'allow_auto_random_explicit_insert';").WillReturnRows(
		sqlmock.NewRows(columns).AddRow("allow_auto_random_explicit_insert", "0"),
	)
	val, err := checkTiDBVariable(context.TODO(), db, "allow_auto_random_explicit_insert", "1")
	require.Nil(t, err)
	require.Equal(t, "1", val)

	mock.ExpectQuery("show session variables like 'no_exist_variable';").WillReturnError(sql.ErrNoRows)
	val, err = checkTiDBVariable(context.TODO(), db, "no_exist_variable", "0")
	require.Nil(t, err)
	require.Equal(t, "", val)

	mock.ExpectQuery("show session variables like 'version';").WillReturnError(sql.ErrConnDone)
	_, err = checkTiDBVariable(context.TODO(), db, "version", "5.7.25-TiDB-v4.0.0")
	require.NotNil(t, err)
	require.Regexp(t, ".*"+sql.ErrConnDone.Error(), err.Error())
}
