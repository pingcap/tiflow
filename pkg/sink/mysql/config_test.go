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
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/aws/aws-sdk-go/aws"
	dmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestGenerateDSNByConfig(t *testing.T) {
	t.Parallel()
	testDefaultConfig := func() {
		db, err := MockTestDB()
		require.Nil(t, err)
		defer db.Close()

		dsn, err := dmysql.ParseDSN("root:123456@tcp(127.0.0.1:4000)/")
		require.Nil(t, err)
		cfg := NewConfig()
		dsnStr, err := generateDSNByConfig(context.TODO(), dsn, cfg, db)
		require.Nil(t, err)
		expectedCfg := []string{
			"tidb_txn_mode=optimistic",
			"readTimeout=2m",
			"writeTimeout=2m",
			"allow_auto_random_explicit_insert=1",
			"transaction_isolation=%22READ-COMMITTED%22",
			"charset=utf8mb4",
			"tidb_placement_mode=%22ignore%22",
		}
		for _, param := range expectedCfg {
			require.Contains(t, dsnStr, param)
		}
		require.False(t, strings.Contains(dsnStr, "time_zone"))
	}

	testTimezoneParam := func() {
		db, err := MockTestDB()
		require.Nil(t, err)
		defer db.Close()

		dsn, err := dmysql.ParseDSN("root:123456@tcp(127.0.0.1:4000)/")
		require.Nil(t, err)
		cfg := NewConfig()
		cfg.Timezone = `"UTC"`
		dsnStr, err := generateDSNByConfig(context.TODO(), dsn, cfg, db)
		require.Nil(t, err)
		require.True(t, strings.Contains(dsnStr, "time_zone=%22UTC%22"))
	}

	testTimeoutConfig := func() {
		db, err := MockTestDB()
		require.Nil(t, err)
		defer db.Close()

		dsn, err := dmysql.ParseDSN("root:123456@tcp(127.0.0.1:4000)/")
		require.Nil(t, err)
		uri, err := url.Parse("mysql://127.0.0.1:3306/?read-timeout=4m&write-timeout=5m&timeout=3m")
		require.Nil(t, err)
		cfg := NewConfig()
		err = cfg.Apply("UTC",
			model.DefaultChangeFeedID("123"), uri, config.GetDefaultReplicaConfig())
		require.Nil(t, err)
		dsnStr, err := generateDSNByConfig(context.TODO(), dsn, cfg, db)
		require.Nil(t, err)
		expectedCfg := []string{
			"readTimeout=4m",
			"writeTimeout=5m",
			"timeout=3m",
		}
		for _, param := range expectedCfg {
			require.True(t, strings.Contains(dsnStr, param))
		}
	}

	testIsolationConfig := func() {
		db, mock, err := sqlmock.New()
		require.Nil(t, err)
		defer db.Close() // nolint:errcheck
		columns := []string{"Variable_name", "Value"}
		mock.ExpectQuery("show session variables like 'allow_auto_random_explicit_insert';").WillReturnRows(
			sqlmock.NewRows(columns).AddRow("allow_auto_random_explicit_insert", "0"),
		)
		mock.ExpectQuery("show session variables like 'tidb_txn_mode';").WillReturnRows(
			sqlmock.NewRows(columns).AddRow("tidb_txn_mode", "pessimistic"),
		)
		// simulate error
		dsn, err := dmysql.ParseDSN("root:123456@tcp(127.0.0.1:4000)/")
		require.Nil(t, err)
		cfg := NewConfig()
		var dsnStr string
		_, err = generateDSNByConfig(context.TODO(), dsn, cfg, db)
		require.Error(t, err)

		// simulate no transaction_isolation
		mock.ExpectQuery("show session variables like 'allow_auto_random_explicit_insert';").WillReturnRows(
			sqlmock.NewRows(columns).AddRow("allow_auto_random_explicit_insert", "0"),
		)
		mock.ExpectQuery("show session variables like 'tidb_txn_mode';").WillReturnRows(
			sqlmock.NewRows(columns).AddRow("tidb_txn_mode", "pessimistic"),
		)
		mock.ExpectQuery("show session variables like 'transaction_isolation';").WillReturnError(sql.ErrNoRows)
		mock.ExpectQuery("show session variables like 'tidb_placement_mode';").
			WillReturnRows(
				sqlmock.NewRows(columns).
					AddRow("tidb_placement_mode", "IGNORE"),
			)
		mock.ExpectQuery("show session variables like 'tidb_enable_external_ts_read';").
			WillReturnRows(
				sqlmock.NewRows(columns).
					AddRow("tidb_enable_external_ts_read", "OFF"),
			)
		dsnStr, err = generateDSNByConfig(context.TODO(), dsn, cfg, db)
		require.Nil(t, err)
		expectedCfg := []string{
			"tx_isolation=%22READ-COMMITTED%22",
		}
		for _, param := range expectedCfg {
			require.True(t, strings.Contains(dsnStr, param))
		}

		// simulate transaction_isolation
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
		mock.ExpectQuery("show session variables like 'tidb_enable_external_ts_read';").
			WillReturnRows(
				sqlmock.NewRows(columns).
					AddRow("tidb_enable_external_ts_read", "OFF"),
			)
		dsnStr, err = generateDSNByConfig(context.TODO(), dsn, cfg, db)
		require.Nil(t, err)
		expectedCfg = []string{
			"transaction_isolation=%22READ-COMMITTED%22",
		}
		for _, param := range expectedCfg {
			require.True(t, strings.Contains(dsnStr, param))
		}
	}

	testDefaultConfig()
	testTimezoneParam()
	testTimeoutConfig()
	testIsolationConfig()
}

func TestApplySinkURIParamsToConfig(t *testing.T) {
	t.Parallel()

	expected := NewConfig()
	expected.WorkerCount = 64
	expected.MaxTxnRow = 20
	expected.MaxMultiUpdateRowCount = 80
	expected.MaxMultiUpdateRowSize = 512
	expected.SafeMode = false
	expected.Timezone = `"UTC"`
	expected.tidbTxnMode = "pessimistic"
	expected.CachePrepStmts = true
	uriStr := "mysql://127.0.0.1:3306/?worker-count=64&max-txn-row=20" +
		"&max-multi-update-row=80&max-multi-update-row-size=512" +
		"&safe-mode=false" +
		"&tidb-txn-mode=pessimistic" +
		"&test-some-deprecated-config=true&test-deprecated-size-config=100" +
		"&cache-prep-stmts=true&prep-stmt-cache-size=1000000"
	uri, err := url.Parse(uriStr)
	require.Nil(t, err)
	cfg := NewConfig()
	err = cfg.Apply("UTC", model.ChangeFeedID{}, uri, config.GetDefaultReplicaConfig())
	require.Nil(t, err)
	require.Equal(t, expected, cfg)
}

func TestParseSinkURIOverride(t *testing.T) {
	t.Parallel()

	cases := []struct {
		uri     string
		checker func(*Config)
	}{{
		uri: "mysql://127.0.0.1:3306/?worker-count=2147483648", // int32 max
		checker: func(sp *Config) {
			require.EqualValues(t, sp.WorkerCount, maxWorkerCount)
		},
	}, {
		uri: "mysql://127.0.0.1:3306/?max-txn-row=2147483648", // int32 max
		checker: func(sp *Config) {
			require.EqualValues(t, sp.MaxTxnRow, maxMaxTxnRow)
		},
	}, {
		uri: "mysql://127.0.0.1:3306/?max-multi-update-row=2147483648", // int32 max
		checker: func(sp *Config) {
			require.EqualValues(t, sp.MaxMultiUpdateRowCount, maxMaxMultiUpdateRowCount)
		},
	}, {
		uri: "mysql://127.0.0.1:3306/?max-multi-update-row-size=2147483648", // int32 max
		checker: func(sp *Config) {
			require.EqualValues(t, sp.MaxMultiUpdateRowSize, maxMaxMultiUpdateRowSize)
		},
	}, {
		uri: "mysql://127.0.0.1:3306/?tidb-txn-mode=badmode",
		checker: func(sp *Config) {
			require.EqualValues(t, sp.tidbTxnMode, defaultTiDBTxnMode)
		},
	}, {
		uri: "mysql://127.0.0.1:3306/?cache-prep-stmts=false",
		checker: func(sp *Config) {
			require.EqualValues(t, sp.CachePrepStmts, false)
		},
	}}
	var uri *url.URL
	var err error
	for _, cs := range cases {
		if cs.uri != "" {
			uri, err = url.Parse(cs.uri)
			require.Nil(t, err)
		} else {
			uri = nil
		}
		cfg := NewConfig()
		err = cfg.Apply("UTC",
			model.DefaultChangeFeedID("changefeed-01"),
			uri, config.GetDefaultReplicaConfig())
		require.Nil(t, err)
		cs.checker(cfg)
	}
}

func TestParseSinkURIBadQueryString(t *testing.T) {
	t.Parallel()

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
		"mysql://127.0.0.1:3306/?safe-mode=not-bool",
		"mysql://127.0.0.1:3306/?time-zone=badtz",
		"mysql://127.0.0.1:3306/?write-timeout=badduration",
		"mysql://127.0.0.1:3306/?read-timeout=badduration",
		"mysql://127.0.0.1:3306/?timeout=badduration",
	}
	var uri *url.URL
	var err error
	for _, uriStr := range uris {
		if uriStr != "" {
			uri, err = url.Parse(uriStr)
			require.Nil(t, err)
		} else {
			uri = nil
		}
		cfg := NewConfig()
		err = cfg.Apply("UTC",
			model.DefaultChangeFeedID("changefeed-01"), uri, config.GetDefaultReplicaConfig())
		require.Error(t, err)
	}
}

func TestCheckTiDBVariable(t *testing.T) {
	t.Parallel()

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

func TestApplyTimezone(t *testing.T) {
	t.Parallel()

	localTimezone, err := util.GetTimezone("Local")
	require.Nil(t, err)

	for _, test := range []struct {
		name                 string
		noChangefeedTimezone bool
		changefeedTimezone   string
		serverTimezone       *time.Location
		expected             string
		expectedHasErr       bool
		expectedErr          string
	}{
		{
			name:                 "no changefeed timezone",
			noChangefeedTimezone: true,
			serverTimezone:       time.UTC,
			expected:             "\"UTC\"",
			expectedHasErr:       false,
		},
		{
			name:                 "empty changefeed timezone",
			noChangefeedTimezone: false,
			changefeedTimezone:   "",
			serverTimezone:       time.UTC,
			expected:             "",
			expectedHasErr:       false,
		},
		{
			name:                 "normal changefeed timezone",
			noChangefeedTimezone: false,
			changefeedTimezone:   "UTC",
			serverTimezone:       time.UTC,
			expected:             "\"UTC\"",
			expectedHasErr:       false,
		},
		{
			name:                 "local timezone",
			noChangefeedTimezone: false,
			changefeedTimezone:   "Local",
			serverTimezone:       localTimezone,
			expected:             "\"" + localTimezone.String() + "\"",
			expectedHasErr:       false,
		},
		{
			name:                 "sink-uri timezone different from server timezone",
			noChangefeedTimezone: false,
			changefeedTimezone:   "UTC",
			serverTimezone:       localTimezone,
			expectedHasErr:       true,
			expectedErr:          "Please make sure that the timezone of the TiCDC server",
		},
		{
			name:                 "unsupported timezone format",
			noChangefeedTimezone: false,
			changefeedTimezone:   "%2B08%3A00", // +08:00
			serverTimezone:       time.UTC,
			expectedHasErr:       true,
			expectedErr:          "unknown time zone +08:00",
		},
	} {
		tc := test
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := NewConfig()
			sinkURI := "mysql://127.0.0.1:3306"
			if !tc.noChangefeedTimezone {
				sinkURI = sinkURI + "?time-zone=" + tc.changefeedTimezone
			}
			uri, err := url.Parse(sinkURI)
			require.Nil(t, err)
			err = cfg.Apply(tc.serverTimezone.String(),
				model.DefaultChangeFeedID("changefeed-01"), uri, config.GetDefaultReplicaConfig())
			if tc.expectedHasErr {
				require.NotNil(t, err)
				require.Contains(t, err.Error(), tc.expectedErr)
			} else {
				require.Nil(t, err)
				require.Equal(t, tc.expected, cfg.Timezone)
			}
		})
	}
}

func TestMergeConfig(t *testing.T) {
	uri := "mysql://topic"
	sinkURI, err := url.Parse(uri)
	require.NoError(t, err)
	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Sink.MySQLConfig = &config.MySQLConfig{
		WorkerCount:                  aws.Int(13),
		MaxTxnRow:                    aws.Int(100),
		MaxMultiUpdateRowSize:        aws.Int(102),
		MaxMultiUpdateRowCount:       aws.Int(103),
		TiDBTxnMode:                  aws.String("pessimistic"),
		TimeZone:                     aws.String("Asia/Shanghai"),
		WriteTimeout:                 aws.String("1m1s"),
		ReadTimeout:                  aws.String("1m2s"),
		Timeout:                      aws.String("1m3s"),
		EnableBatchDML:               aws.Bool(true),
		EnableMultiStatement:         aws.Bool(true),
		EnableCachePreparedStatement: aws.Bool(true),
	}
	c := NewConfig()
	err = c.Apply("Asia/Shanghai", model.DefaultChangeFeedID("test"), sinkURI, replicaConfig)
	require.NoError(t, err)
	require.Equal(t, 13, c.WorkerCount)
	require.Equal(t, 100, c.MaxTxnRow)
	require.Equal(t, 102, c.MaxMultiUpdateRowSize)
	require.Equal(t, 103, c.MaxMultiUpdateRowCount)
	require.Equal(t, "pessimistic", c.tidbTxnMode)
	require.Equal(t, "\"Asia/Shanghai\"", c.Timezone)
	require.Equal(t, "1m1s", c.WriteTimeout)
	require.Equal(t, "1m2s", c.ReadTimeout)
	require.Equal(t, "1m3s", c.DialTimeout)
	require.Equal(t, true, c.BatchDMLEnable)
	require.Equal(t, true, c.MultiStmtEnable)
	require.Equal(t, true, c.CachePrepStmts)

	uri = "mysql://topic?" +
		"worker-count=13&" +
		"max-txn-row=100&" +
		"max-multi-update-row-size=102&" +
		"max-multi-update-row=103&" +
		"tidb-txn-mode=pessimistic&" +
		"time-zone=Asia/Shanghai&" +
		"write-timeout=1m1s&" +
		"read-timeout=1m2s&" +
		"timeout=1m3s&" +
		"batch-dml-enable=true&" +
		"multi-stmt-enable=true&" +
		"cache-prep-stmts=true"
	sinkURI, err = url.Parse(uri)
	require.NoError(t, err)
	replicaConfig = config.GetDefaultReplicaConfig()
	replicaConfig.Sink.MySQLConfig = &config.MySQLConfig{
		WorkerCount:                  aws.Int(11),
		MaxTxnRow:                    aws.Int(130),
		MaxMultiUpdateRowSize:        aws.Int(142),
		MaxMultiUpdateRowCount:       aws.Int(153),
		TiDBTxnMode:                  aws.String("optimistic"),
		TimeZone:                     aws.String("utc"),
		WriteTimeout:                 aws.String("2m1s"),
		ReadTimeout:                  aws.String("3m2s"),
		Timeout:                      aws.String("4m3s"),
		EnableBatchDML:               aws.Bool(false),
		EnableMultiStatement:         aws.Bool(false),
		EnableCachePreparedStatement: aws.Bool(false),
	}
	c = NewConfig()
	err = c.Apply("Asia/Shanghai", model.DefaultChangeFeedID("test"), sinkURI, replicaConfig)
	require.NoError(t, err)
	require.Equal(t, 13, c.WorkerCount)
	require.Equal(t, 100, c.MaxTxnRow)
	require.Equal(t, 102, c.MaxMultiUpdateRowSize)
	require.Equal(t, 103, c.MaxMultiUpdateRowCount)
	require.Equal(t, "pessimistic", c.tidbTxnMode)
	require.Equal(t, "\"Asia/Shanghai\"", c.Timezone)
	require.Equal(t, "1m1s", c.WriteTimeout)
	require.Equal(t, "1m2s", c.ReadTimeout)
	require.Equal(t, "1m3s", c.DialTimeout)
	require.Equal(t, true, c.BatchDMLEnable)
	require.Equal(t, true, c.MultiStmtEnable)
	require.Equal(t, true, c.CachePrepStmts)
}
