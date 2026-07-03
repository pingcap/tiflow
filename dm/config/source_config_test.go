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

package config

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	bf "github.com/pingcap/tiflow/pkg/binlog-filter"
	"github.com/stretchr/testify/require"
)

func TestConfigFunctions(t *testing.T) {
	cfg, err := ParseYaml(SampleSourceConfig)
	require.NoError(t, err)
	cfg.RelayDir = "./xx"
	require.Equal(t, uint32(101), cfg.ServerID)

	// test clone
	clone1 := cfg.Clone()
	require.Equal(t, cfg, clone1)
	clone1.ServerID = 100
	require.Equal(t, uint32(101), cfg.ServerID)

	// test format
	require.Contains(t, cfg.String(), `server-id":101`)
	tomlStr, err := clone1.Toml()
	require.NoError(t, err)
	require.Contains(t, tomlStr, `server-id = 100`)
	yamlStr, err := clone1.Yaml()
	require.NoError(t, err)
	require.Contains(t, yamlStr, `server-id: 100`)
	originCfgStr, err := cfg.Toml()
	require.NoError(t, err)
	require.Contains(t, originCfgStr, `server-id = 101`)
	originCfgYamlStr, err := cfg.Yaml()
	require.NoError(t, err)
	require.Contains(t, originCfgYamlStr, `server-id: 101`)

	// test update config file and reload
	require.NoError(t, cfg.Parse(tomlStr))
	require.Equal(t, uint32(100), cfg.ServerID)
	cfg1, err := ParseYaml(yamlStr)
	require.NoError(t, err)
	require.Equal(t, uint32(100), cfg1.ServerID)
	cfg.Filters = []*bf.BinlogEventRule{}
	cfg.Tracer = map[string]interface{}{}

	var cfg2 SourceConfig
	require.NoError(t, cfg2.Parse(originCfgStr))
	require.Equal(t, uint32(101), cfg2.ServerID)

	cfg3, err := ParseYaml(originCfgYamlStr)
	require.NoError(t, err)
	require.Equal(t, uint32(101), cfg3.ServerID)

	// test decrypt password
	clone1.From.Password = "1234"
	// fix empty map after marshal/unmarshal becomes nil
	clone1.From.Session = cfg.From.Session
	clone1.Tracer = map[string]interface{}{}
	clone1.Filters = []*bf.BinlogEventRule{}
	clone2 := cfg.DecryptPassword()
	require.Equal(t, clone1, clone2)

	cfg.From.Password = "xxx"
	cfg.DecryptPassword()

	cfg.From.Password = ""
	clone3 := cfg.DecryptPassword()
	require.Equal(t, cfg, clone3)

	// test toml and parse again
	clone4 := cfg.Clone()
	clone4.Checker.CheckEnable = true
	clone4.Checker.BackoffRollback = Duration{time.Minute * 5}
	clone4.Checker.BackoffMax = Duration{time.Minute * 5}
	clone4toml, err := clone4.Toml()
	require.NoError(t, err)
	require.Contains(t, clone4toml, `backoff-rollback = "5m`)
	require.Contains(t, clone4toml, `backoff-max = "5m`)

	var clone5 SourceConfig
	require.NoError(t, clone5.Parse(clone4toml))
	require.Equal(t, *clone4, clone5)
	clone4yaml, err := clone4.Yaml()
	require.NoError(t, err)
	require.Contains(t, clone4yaml, `backoff-rollback: 5m`)
	require.Contains(t, clone4yaml, `backoff-max: 5m`)

	clone6, err := ParseYaml(clone4yaml)
	require.NoError(t, err)
	clone6.From.Session = nil
	require.Equal(t, clone4, clone6)

	// test invalid config
	dir2 := t.TempDir()
	configFile := path.Join(dir2, "dm-worker-invalid.toml")
	configContent := []byte(`
source-id: haha
aaa: xxx
`)
	err = os.WriteFile(configFile, configContent, 0o644)
	require.NoError(t, err)
	_, err = LoadFromFile(configFile)
	require.ErrorContains(t, err, "field aaa not found in type config.SourceConfig")
}

func TestConfigVerify(t *testing.T) {
	newConfig := func() *SourceConfig {
		cfg, err := ParseYaml(SampleSourceConfig)
		require.NoError(t, err)
		cfg.RelayDir = "./xx"
		return cfg
	}
	testCases := []struct {
		genFunc     func() *SourceConfig
		errorFormat string
	}{
		{
			func() *SourceConfig {
				return newConfig()
			},
			"",
		},
		{
			func() *SourceConfig {
				cfg := newConfig()
				cfg.SourceID = ""
				return cfg
			},
			".*dm-worker should bind a non-empty source ID which represents a MySQL/MariaDB instance or a replica group.*",
		},
		{
			func() *SourceConfig {
				cfg := newConfig()
				cfg.SourceID = "source-id-length-more-than-thirty-two"
				return cfg
			},
			fmt.Sprintf(".*the length of source ID .* is more than max allowed value %d.*", MaxSourceIDLength),
		},
		{
			func() *SourceConfig {
				cfg := newConfig()
				cfg.EnableRelay = true
				cfg.RelayBinLogName = "mysql-binlog"
				return cfg
			},
			".*not valid.*",
		},
		{
			// after support `start-relay`, we always check Relay related config
			func() *SourceConfig {
				cfg := newConfig()
				cfg.RelayBinLogName = "mysql-binlog"
				return cfg
			},
			".*not valid.*",
		},
		{
			func() *SourceConfig {
				cfg := newConfig()
				cfg.EnableRelay = true
				cfg.RelayBinlogGTID = "9afe121c-40c2-11e9-9ec7-0242ac110002:1-rtc"
				return cfg
			},
			".*relay-binlog-gtid 9afe121c-40c2-11e9-9ec7-0242ac110002:1-rtc:.*",
		},
		{
			func() *SourceConfig {
				cfg := newConfig()
				cfg.From.Password = "not-encrypt"
				return cfg
			},
			"",
		},
		{
			func() *SourceConfig {
				cfg := newConfig()
				cfg.From.Password = "" // password empty
				return cfg
			},
			"",
		},
		{
			func() *SourceConfig {
				cfg := newConfig()
				cfg.From.Password = "123456" // plaintext password
				return cfg
			},
			"",
		},
		{
			func() *SourceConfig {
				cfg := newConfig()
				cfg.From.Password = "/Q7B9DizNLLTTfiZHv9WoEAKamfpIUs=" // encrypt password (123456)
				return cfg
			},
			"",
		},
	}

	for _, tc := range testCases {
		cfg := tc.genFunc()
		err := cfg.Verify()
		if tc.errorFormat != "" {
			require.Error(t, err)
			lines := strings.Split(err.Error(), "\n")
			require.Regexp(t, tc.errorFormat, lines[0])
		} else {
			require.NoError(t, err)
		}
	}
}

func TestSourceConfigForDowngrade(t *testing.T) {
	cfg, err := ParseYaml(SampleSourceConfig)
	require.NoError(t, err)

	// make sure all new field were added
	cfgForDowngrade := NewSourceConfigForDowngrade(cfg)
	cfgReflect := reflect.Indirect(reflect.ValueOf(cfg))
	cfgForDowngradeReflect := reflect.Indirect(reflect.ValueOf(cfgForDowngrade))
	// auto-fix-gtid, meta-dir are not written when downgrade
	require.Equal(t, cfgForDowngradeReflect.NumField()+2, cfgReflect.NumField())

	// make sure all field were copied
	cfgForClone := &SourceConfigForDowngrade{}
	Clone(cfgForClone, cfg)
	require.Equal(t, cfgForClone, cfgForDowngrade)
}

func subtestFlavor(t *testing.T, cfg *SourceConfig, sqlInfo, expectedFlavor, expectedError string) {
	t.Helper()

	cfg.Flavor = ""
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version';").
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
			AddRow("version", sqlInfo))
	mock.ExpectClose()

	err = cfg.AdjustFlavor(context.Background(), conn.NewBaseDBForTest(db))
	if expectedError == "" {
		require.NoError(t, err)
		require.Equal(t, expectedFlavor, cfg.Flavor)
	} else {
		require.ErrorContains(t, err, expectedError)
	}
}

func TestAdjustFlavor(t *testing.T) {
	cfg, err := ParseYaml(SampleSourceConfig)
	require.NoError(t, err)
	cfg.RelayDir = "./xx"

	cfg.Flavor = "mariadb"
	err = cfg.AdjustFlavor(context.Background(), nil)
	require.NoError(t, err)
	require.Equal(t, mysql.MariaDBFlavor, cfg.Flavor)
	cfg.Flavor = "MongoDB"
	err = cfg.AdjustFlavor(context.Background(), nil)
	require.ErrorContains(t, err, "flavor MongoDB not supported")

	subtestFlavor(t, cfg, "10.4.8-MariaDB-1:10.4.8+maria~bionic", mysql.MariaDBFlavor, "")
	subtestFlavor(t, cfg, "5.7.26-log", mysql.MySQLFlavor, "")
}

func TestAdjustServerID(t *testing.T) {
	originGetAllServerIDFunc := getAllServerIDFunc
	defer func() {
		getAllServerIDFunc = originGetAllServerIDFunc
	}()
	getAllServerIDFunc = getMockServerIDs

	cfg, err := ParseYaml(SampleSourceConfig)
	require.NoError(t, err)
	cfg.RelayDir = "./xx"

	require.NoError(t, cfg.AdjustServerID(context.Background(), nil))
	require.Equal(t, uint32(101), cfg.ServerID)

	cfg.ServerID = 0
	require.NoError(t, cfg.AdjustServerID(context.Background(), nil))
	require.NotEqual(t, 0, cfg.ServerID)
}

func TestAdjustServerIDFallback(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	mock.ExpectQuery("SHOW SLAVE HOSTS").
		WillReturnError(errors.New("mysql error 1227: Access denied; you need (at least one of) the REPLICATION SLAVE privilege(s) for this operation"))
	mock.ExpectClose()

	cfg, err := ParseYaml(SampleSourceConfig)
	require.NoError(t, err)
	cfg.ServerID = 0

	err = cfg.AdjustServerID(context.Background(), conn.NewBaseDBForTest(db))
	require.NoError(t, err)
	require.NotEqual(t, 0, cfg.ServerID)
}

func getMockServerIDs(ctx *tcontext.Context, db *conn.BaseDB) (map[uint32]struct{}, error) {
	return map[uint32]struct{}{
		1: {},
		2: {},
	}, nil
}

func TestAdjustCaseSensitive(t *testing.T) {
	cfg, err := ParseYaml(SampleSourceConfig)
	require.NoError(t, err)

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	mock.ExpectQuery("SELECT @@lower_case_table_names;").
		WillReturnRows(sqlmock.NewRows([]string{"@@lower_case_table_names"}).AddRow(conn.LCTableNamesMixed))
	require.NoError(t, cfg.AdjustCaseSensitive(context.Background(), conn.NewBaseDBForTest(db)))
	require.False(t, cfg.CaseSensitive)

	mock.ExpectQuery("SELECT @@lower_case_table_names;").
		WillReturnRows(sqlmock.NewRows([]string{"@@lower_case_table_names"}).AddRow(conn.LCTableNamesSensitive))
	require.NoError(t, cfg.AdjustCaseSensitive(context.Background(), conn.NewBaseDBForTest(db)))
	require.True(t, cfg.CaseSensitive)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestEmbedSampleFile(t *testing.T) {
	data, err := os.ReadFile("./source.yaml")
	require.NoError(t, err)
	require.Equal(t, SampleSourceConfig, string(data))
}
