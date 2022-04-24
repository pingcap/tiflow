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
	"database/sql"
	"fmt"
	"os"
	"path"
	"reflect"
	"strings"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-mysql-org/go-mysql/mysql"
	. "github.com/pingcap/check"
	bf "github.com/pingcap/tidb-tools/pkg/binlog-filter"

	"github.com/pingcap/tiflow/dm/pkg/utils"
)

func (t *testConfig) TestConfig(c *C) {
	cfg, err := ParseYaml(SampleSourceConfig)
	c.Assert(err, IsNil)
	cfg.RelayDir = "./xx"
	c.Assert(cfg.RelayDir, Equals, "./xx")
	c.Assert(cfg.ServerID, Equals, uint32(101))

	// test clone
	clone1 := cfg.Clone()
	c.Assert(cfg, DeepEquals, clone1)
	clone1.ServerID = 100
	c.Assert(cfg.ServerID, Equals, uint32(101))

	// test format
	c.Assert(cfg.String(), Matches, `.*"server-id":101.*`)
	tomlStr, err := clone1.Toml()
	c.Assert(err, IsNil)
	c.Assert(tomlStr, Matches, `(.|\n)*server-id = 100(.|\n)*`)
	yamlStr, err := clone1.Yaml()
	c.Assert(err, IsNil)
	c.Assert(yamlStr, Matches, `(.|\n)*server-id: 100(.|\n)*`)
	originCfgStr, err := cfg.Toml()
	c.Assert(err, IsNil)
	c.Assert(originCfgStr, Matches, `(.|\n)*server-id = 101(.|\n)*`)
	originCfgYamlStr, err := cfg.Yaml()
	c.Assert(err, IsNil)
	c.Assert(originCfgYamlStr, Matches, `(.|\n)*server-id: 101(.|\n)*`)

	// test update config file and reload
	c.Assert(cfg.Parse(tomlStr), IsNil)
	c.Assert(cfg.ServerID, Equals, uint32(100))
	cfg1, err := ParseYaml(yamlStr)
	c.Assert(err, IsNil)
	c.Assert(cfg1.ServerID, Equals, uint32(100))
	cfg.Filters = []*bf.BinlogEventRule{}
	cfg.Tracer = map[string]interface{}{}

	var cfg2 SourceConfig
	c.Assert(cfg2.Parse(originCfgStr), IsNil)
	c.Assert(cfg2.ServerID, Equals, uint32(101))

	cfg3, err := ParseYaml(originCfgYamlStr)
	c.Assert(err, IsNil)
	c.Assert(cfg3.ServerID, Equals, uint32(101))

	// test decrypt password
	clone1.From.Password = "1234"
	// fix empty map after marshal/unmarshal becomes nil
	clone1.From.Session = cfg.From.Session
	clone1.Tracer = map[string]interface{}{}
	clone1.Filters = []*bf.BinlogEventRule{}
	clone2 := cfg.DecryptPassword()
	c.Assert(clone2, DeepEquals, clone1)

	cfg.From.Password = "xxx"
	cfg.DecryptPassword()

	cfg.From.Password = ""
	clone3 := cfg.DecryptPassword()
	c.Assert(clone3, DeepEquals, cfg)

	// test toml and parse again
	clone4 := cfg.Clone()
	clone4.Checker.CheckEnable = true
	clone4.Checker.BackoffRollback = Duration{time.Minute * 5}
	clone4.Checker.BackoffMax = Duration{time.Minute * 5}
	clone4toml, err := clone4.Toml()
	c.Assert(err, IsNil)
	c.Assert(clone4toml, Matches, "(.|\n)*backoff-rollback = \"5m(.|\n)*")
	c.Assert(clone4toml, Matches, "(.|\n)*backoff-max = \"5m(.|\n)*")

	var clone5 SourceConfig
	c.Assert(clone5.Parse(clone4toml), IsNil)
	c.Assert(clone5, DeepEquals, *clone4)
	clone4yaml, err := clone4.Yaml()
	c.Assert(err, IsNil)
	c.Assert(clone4yaml, Matches, "(.|\n)*backoff-rollback: 5m(.|\n)*")
	c.Assert(clone4yaml, Matches, "(.|\n)*backoff-max: 5m(.|\n)*")

	clone6, err := ParseYaml(clone4yaml)
	c.Assert(err, IsNil)
	clone6.From.Session = nil
	c.Assert(clone6, DeepEquals, clone4)

	// test invalid config
	dir2 := c.MkDir()
	configFile := path.Join(dir2, "dm-worker-invalid.toml")
	configContent := []byte(`
source-id: haha
aaa: xxx
`)
	err = os.WriteFile(configFile, configContent, 0o644)
	c.Assert(err, IsNil)
	_, err = LoadFromFile(configFile)
	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, "(.|\n)*field aaa not found in type config.SourceConfig(.|\n)*")
}

func (t *testConfig) TestConfigVerify(c *C) {
	newConfig := func() *SourceConfig {
		cfg, err := ParseYaml(SampleSourceConfig)
		c.Assert(err, IsNil)
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
			c.Assert(err, NotNil)
			lines := strings.Split(err.Error(), "\n")
			c.Assert(lines[0], Matches, tc.errorFormat)
		} else {
			c.Assert(err, IsNil)
		}
	}
}

func (t *testConfig) TestSourceConfigForDowngrade(c *C) {
	cfg, err := ParseYaml(SampleSourceConfig)
	c.Assert(err, IsNil)

	// make sure all new field were added
	cfgForDowngrade := NewSourceConfigForDowngrade(cfg)
	cfgReflect := reflect.Indirect(reflect.ValueOf(cfg))
	cfgForDowngradeReflect := reflect.Indirect(reflect.ValueOf(cfgForDowngrade))
	// auto-fix-gtid, meta-dir are not written when downgrade
	c.Assert(cfgReflect.NumField(), Equals, cfgForDowngradeReflect.NumField()+2)

	// make sure all field were copied
	cfgForClone := &SourceConfigForDowngrade{}
	Clone(cfgForClone, cfg)
	c.Assert(cfgForDowngrade, DeepEquals, cfgForClone)
}

func subtestFlavor(c *C, cfg *SourceConfig, sqlInfo, expectedFlavor, expectedError string) {
	cfg.Flavor = ""
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version';").
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
			AddRow("version", sqlInfo))
	mock.ExpectClose()

	err = cfg.AdjustFlavor(context.Background(), db)
	if expectedError == "" {
		c.Assert(err, IsNil)
		c.Assert(cfg.Flavor, Equals, expectedFlavor)
	} else {
		c.Assert(err, ErrorMatches, expectedError)
	}
}

func (t *testConfig) TestAdjustFlavor(c *C) {
	cfg, err := ParseYaml(SampleSourceConfig)
	c.Assert(err, IsNil)
	cfg.RelayDir = "./xx"

	cfg.Flavor = "mariadb"
	err = cfg.AdjustFlavor(context.Background(), nil)
	c.Assert(err, IsNil)
	c.Assert(cfg.Flavor, Equals, mysql.MariaDBFlavor)
	cfg.Flavor = "MongoDB"
	err = cfg.AdjustFlavor(context.Background(), nil)
	c.Assert(err, ErrorMatches, ".*flavor MongoDB not supported.*")

	subtestFlavor(c, cfg, "10.4.8-MariaDB-1:10.4.8+maria~bionic", mysql.MariaDBFlavor, "")
	subtestFlavor(c, cfg, "5.7.26-log", mysql.MySQLFlavor, "")
}

func (t *testConfig) TestAdjustServerID(c *C) {
	originGetAllServerIDFunc := getAllServerIDFunc
	defer func() {
		getAllServerIDFunc = originGetAllServerIDFunc
	}()
	getAllServerIDFunc = getMockServerIDs

	cfg, err := ParseYaml(SampleSourceConfig)
	c.Assert(err, IsNil)
	cfg.RelayDir = "./xx"

	c.Assert(cfg.AdjustServerID(context.Background(), nil), IsNil)
	c.Assert(cfg.ServerID, Equals, uint32(101))

	cfg.ServerID = 0
	c.Assert(cfg.AdjustServerID(context.Background(), nil), IsNil)
	c.Assert(cfg.ServerID, Not(Equals), 0)
}

func getMockServerIDs(ctx context.Context, db *sql.DB) (map[uint32]struct{}, error) {
	return map[uint32]struct{}{
		1: {},
		2: {},
	}, nil
}

func (t *testConfig) TestAdjustCaseSensitive(c *C) {
	cfg, err := ParseYaml(SampleSourceConfig)
	c.Assert(err, IsNil)

	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	mock.ExpectQuery("SELECT @@lower_case_table_names;").
		WillReturnRows(sqlmock.NewRows([]string{"@@lower_case_table_names"}).AddRow(utils.LCTableNamesMixed))
	c.Assert(cfg.AdjustCaseSensitive(context.Background(), db), IsNil)
	c.Assert(cfg.CaseSensitive, Equals, false)

	mock.ExpectQuery("SELECT @@lower_case_table_names;").
		WillReturnRows(sqlmock.NewRows([]string{"@@lower_case_table_names"}).AddRow(utils.LCTableNamesSensitive))
	c.Assert(cfg.AdjustCaseSensitive(context.Background(), db), IsNil)
	c.Assert(cfg.CaseSensitive, Equals, true)

	c.Assert(mock.ExpectationsWereMet(), IsNil)
}

func (t *testConfig) TestEmbedSampleFile(c *C) {
	data, err := os.ReadFile("./source.yaml")
	c.Assert(err, IsNil)
	c.Assert(SampleSourceConfig, Equals, string(data))
}
