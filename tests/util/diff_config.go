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

package util

import (
	"database/sql"
	"encoding/json"
	"flag"
	"net/url"
	"strconv"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	router "github.com/pingcap/tidb-tools/pkg/table-router"
	"go.uber.org/zap"
)

const (
	percent0   = 0
	percent100 = 100
)

var sourceInstanceMap map[string]interface{} = make(map[string]interface{})

// DBConfig is the config of database, and keep the connection.
type DiffDBConfig struct {
	dbutil.DBConfig

	InstanceID string `toml:"instance-id" json:"instance-id"`

	Conn *sql.DB
}

// Valid returns true if database's config is valide.
func (c *DiffDBConfig) Valid() bool {
	if c.InstanceID == "" {
		log.Error("must specify source database's instance id")
		return false
	}
	sourceInstanceMap[c.InstanceID] = struct{}{}

	return true
}

// CheckTables saves the tables need to check.
type DiffCheckTables struct {
	// schema name
	Schema string `toml:"schema" json:"schema"`

	// table list
	Tables []string `toml:"tables" json:"tables"`

	ExcludeTables []string `toml:"exclude-tables" json:"exclude-tables"`
}

// TableConfig is the config of table.
type DiffTableConfig struct {
	// table's origin information
	DiffTableInstance
	// columns be ignored, will not check this column's data
	IgnoreColumns []string `toml:"ignore-columns"`
	// field should be the primary key, unique key or field with index
	Fields string `toml:"index-fields"`
	// select range, for example: "age > 10 AND age < 20"
	Range string `toml:"range"`
	// set true if comparing sharding tables with target table, should have more than one source tables.
	IsSharding bool `toml:"is-sharding"`
	// saves the source tables's info.
	// may have more than one source for sharding tables.
	// or you want to compare table with different schema and table name.
	// SourceTables can be nil when source and target is one-to-one correspondence.
	SourceTables    []DiffTableInstance `toml:"source-tables"`
	TargetTableInfo *model.TableInfo

	// collation config in mysql/tidb
	Collation string `toml:"collation"`
}

// Valid returns true if table's config is valide.
func (t *DiffTableConfig) Valid() bool {
	if t.Schema == "" || t.Table == "" {
		log.Error("schema and table's name can't be empty")
		return false
	}

	if t.IsSharding {
		if len(t.SourceTables) <= 1 {
			log.Error("must have more than one source tables if comparing sharding tables")
			return false
		}

	} else {
		if len(t.SourceTables) > 1 {
			log.Error("have more than one source table in no sharding mode")
			return false
		}
	}

	for _, sourceTable := range t.SourceTables {
		if !sourceTable.Valid() {
			return false
		}
	}

	return true
}

// TableInstance saves the base information of table.
type DiffTableInstance struct {
	// database's instance id
	InstanceID string `toml:"instance-id" json:"instance-id"`
	// schema name
	Schema string `toml:"schema"`
	// table name
	Table string `toml:"table"`
}

// Valid returns true if table instance's info is valide.
// should be executed after source database's check.
func (t *DiffTableInstance) Valid() bool {
	if t.InstanceID == "" {
		log.Error("must specify the database's instance id for source table")
		return false
	}

	if _, ok := sourceInstanceMap[t.InstanceID]; !ok {
		log.Error("unknown database instance id", zap.String("instance id", t.InstanceID))
		return false
	}

	if t.Schema == "" || t.Table == "" {
		log.Error("schema and table's name can't be empty")
		return false
	}

	return true
}

// Config is the configuration.
type DiffConfig struct {
	*flag.FlagSet `json:"-"`

	// log level
	LogLevel string `toml:"log-level" json:"log-level"`

	// source database's config
	SourceDBCfg []DiffDBConfig `toml:"source-db" json:"source-db"`

	// target database's config
	TargetDBCfg DiffDBConfig `toml:"target-db" json:"target-db"`

	// for example, the whole data is [1...100]
	// we can split these data to [1...10], [11...20], ..., [91...100]
	// the [1...10] is a chunk, and it's chunk size is 10
	// size of the split chunk
	ChunkSize int `toml:"chunk-size" json:"chunk-size"`

	// sampling check percent, for example 10 means only check 10% data
	Sample int `toml:"sample-percent" json:"sample-percent"`

	// how many goroutines are created to check data
	CheckThreadCount int `toml:"check-thread-count" json:"check-thread-count"`

	// set false if want to comapre the data directly
	UseChecksum bool `toml:"use-checksum" json:"use-checksum"`

	// set true if just want compare data by checksum, will skip select data when checksum is not equal.
	OnlyUseChecksum bool `toml:"only-use-checksum" json:"only-use-checksum"`

	// the name of the file which saves sqls used to fix different data
	FixSQLFile string `toml:"fix-sql-file" json:"fix-sql-file"`

	// the tables to be checked
	Tables []*DiffCheckTables `toml:"check-tables" json:"check-tables"`

	// TableRules defines table name and database name's conversion relationship between source database and target database
	TableRules []*router.TableRule `toml:"table-rules" json:"table-rules"`

	// the config of table
	TableCfgs []*DiffTableConfig `toml:"table-config" json:"table-config"`

	// ignore check table's struct
	IgnoreStructCheck bool `toml:"ignore-struct-check" json:"ignore-struct-check"`

	// ignore check table's data
	IgnoreDataCheck bool `toml:"ignore-data-check" json:"ignore-data-check"`

	// set true will continue check from the latest checkpoint
	UseCheckpoint bool `toml:"use-checkpoint" json:"use-checkpoint"`

	// DMAddr is dm-master's address, the format should like "http://127.0.0.1:8261"
	DMAddr string `toml:"dm-addr" json:"dm-addr"`
	// DMTask is dm's task name
	DMTask string `toml:"dm-task" json:"dm-task"`

	// config file
	ConfigFile string

	// print version if set true
	PrintVersion bool
}

// NewConfig creates a new config.
func NewDiffConfig() *DiffConfig {
	cfg := &DiffConfig{}
	cfg.FlagSet = flag.NewFlagSet("diff", flag.ContinueOnError)
	fs := cfg.FlagSet

	fs.StringVar(&cfg.ConfigFile, "config", "", "Config file")
	fs.StringVar(&cfg.LogLevel, "L", "info", "log level: debug, info, warn, error, fatal")
	fs.IntVar(&cfg.ChunkSize, "chunk-size", 1000, "diff check chunk size")
	fs.IntVar(&cfg.Sample, "sample", 100, "the percent of sampling check")
	fs.IntVar(&cfg.CheckThreadCount, "check-thread-count", 1, "how many goroutines are created to check data")
	fs.BoolVar(&cfg.UseChecksum, "use-checksum", true, "set false if want to comapre the data directly")
	fs.StringVar(&cfg.FixSQLFile, "fix-sql-file", "fix.sql", "the name of the file which saves sqls used to fix different data")
	fs.BoolVar(&cfg.PrintVersion, "V", false, "print version of sync_diff_inspector")
	fs.BoolVar(&cfg.IgnoreDataCheck, "ignore-data-check", false, "ignore check table's data")
	fs.BoolVar(&cfg.IgnoreStructCheck, "ignore-struct-check", false, "ignore check table's struct")
	fs.BoolVar(&cfg.UseCheckpoint, "use-checkpoint", true, "set true will continue check from the latest checkpoint")

	return cfg
}

// Parse parses flag definitions from the argument list.
func (c *DiffConfig) Parse(arguments []string) error {
	// Parse first to get config file.
	err := c.FlagSet.Parse(arguments)
	if err != nil {
		return errors.Trace(err)
	}

	// Load config file if specified.
	if c.ConfigFile != "" {
		err = c.configFromFile(c.ConfigFile)
		if err != nil {
			return errors.Trace(err)
		}
	}

	// Parse again to replace with command line options.
	err = c.FlagSet.Parse(arguments)
	if err != nil {
		return errors.Trace(err)
	}

	if len(c.FlagSet.Args()) != 0 {
		return errors.Errorf("'%s' is an invalid flag", c.FlagSet.Arg(0))
	}

	return nil
}

func (c *DiffConfig) String() string {
	cfg, err := json.Marshal(c)
	if err != nil {
		return "<nil>"
	}
	return string(cfg)
}

// configFromFile loads config from file.
func (c *DiffConfig) configFromFile(path string) error {
	_, err := toml.DecodeFile(path, c)
	return errors.Trace(err)
}

func (c *DiffConfig) configToFile(path string) error {
	_, err := toml.DecodeFile(path, c)
	return errors.Trace(err)
}

func (c *DiffConfig) checkConfig() bool {
	if c.Sample > percent100 || c.Sample < percent0 {
		log.Error("sample must be greater than 0 and less than or equal to 100!")
		return false
	}

	if c.CheckThreadCount <= 0 {
		log.Error("check-thcount must greater than 0!")
		return false
	}

	if len(c.DMAddr) != 0 {
		u, err := url.Parse(c.DMAddr)
		if err != nil || u.Scheme == "" || u.Host == "" {
			log.Error("dm-addr's format should like 'http://127.0.0.1:8261'")
			return false
		}

		if len(c.DMTask) == 0 {
			log.Error("must set the `dm-task` if set `dm-addr`")
			return false
		}

		emptyDBConfig := DiffDBConfig{}
		// source DB, target DB and check table's information will get from DM, should not set them
		if len(c.SourceDBCfg) != 0 || c.TargetDBCfg != emptyDBConfig {
			log.Error("should not set `source-db` or `target-db`, diff will generate them automatically when set `dm-addr` and `dm-task`")
			return false
		}

		if len(c.Tables) != 0 || len(c.TableRules) != 0 || len(c.TableCfgs) != 0 {
			log.Error("should not set `check-tables`, `table-rules` or `table-config`, diff will generate them automatically when set `dm-addr` and `dm-task`")
			return false
		}
	} else {
		if len(c.SourceDBCfg) == 0 {
			log.Error("must have at least one source database")
			return false
		}

		for i := range c.SourceDBCfg {
			if !c.SourceDBCfg[i].Valid() {
				return false
			}
			if c.SourceDBCfg[i].Snapshot != "" {
				c.SourceDBCfg[i].Snapshot = strconv.Quote(c.SourceDBCfg[i].Snapshot)
			}
		}

		if c.TargetDBCfg.InstanceID == "" {
			c.TargetDBCfg.InstanceID = "target"
		}
		if c.TargetDBCfg.Snapshot != "" {
			c.TargetDBCfg.Snapshot = strconv.Quote(c.TargetDBCfg.Snapshot)
		}
		if _, ok := sourceInstanceMap[c.TargetDBCfg.InstanceID]; ok {
			log.Error("target has same instance id in source", zap.String("instance id", c.TargetDBCfg.InstanceID))
			return false
		}

		if len(c.Tables) == 0 {
			log.Error("must specify check tables")
			return false
		}

		for _, tableCfg := range c.TableCfgs {
			if !tableCfg.Valid() {
				return false
			}
		}
	}

	if c.OnlyUseChecksum {
		if !c.UseChecksum {
			log.Error("need set use-checksum = true")
			return false
		}
	} else {
		if len(c.FixSQLFile) == 0 {
			log.Warn("fix-sql-file is invalid, will use default value 'fix.sql'")
			c.FixSQLFile = "fix.sql"
		}
	}

	return true
}
