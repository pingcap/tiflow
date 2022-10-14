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

package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/BurntSushi/toml"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/errors"
	bf "github.com/pingcap/tidb-tools/pkg/binlog-filter"
	"github.com/pingcap/tidb/util/filter"
	router "github.com/pingcap/tidb/util/table-router"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/pkg/version"
)

// commonConfig collects common item for both new config and old config.
type commonConfig struct {
	*flag.FlagSet `json:"-"`

	// task name
	Name         string
	printVersion bool
	ConfigFile   string
	ServerID     int
	Flavor       string
	WorkerCount  int
	Batch        int
	StatusAddr   string
	Meta         string

	LogLevel  string
	LogFile   string
	LogFormat string
	LogRotate string

	EnableGTID bool
	SafeMode   bool
	MaxRetry   int

	// deprecated
	TimezoneStr string

	SyncerConfigFormat bool
}

func (c *commonConfig) newConfigFromSyncerConfig(args []string) (*config.SubTaskConfig, error) {
	cfg := &syncerConfig{
		printVersion: c.printVersion,
		ConfigFile:   c.ConfigFile,
		ServerID:     c.ServerID,
		Flavor:       c.Flavor,
		WorkerCount:  c.WorkerCount,
		Batch:        c.Batch,
		StatusAddr:   c.StatusAddr,
		Meta:         c.Meta,
		LogLevel:     c.LogLevel,
		LogFile:      c.LogFile,
		LogFormat:    c.LogFormat,
		LogRotate:    c.LogRotate,
		EnableGTID:   c.EnableGTID,
		SafeMode:     c.SafeMode,
		MaxRetry:     c.MaxRetry,
	}

	cfg.FlagSet = flag.NewFlagSet("dm-syncer", flag.ContinueOnError)
	fs := cfg.FlagSet

	var SyncerConfigFormat bool
	var timezoneStr string

	fs.BoolVar(&cfg.printVersion, "V", false, "prints version and exit")
	fs.StringVar(&cfg.Name, "name", "", "the task name")
	fs.StringVar(&cfg.ConfigFile, "config", "", "path to config file")
	fs.IntVar(&cfg.ServerID, "server-id", 101, "MySQL slave server ID")
	fs.StringVar(&cfg.Flavor, "flavor", mysql.MySQLFlavor, "use flavor for different MySQL source versions; support \"mysql\", \"mariadb\" now; if you replicate from mariadb, please set it to \"mariadb\"")
	fs.IntVar(&cfg.WorkerCount, "c", 16, "parallel worker count")
	fs.IntVar(&cfg.Batch, "b", 100, "batch commit count")
	fs.StringVar(&cfg.StatusAddr, "status-addr", ":8271", "status addr")
	fs.StringVar(&cfg.Meta, "meta", "syncer.meta", "syncer meta info")
	// fs.StringVar(&cfg.PersistentTableDir, "persistent-dir", "", "syncer history table structures persistent dir; set to non-empty string will choosing history table structure according to column length when constructing DML")
	fs.StringVar(&cfg.LogLevel, "L", "info", "log level: debug, info, warn, error, fatal")
	fs.StringVar(&cfg.LogFile, "log-file", "", "log file path")
	fs.StringVar(&cfg.LogFormat, "log-format", "text", `the format of the log, "text" or "json"`)
	fs.StringVar(&cfg.LogRotate, "log-rotate", "day", "log file rotate type, hour/day")
	fs.BoolVar(&cfg.EnableGTID, "enable-gtid", false, "enable gtid mode")
	fs.BoolVar(&cfg.SafeMode, "safe-mode", false, "enable safe mode to make syncer reentrant")
	fs.IntVar(&cfg.MaxRetry, "max-retry", 100, "maxinum retry when network interruption")
	fs.StringVar(&timezoneStr, "timezone", "", "target database timezone location string")
	fs.BoolVar(&SyncerConfigFormat, "syncer-config-format", false, "read syncer config format")

	if err := fs.Parse(args); err != nil {
		return nil, errors.Trace(err)
	}

	if cfg.ConfigFile != "" {
		_, err := toml.DecodeFile(cfg.ConfigFile, cfg)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	if err := fs.Parse(args); err != nil {
		return nil, errors.Trace(err)
	}

	if timezoneStr != "" {
		log.L().Warn("'--timezone' is deprecated, needn't set it anymore.")
	}

	return cfg.convertToNewFormat()
}

func (c *commonConfig) parse(args []string) (*config.SubTaskConfig, error) {
	if err := c.FlagSet.Parse(args); err != nil {
		return nil, errors.Trace(err)
	}
	if c.printVersion {
		fmt.Println(version.GetRawInfo())
		return nil, flag.ErrHelp
	}

	if c.SyncerConfigFormat {
		return c.newConfigFromSyncerConfig(args)
	}

	return c.newSubTaskConfig(args)
}

func (c *commonConfig) newSubTaskConfig(args []string) (*config.SubTaskConfig, error) {
	cfg := &config.SubTaskConfig{}
	cfg.SetFlagSet(flag.NewFlagSet("dm-syncer", flag.ContinueOnError))
	fs := cfg.GetFlagSet()

	var syncerConfigFormat bool
	var printVersion bool
	var serverID uint
	var timezoneStr string

	fs.BoolVar(&printVersion, "V", false, "prints version and exit")
	fs.StringVar(&cfg.Name, "name", "", "the task name")
	fs.StringVar(&cfg.ConfigFile, "config", "", "path to config file")
	fs.UintVar(&serverID, "server-id", 101, "MySQL slave server ID")
	fs.StringVar(&cfg.Flavor, "flavor", mysql.MySQLFlavor, "use flavor for different MySQL source versions; support \"mysql\", \"mariadb\" now; if you replicate from mariadb, please set it to \"mariadb\"")
	fs.IntVar(&cfg.WorkerCount, "c", 16, "parallel worker count")
	fs.IntVar(&cfg.Batch, "b", 100, "batch commit count")
	fs.StringVar(&cfg.StatusAddr, "status-addr", ":8271", "status addr")
	// fs.StringVar(&cfg.PersistentTableDir, "persistent-dir", "", "syncer history table structures persistent dir; set to non-empty string will choosing history table structure according to column length when constructing DML")
	fs.StringVar(&cfg.LogLevel, "L", "info", "log level: debug, info, warn, error, fatal")
	fs.StringVar(&cfg.LogFile, "log-file", "", "log file path")
	fs.StringVar(&cfg.LogFormat, "log-format", "text", `the format of the log, "text" or "json"`)
	fs.StringVar(&cfg.LogRotate, "log-rotate", "day", "log file rotate type, hour/day")
	fs.BoolVar(&cfg.EnableGTID, "enable-gtid", false, "enable gtid mode")
	fs.BoolVar(&cfg.SafeMode, "safe-mode", false, "enable safe mode to make syncer reentrant")
	fs.IntVar(&cfg.MaxRetry, "max-retry", 100, "maxinum retry when network interruption")
	fs.StringVar(&timezoneStr, "timezone", "", "target database timezone location string")
	fs.StringVar(&cfg.Name, "cp-table-prefix", "dm-syncer", "the prefix of the checkpoint table name")
	fs.BoolVar(&syncerConfigFormat, "syncer-config-format", false, "read syncer config format")

	cfg.ServerID = uint32(serverID)

	if err := cfg.Parse(args, false); err != nil {
		return nil, errors.Trace(err)
	}
	if timezoneStr != "" {
		log.L().Warn("'--timezone' is deprecated, needn't set it anymore.")
	}

	if serverID != 101 {
		cfg.ServerID = uint32(serverID)
	}

	return cfg, nil
}

func newCommonConfig() *commonConfig {
	cfg := &commonConfig{}
	cfg.FlagSet = flag.NewFlagSet("dm-syncer", flag.ContinueOnError)
	fs := cfg.FlagSet

	fs.BoolVar(&cfg.printVersion, "V", false, "prints version and exit")
	fs.StringVar(&cfg.Name, "name", "", "the task name")
	fs.StringVar(&cfg.ConfigFile, "config", "", "path to config file")
	fs.IntVar(&cfg.ServerID, "server-id", 101, "MySQL slave server ID")
	fs.StringVar(&cfg.Flavor, "flavor", mysql.MySQLFlavor, "use flavor for different MySQL source versions; support \"mysql\", \"mariadb\" now; if you replicate from mariadb, please set it to \"mariadb\"")
	fs.IntVar(&cfg.WorkerCount, "c", 16, "parallel worker count")
	fs.IntVar(&cfg.Batch, "b", 100, "batch commit count")
	fs.StringVar(&cfg.StatusAddr, "status-addr", ":8271", "status addr")
	fs.StringVar(&cfg.Meta, "meta", "syncer.meta", "syncer meta info")
	// fs.StringVar(&cfg.PersistentTableDir, "persistent-dir", "", "syncer history table structures persistent dir; set to non-empty string will choosing history table structure according to column length when constructing DML")
	fs.StringVar(&cfg.LogLevel, "L", "info", "log level: debug, info, warn, error, fatal")
	fs.StringVar(&cfg.LogFile, "log-file", "", "log file path")
	fs.StringVar(&cfg.LogFormat, "log-format", "text", `the format of the log, "text" or "json"`)
	fs.StringVar(&cfg.LogRotate, "log-rotate", "day", "log file rotate type, hour/day")
	fs.BoolVar(&cfg.EnableGTID, "enable-gtid", false, "enable gtid mode")
	fs.BoolVar(&cfg.SafeMode, "safe-mode", false, "enable safe mode to make syncer reentrant")
	fs.IntVar(&cfg.MaxRetry, "max-retry", 100, "maxinum retry when network interruption")
	fs.BoolVar(&cfg.SyncerConfigFormat, "syncer-config-format", false, "read syncer config format")

	return cfg
}

// syncerConfig is the format of syncer tools, eventually it will be converted to new SubTaskConfig format.
type syncerConfig struct {
	*flag.FlagSet `json:"-"`

	Name      string `toml:"name" json:"name"`
	LogLevel  string `toml:"log-level" json:"log-level"`
	LogFile   string `toml:"log-file" json:"log-file"`
	LogFormat string `toml:"log-format" json:"log-format"`
	LogRotate string `toml:"log-rotate" json:"log-rotate"`

	StatusAddr string `toml:"status-addr" json:"status-addr"`

	ServerID int    `toml:"server-id" json:"server-id"`
	Meta     string `toml:"meta" json:"meta"`
	// NOTE: This item is deprecated.
	PersistentTableDir string `toml:"persistent-dir" json:"persistent-dir"`
	Flavor             string `toml:"flavor" json:"flavor"`

	WorkerCount int `toml:"worker-count" json:"worker-count"`
	Batch       int `toml:"batch" json:"batch"`
	MaxRetry    int `toml:"max-retry" json:"max-retry"`

	// Ref: http://dev.mysql.com/doc/refman/5.7/en/replication-options-slave.html#option_mysqld_replicate-do-table
	DoTables []*filter.Table `toml:"replicate-do-table" json:"replicate-do-table"`
	DoDBs    []string        `toml:"replicate-do-db" json:"replicate-do-db"`

	// Ref: http://dev.mysql.com/doc/refman/5.7/en/replication-options-slave.html#option_mysqld_replicate-ignore-db
	IgnoreTables []*filter.Table `toml:"replicate-ignore-table" json:"replicate-ignore-table"`
	IgnoreDBs    []string        `toml:"replicate-ignore-db" json:"replicate-ignore-db"`

	SkipDDLs []string `toml:"skip-ddls" json:"skip-ddls"`
	// NOTE: SkipSQL and SkipEvents are no longer used, leave the comments to remind others.
	// SkipSQLs is deprecated, please use SkipDDLs instead.
	// SkipSQLs []string `toml:"skip-sqls" json:"-"` // omit it since it's deprecated
	// SkipEvents is deprecated, please use SkipDMLs instead.
	// SkipEvents []string   `toml:"skip-events" json:"-"` // omit it since it's deprecated
	SkipDMLs []*SkipDML `toml:"skip-dmls" json:"skip-dmls"`

	RouteRules []*RouteRule `toml:"route-rules" json:"route-rules"`

	From config.DBConfig `toml:"from" json:"from"`
	To   config.DBConfig `toml:"to" json:"to"`

	EnableGTID  bool `toml:"enable-gtid" json:"enable-gtid"`
	AutoFixGTID bool `toml:"auto-fix-gtid" json:"auto-fix-gtid"`

	SafeMode   bool   `toml:"safe-mode" json:"safe-mode"`
	ConfigFile string `json:"config-file"`

	// NOTE: These four configs are all deprecated.
	// We leave this items as comments to remind others there WERE old config items.
	// stopOnDDL               bool   `toml:"stop-on-ddl" json:"stop-on-ddl"`
	// MaxDDLConnectionTimeout string `toml:"execute-ddl-timeout" json:"execute-ddl-timeout"`
	// MaxDMLConnectionTimeout string `toml:"execute-dml-timeout" json:"execute-dml-timeout"`
	// ExecutionQueueLength    int    `toml:"execute-queue-length" json:"execute-queue-length"`

	TimezoneStr string `toml:"timezone" json:"timezone"`

	printVersion bool
}

// RouteRule is route rule that syncing
// schema/table to specified schema/table
// This config has been replaced by `router.TableRule`.
type RouteRule struct {
	PatternSchema string `toml:"pattern-schema" json:"pattern-schema"`
	PatternTable  string `toml:"pattern-table" json:"pattern-table"`
	TargetSchema  string `toml:"target-schema" json:"target-schema"`
	TargetTable   string `toml:"target-table" json:"target-table"`
}

// SkipDML defines config rule of skipping dml.
// This config has been replaced by BinLog filter.
type SkipDML struct {
	Schema string `toml:"db-name" json:"db-name"`
	Table  string `toml:"tbl-name" json:"tbl-name"`
	Type   string `toml:"type" json:"type"`
}

func loadMetaFile(metaFile string) (*config.Meta, error) {
	file, err := os.Open(metaFile)
	if err != nil {
		return nil, errors.Trace(err)
	}

	defer file.Close()

	meta := &config.Meta{}
	_, err = toml.DecodeReader(file, meta)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return meta, nil
}

func (oc *syncerConfig) convertToNewFormat() (*config.SubTaskConfig, error) {
	meta, err := loadMetaFile(oc.Meta)
	if err != nil {
		return nil, errors.Trace(err)
	}
	newTask := &config.SubTaskConfig{
		Name:     oc.Name,
		SourceID: fmt.Sprintf("%s_source", oc.Name),
		Mode:     config.ModeIncrement,
		Meta:     meta,

		LogLevel:  oc.LogLevel,
		LogFile:   oc.LogFile,
		LogFormat: oc.LogFormat,
		LogRotate: oc.LogRotate,

		StatusAddr: oc.StatusAddr,
		ServerID:   uint32(oc.ServerID),
		Flavor:     oc.Flavor,

		SyncerConfig: config.SyncerConfig{
			WorkerCount: oc.WorkerCount,
			Batch:       oc.Batch,
			EnableGTID:  oc.EnableGTID,
			SafeMode:    oc.SafeMode,
		},

		BAList: &filter.Rules{
			DoTables:     oc.DoTables,
			DoDBs:        oc.DoDBs,
			IgnoreTables: oc.IgnoreTables,
			IgnoreDBs:    oc.IgnoreDBs,
		},

		ConfigFile: oc.ConfigFile,
		From:       oc.From,
		To:         oc.To,
	}

	for _, rule := range oc.RouteRules {
		newTask.RouteRules = append(newTask.RouteRules, &router.TableRule{
			SchemaPattern: rule.PatternSchema,
			TablePattern:  rule.PatternTable,
			TargetSchema:  rule.TargetSchema,
			TargetTable:   rule.TargetTable,
		})
	}

	newTask.FilterRules, err = generateBinlogEventRule(oc.SkipDDLs, oc.SkipDMLs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	err = newTask.Adjust(false)
	return newTask, err
}

func generateBinlogEventRule(skipDDLs []string, skipDMLs []*SkipDML) ([]*bf.BinlogEventRule, error) {
	result := make([]*bf.BinlogEventRule, 0, 1+len(skipDMLs))
	ddlEvents := &bf.BinlogEventRule{}
	for _, skipDDL := range skipDDLs {
		if tp, _ := bf.ClassifyEvent(bf.EventType(skipDDL)); tp != "ddl" {
			return nil, errors.NotValidf("event type %s", skipDDL)
		}
		ddlEvents.SQLPattern = append(ddlEvents.SQLPattern, skipDDL)
	}
	for _, skipDML := range skipDMLs {
		if tp, _ := bf.ClassifyEvent(bf.EventType(skipDML.Type)); tp != "dml" {
			return nil, errors.NotValidf("event type %s", skipDML.Type)
		}
		found := false
		for _, evt := range result {
			if evt.SchemaPattern == skipDML.Schema && evt.TablePattern == skipDML.Table {
				found = true
				evt.Events = append(evt.Events, bf.EventType(skipDML.Type))
				break
			}
		}
		if !found {
			result = append(result, &bf.BinlogEventRule{
				SchemaPattern: skipDML.Schema,
				TablePattern:  skipDML.Table,
				Events:        []bf.EventType{bf.EventType(skipDML.Type)},
			})
		}
	}
	return result, nil
}
