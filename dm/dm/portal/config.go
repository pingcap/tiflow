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

package portal

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path"

	"github.com/pingcap/errors"
	bf "github.com/pingcap/tidb-tools/pkg/binlog-filter"
	"github.com/pingcap/tidb-tools/pkg/filter"
	router "github.com/pingcap/tidb-tools/pkg/table-router"
	"go.uber.org/zap"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/utils"
)

// Config is the dm-portal's config.
type Config struct {
	*flag.FlagSet `json:"-"`

	// Port is the port for server to listen
	Port int

	// TaskFilePath is the path used to save generated task config file
	TaskFilePath string

	// Timeout is the timeout for connect database and query, unit: second
	Timeout int

	printVersion bool
}

// NewConfig creates a new config.
func NewConfig() *Config {
	cfg := &Config{}
	cfg.FlagSet = flag.NewFlagSet("dm-portal", flag.ContinueOnError)
	fs := cfg.FlagSet

	fs.IntVar(&cfg.Port, "port", 8280, "the port for server to listen")
	fs.StringVar(&cfg.TaskFilePath, "task-file-path", "/tmp/", "the path used to save generated task config file")
	fs.IntVar(&cfg.Timeout, "timeout", 5, "the timeout for connect database and query, unit: second")
	fs.BoolVar(&cfg.printVersion, "V", false, "prints version and exit")

	return cfg
}

// Parse parses the cmd config.
func (cfg *Config) Parse(arguments []string) {
	// Parse first to get config file
	perr := cfg.FlagSet.Parse(arguments)
	switch perr {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		os.Exit(2)
	}

	if cfg.printVersion {
		fmt.Println(utils.GetRawInfo())
		os.Exit(0)
	}
}

// Valid checks whether the config is valid.
func (cfg *Config) Valid() error {
	if cfg.Port < 1 || cfg.Port > 65535 {
		return errors.Errorf("port %d is out of range [1, 65535]", cfg.Port)
	}

	if len(cfg.TaskFilePath) == 0 {
		return errors.New("task-file-path is empty")
	}

	if cfg.Timeout <= 0 {
		return errors.New("timeout is less or equal to 0")
	}

	// try create a test file under cfg.TaskFilePath to test the Permission
	filepath := path.Join(cfg.TaskFilePath, "dm-portal-test-task-300555926.yaml")
	f, err := os.Create(filepath)
	if err != nil {
		return errors.Annotatef(err, "can't generate task config file under %s", cfg.TaskFilePath)
	}
	defer func() {
		f.Close()
		os.Remove(filepath)
	}()

	return nil
}

// String return config's string.
func (cfg *Config) String() string {
	return fmt.Sprintf("dm-portal config: { port: %d, task-file-path: %s }", cfg.Port, cfg.TaskFilePath)
}

// DMTaskConfig is the dm task's config used for dm-portal.
type DMTaskConfig struct {
	Name string `yaml:"name" json:"name"`

	TaskMode string `yaml:"task-mode" json:"task-mode"`

	IsSharding bool `yaml:"is-sharding" json:"is-sharding"`

	TargetDB *DBConfig `yaml:"target-database" json:"target-database"`

	MySQLInstances []*MySQLInstance `yaml:"mysql-instances" json:"mysql-instances"`

	Routes  map[string]*router.TableRule   `yaml:"routes" json:"routes"`
	Filters map[string]*bf.BinlogEventRule `yaml:"filters" json:"filters"`
	BAList  map[string]*filter.Rules       `yaml:"block-allow-list" json:"block-allow-list"`

	Mydumpers map[string]*config.MydumperConfig `yaml:"mydumpers" json:"mydumpers"`
}

func (d *DMTaskConfig) String() string {
	cfgBytes, err := json.Marshal(d)
	if err != nil {
		log.L().Error("marshal config failed",
			zap.Error(err))
	}

	return string(cfgBytes)
}

// Verify does verification on DMTaskConfig.
func (d *DMTaskConfig) Verify() error {
	if len(d.Name) == 0 {
		return errors.New("task name should not be empty")
	}

	switch d.TaskMode {
	case "":
		return errors.New("task mode should not be empty")
	case config.ModeAll, config.ModeFull, config.ModeIncrement:
	default:
		return errors.New("task mode should be 'all', 'full' or 'incremental'")
	}

	for _, mysqlInstance := range d.MySQLInstances {
		if err := mysqlInstance.Verify(); err != nil {
			return err
		}
	}

	return nil
}

// MySQLInstance represents a sync config of a MySQL instance.
type MySQLInstance struct {
	// it represents a MySQL/MariaDB instance or a replica group
	SourceID string `yaml:"source-id" json:"source-id"`
	Meta     *Meta  `yaml:"meta" json:"meta"`

	FilterRules []string `yaml:"filter-rules" json:"filter-rules"`
	RouteRules  []string `yaml:"route-rules" json:"route-rules"`
	BAListName  string   `yaml:"block-allow-list" json:"block-allow-list"`

	MydumperConfigName string `yaml:"mydumper-config-name" json:"mydumper-config-name"`
}

// Verify does verification on MySQLInstance
// copied from https://github.com/pingcap/dm/blob/369933f31b8ff8246178a2b28df2cd98f88c64b9/dm/config/task.go#L94
func (m *MySQLInstance) Verify() error {
	if m == nil {
		return errors.New("mysql instance config must specify")
	}

	if m.SourceID == "" {
		return errors.NotValidf("empty source-id")
	}

	if err := m.Meta.Verify(); err != nil {
		return errors.Annotatef(err, "source %s", m.SourceID)
	}

	return nil
}

// DBConfig is the DB's config.
type DBConfig struct {
	Host string `yaml:"host" json:"host"`

	Port int `yaml:"port" json:"port"`

	User string `yaml:"user" json:"user"`

	Password string `yaml:"password" json:"password"`
}

// Meta represents binlog's meta pos
// NOTE: refine to put these config structs into pkgs
// NOTE: now, syncer does not support GTID mode and which is supported by relay.
type Meta struct {
	BinLogName string `yaml:"binlog-name" json:"binlog-name"`
	BinLogPos  uint32 `yaml:"binlog-pos" json:"binlog-pos"`
}

// Verify does verification on configs.
func (m *Meta) Verify() error {
	if m != nil && len(m.BinLogName) == 0 {
		return errors.New("binlog-name must specify")
	}

	return nil
}
