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

package config

import (
	"os"

	"github.com/pingcap/errors"
	bf "github.com/pingcap/tidb-tools/pkg/binlog-filter"
	"github.com/pingcap/tidb-tools/pkg/column-mapping"
	"github.com/pingcap/tidb/util/filter"
	router "github.com/pingcap/tidb/util/table-router"
	dmconfig "github.com/pingcap/tiflow/dm/dm/config"
	"gopkg.in/yaml.v2"
)

// JobCfg copies from tiflow/dm/config/config.go and removes some deprecated fields.
type JobCfg struct {
	Name                string                                `yaml:"name" toml:"name" json:"name"`
	TaskMode            string                                `yaml:"task-mode" toml:"task-mode" json:"task-mode"`
	IsSharding          bool                                  `yaml:"is-sharding" toml:"is-sharding" json:"is-sharding"`
	ShardMode           string                                `yaml:"shard-mode" toml:"shard-mode" json:"shard-mode"` // when `shard-mode` set, we always enable sharding support.
	IgnoreCheckingItems []string                              `yaml:"ignore-checking-items" toml:"ignore-checking-items" json:"ignore-checking-items"`
	Timezone            string                                `yaml:"timezone" toml:"timezone" json:"timezone"`
	CaseSensitive       bool                                  `yaml:"case-sensitive" toml:"case-sensitive" json:"case-sensitive"`
	CollationCompatible string                                `yaml:"collation_compatible" toml:"collation_compatible" json:"collation_compatible"`
	TargetDB            *dmconfig.DBConfig                    `yaml:"target-database" toml:"target-database" json:"target-database"`
	ShadowTableRules    []string                              `yaml:"shadow-table-rules" toml:"shadow-table-rules" json:"shadow-table-rules"`
	TrashTableRules     []string                              `yaml:"trash-table-rules" toml:"trash-table-rules" json:"trash-table-rules"`
	Filters             map[string]*bf.BinlogEventRule        `yaml:"filters" toml:"filters" json:"filters"`
	ExprFilter          map[string]*dmconfig.ExpressionFilter `yaml:"expression-filter" toml:"expression-filter" json:"expression-filter"`
	BAList              map[string]*filter.Rules              `yaml:"block-allow-list" toml:"block-allow-list" json:"block-allow-list"`
	Mydumpers           map[string]*dmconfig.MydumperConfig   `yaml:"mydumpers" toml:"mydumpers" json:"mydumpers"`
	Loaders             map[string]*dmconfig.LoaderConfig     `yaml:"loaders" toml:"loaders" json:"loaders"`
	Syncers             map[string]*dmconfig.SyncerConfig     `yaml:"syncers" toml:"syncers" json:"syncers"`
	Routes              map[string]*router.TableRule          `yaml:"routes" toml:"routes" json:"routes"`

	// remove source config, use db config instead.
	Upstreams []*UpstreamCfg `yaml:"upstreams" toml:"upstreams" json:"upstreams"`

	// no need experimental features?
	Experimental struct {
		AsyncCheckpointFlush bool `yaml:"async-checkpoint-flush" toml:"async-checkpoint-flush" json:"async-checkpoint-flush"`
	} `yaml:"experimental" toml:"experimental" json:"experimental"`

	// remove them later
	MetaSchema     string                  `yaml:"meta-schema" toml:"meta-schema" json:"meta-schema"`
	OnlineDDL      bool                    `yaml:"online-ddl" toml:"online-ddl" json:"online-ddl"`
	ColumnMappings map[string]*column.Rule `yaml:"column-mappings" toml:"column-mappings" json:"column-mappings"`

	// removed
	// CleanDumpFile  bool                    `yaml:"clean-dump-file" toml:"clean-dump-file" json:"clean-dump-file"`

	// should use a extra job?
	// Validators map[string]*dmconfig.ValidatorConfig `yaml:"validators" toml:"validators" json:"validators"`

	// deprecated
	// EnableHeartbeat bool `yaml:"enable-heartbeat" toml:"enable-heartbeat" json:"enable-heartbeat"`
	// HeartbeatUpdateInterval int `yaml:"heartbeat-update-interval" toml:"heartbeat-update-interval" json:"heartbeat-update-interval"`
	// HeartbeatReportInterval int    `yaml:"heartbeat-report-interval" toml:"heartbeat-report-interval" json:"heartbeat-report-interval"`
	// pt/gh-ost name rule,support regex
	// OnlineDDLScheme string `yaml:"online-ddl-scheme" toml:"online-ddl-scheme" json:"online-ddl-scheme"`
	// BWList map[string]*filter.Rules `yaml:"black-white-list" toml:"black-white-list" json:"black-white-list"`
	// EnableANSIQuotes bool `yaml:"ansi-quotes" toml:"ansi-quotes" json:"ansi-quotes"`
	// RemoveMeta bool `yaml:"remove-meta"`
}

// The difference between task configuration and job configuration is that a task has only one usptream.
type TaskCfg JobCfg

// UpstreamCfg add db-config to dmconfig.MySQLInstance, because we no need source cfg now.
type UpstreamCfg struct {
	dmconfig.MySQLInstance `yaml:",inline" toml:",inline" json:",inline"`
	DBCfg                  *dmconfig.DBConfig `yaml:"db-config" toml:"db-config" json:"db-config"`
}

func (c *JobCfg) DecodeFile(fpath string) error {
	bs, err := os.ReadFile(fpath)
	if err != nil {
		return errors.Trace(err)
	}

	if err = yaml.UnmarshalStrict(bs, c); err != nil {
		return err
	}
	return c.adjust()
}

// TODO: unify config type
// Now, dmJobmaster use yaml, dmWorker use toml, and lib use json...
func (c *JobCfg) Decode(content []byte) error {
	if err := yaml.UnmarshalStrict(content, c); err != nil {
		return err
	}
	return c.adjust()
}

// copy from tiflow/dm/config/config.go#adjust
// should be refactor
func (c *JobCfg) adjust() error {
	for _, upstream := range c.Upstreams {
		if len(upstream.MydumperConfigName) > 0 {
			rule, ok := c.Mydumpers[upstream.MydumperConfigName]
			if !ok {
				return errors.Errorf("mydumper config %s not exist in mydumpers", upstream.MydumperConfigName)
			}
			upstream.Mydumper = new(dmconfig.MydumperConfig)
			*upstream.Mydumper = *rule
		}
		if len(upstream.LoaderConfigName) > 0 {
			rule, ok := c.Loaders[upstream.LoaderConfigName]
			if !ok {
				return errors.Errorf("loader config %s not exist in loaders", upstream.LoaderConfigName)
			}
			upstream.Loader = new(dmconfig.LoaderConfig)
			*upstream.Loader = *rule // ref loader config
		}
		if len(upstream.SyncerConfigName) > 0 {
			rule, ok := c.Syncers[upstream.SyncerConfigName]
			if !ok {
				return errors.Errorf("syncer config %s not exist in syncers", upstream.SyncerConfigName)
			}
			upstream.Syncer = new(dmconfig.SyncerConfig)
			*upstream.Syncer = *rule // ref syncer config
		}
	}

	return nil
}

func (c *JobCfg) Yaml() (string, error) {
	b, err := yaml.Marshal(c)
	return string(b), err
}

func (c *JobCfg) Clone() (*JobCfg, error) {
	content, err := c.Yaml()
	if err != nil {
		return nil, err
	}
	clone := &JobCfg{}
	err = yaml.Unmarshal([]byte(content), clone)
	return clone, err
}

func (c *JobCfg) ToTaskConfigs() map[string]*TaskCfg {
	taskCfgs := make(map[string]*TaskCfg, len(c.Upstreams))
	for _, mysqlInstance := range c.Upstreams {
		// nolint:errcheck
		jobCfg, _ := c.Clone()
		jobCfg.Upstreams = []*UpstreamCfg{mysqlInstance}

		taskCfg := (*TaskCfg)(jobCfg)
		taskCfgs[mysqlInstance.SourceID] = taskCfg
	}
	return taskCfgs
}

// ToDMSubtaskCfg adapts a TaskCfg to a SubTaskCfg for worker now.
// TODO: fully support all fields
func (c *TaskCfg) ToDMSubTaskCfg() *dmconfig.SubTaskConfig {
	cfg := &dmconfig.SubTaskConfig{}
	cfg.IsSharding = c.IsSharding
	cfg.ShardMode = c.ShardMode
	cfg.OnlineDDL = c.OnlineDDL
	cfg.ShadowTableRules = c.ShadowTableRules
	cfg.TrashTableRules = c.TrashTableRules
	cfg.CaseSensitive = c.CaseSensitive
	cfg.CollationCompatible = c.CollationCompatible
	cfg.Name = c.Name
	cfg.Mode = c.TaskMode
	cfg.IgnoreCheckingItems = c.IgnoreCheckingItems
	cfg.MetaSchema = c.MetaSchema
	cfg.Timezone = c.Timezone
	cfg.Meta = c.Upstreams[0].Meta
	cfg.From = *c.Upstreams[0].DBCfg
	cfg.To = *c.TargetDB
	cfg.Experimental = c.Experimental
	cfg.CollationCompatible = c.CollationCompatible
	cfg.SourceID = c.Upstreams[0].SourceID
	cfg.BAList = c.BAList[c.Upstreams[0].BAListName]

	cfg.RouteRules = make([]*router.TableRule, len(c.Upstreams[0].RouteRules))
	for j, name := range c.Upstreams[0].RouteRules {
		cfg.RouteRules[j] = c.Routes[name]
	}

	cfg.FilterRules = make([]*bf.BinlogEventRule, len(c.Upstreams[0].FilterRules))
	for j, name := range c.Upstreams[0].FilterRules {
		cfg.FilterRules[j] = c.Filters[name]
	}

	cfg.ColumnMappingRules = make([]*column.Rule, len(c.Upstreams[0].ColumnMappingRules))
	for j, name := range c.Upstreams[0].ColumnMappingRules {
		cfg.ColumnMappingRules[j] = c.ColumnMappings[name]
	}

	cfg.ExprFilter = make([]*dmconfig.ExpressionFilter, len(c.Upstreams[0].ExpressionFilters))
	for j, name := range c.Upstreams[0].ExpressionFilters {
		cfg.ExprFilter[j] = c.ExprFilter[name]
	}

	cfg.MydumperConfig = *c.Upstreams[0].Mydumper
	cfg.LoaderConfig = *c.Upstreams[0].Loader
	cfg.SyncerConfig = *c.Upstreams[0].Syncer

	return cfg
}
