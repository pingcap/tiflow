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

package config

import (
	bf "github.com/pingcap/tidb-tools/pkg/binlog-filter"
	"github.com/pingcap/tidb-tools/pkg/column-mapping"
	"github.com/pingcap/tidb-tools/pkg/filter"
	router "github.com/pingcap/tidb-tools/pkg/table-router"

	"github.com/pingcap/dm/openapi"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
)

// TaskConfigToSubTaskConfigs generates sub task configs by TaskConfig.
func TaskConfigToSubTaskConfigs(c *TaskConfig, sources map[string]DBConfig) ([]*SubTaskConfig, error) {
	cfgs := make([]*SubTaskConfig, len(c.MySQLInstances))
	for i, inst := range c.MySQLInstances {
		dbCfg, exist := sources[inst.SourceID]
		if !exist {
			return nil, terror.ErrConfigSourceIDNotFound.Generate(inst.SourceID)
		}

		cfg := NewSubTaskConfig()
		cfg.IsSharding = c.IsSharding
		cfg.ShardMode = c.ShardMode
		cfg.OnlineDDL = c.OnlineDDL
		cfg.TrashTableRules = c.TrashTableRules
		cfg.ShadowTableRules = c.ShadowTableRules
		cfg.IgnoreCheckingItems = c.IgnoreCheckingItems
		cfg.Name = c.Name
		cfg.Mode = c.TaskMode
		cfg.CaseSensitive = c.CaseSensitive
		cfg.MetaSchema = c.MetaSchema
		cfg.EnableHeartbeat = false
		cfg.HeartbeatUpdateInterval = c.HeartbeatUpdateInterval
		cfg.HeartbeatReportInterval = c.HeartbeatReportInterval
		cfg.Meta = inst.Meta

		fromClone := dbCfg.Clone()
		if fromClone == nil {
			return nil, terror.ErrConfigMySQLInstNotFound
		}
		cfg.From = *fromClone
		toClone := c.TargetDB.Clone()
		if toClone == nil {
			return nil, terror.ErrConfigNeedTargetDB
		}
		cfg.To = *toClone

		cfg.SourceID = inst.SourceID

		cfg.RouteRules = make([]*router.TableRule, len(inst.RouteRules))
		for j, name := range inst.RouteRules {
			cfg.RouteRules[j] = c.Routes[name]
		}

		cfg.FilterRules = make([]*bf.BinlogEventRule, len(inst.FilterRules))
		for j, name := range inst.FilterRules {
			cfg.FilterRules[j] = c.Filters[name]
		}

		cfg.ColumnMappingRules = make([]*column.Rule, len(inst.ColumnMappingRules))
		for j, name := range inst.ColumnMappingRules {
			cfg.ColumnMappingRules[j] = c.ColumnMappings[name]
		}

		cfg.ExprFilter = make([]*ExpressionFilter, len(inst.ExpressionFilters))
		for j, name := range inst.ExpressionFilters {
			cfg.ExprFilter[j] = c.ExprFilter[name]
		}

		cfg.BAList = c.BAList[inst.BAListName]

		cfg.MydumperConfig = *inst.Mydumper
		cfg.LoaderConfig = *inst.Loader
		cfg.SyncerConfig = *inst.Syncer

		cfg.CleanDumpFile = c.CleanDumpFile

		if err := cfg.Adjust(true); err != nil {
			return nil, terror.Annotatef(err, "source %s", inst.SourceID)
		}
		if c.TiDB != nil {
			cfg.TiDB = *c.TiDB
		}
		cfgs[i] = cfg
	}
	if c.EnableHeartbeat {
		log.L().Warn("DM 2.0 does not support heartbeat feature, will overwrite it to false")
	}
	return cfgs, nil
}

// OpenAPITaskToSubTaskConfigs generates sub task configs by openapi.Task.
func OpenAPITaskToSubTaskConfigs(task *openapi.Task, toDBCfg *DBConfig, sourceCfgMap map[string]*SourceConfig) (
	[]SubTaskConfig, error) {
	// source name -> migrate rule list
	tableMigrateRuleMap := make(map[string][]openapi.TaskTableMigrateRule)
	for _, rule := range task.TableMigrateRule {
		tableMigrateRuleMap[rule.Source.SourceName] = append(tableMigrateRuleMap[rule.Source.SourceName], rule)
	}
	// rule name -> rule template
	eventFilterTemplateMap := make(map[string]bf.BinlogEventRule)
	if task.BinlogFilterRule != nil {
		for ruleName, rule := range task.BinlogFilterRule.AdditionalProperties {
			ruleT := bf.BinlogEventRule{Action: bf.Ignore}
			if rule.IgnoreEvent != nil {
				events := make([]bf.EventType, len(*rule.IgnoreEvent))
				for i, eventStr := range *rule.IgnoreEvent {
					events[i] = bf.EventType(eventStr)
				}
				ruleT.Events = events
			}
			if rule.IgnoreSql != nil {
				ruleT.SQLPattern = *rule.IgnoreSql
			}
			eventFilterTemplateMap[ruleName] = ruleT
		}
	}
	// start to generate sub task configs
	subTaskCfgList := make([]SubTaskConfig, len(task.SourceConfig.SourceConf))
	for i, sourceCfg := range task.SourceConfig.SourceConf {
		// precheck source config
		_, exist := sourceCfgMap[sourceCfg.SourceName]
		if !exist {
			return nil, terror.ErrConfigSourceIDNotFound.Generate(sourceCfg.SourceName)
		}
		subTaskCfg := NewSubTaskConfig()
		// set task name and mode
		subTaskCfg.Name = task.Name
		subTaskCfg.Mode = string(task.TaskMode)
		// set task meta
		subTaskCfg.MetaFile = *task.MetaSchema
		// add binlog meta
		if sourceCfg.BinlogGtid != nil || sourceCfg.BinlogName != nil || sourceCfg.BinlogPos != nil {
			meta := &Meta{}
			if sourceCfg.BinlogGtid != nil {
				meta.BinLogGTID = *sourceCfg.BinlogGtid
			}
			if sourceCfg.BinlogName != nil {
				meta.BinLogName = *sourceCfg.BinlogName
			}
			if sourceCfg.BinlogPos != nil {
				pos := uint32(*sourceCfg.BinlogPos)
				meta.BinLogPos = pos
			}
			subTaskCfg.Meta = meta
		}
		// set shard config
		if task.ShardMode != nil {
			subTaskCfg.IsSharding = true
			mode := *task.ShardMode
			subTaskCfg.ShardMode = string(mode)
		} else {
			subTaskCfg.IsSharding = false
		}
		// set online ddl plugin config
		subTaskCfg.OnlineDDL = task.EnhanceOnlineSchemaChange
		// set case sensitive from source
		subTaskCfg.CaseSensitive = sourceCfgMap[sourceCfg.SourceName].CaseSensitive
		// set source db config
		subTaskCfg.SourceID = sourceCfg.SourceName
		subTaskCfg.From = sourceCfgMap[sourceCfg.SourceName].From
		// set target db config
		subTaskCfg.To = *toDBCfg.Clone()
		// TODO set meet error policy
		// TODO ExprFilter
		// set full unit config
		subTaskCfg.MydumperConfig = DefaultMydumperConfig()
		subTaskCfg.LoaderConfig = DefaultLoaderConfig()
		if fullCfg := task.SourceConfig.FullMigrateConf; fullCfg != nil {
			if fullCfg.ExportThreads != nil {
				subTaskCfg.MydumperConfig.Threads = *fullCfg.ExportThreads
			}
			if fullCfg.ImportThreads != nil {
				subTaskCfg.LoaderConfig.PoolSize = *fullCfg.ImportThreads
			}
			if fullCfg.DataDir != nil {
				subTaskCfg.LoaderConfig.Dir = *fullCfg.DataDir
			}
		}
		// set incremental config
		subTaskCfg.SyncerConfig = DefaultSyncerConfig()
		if incrCfg := task.SourceConfig.IncrMigrateConf; incrCfg != nil {
			if incrCfg.ReplThreads != nil {
				subTaskCfg.SyncerConfig.WorkerCount = *incrCfg.ReplThreads
			}
			if incrCfg.ReplBatch != nil {
				subTaskCfg.SyncerConfig.Batch = *incrCfg.ReplBatch
			}
		}
		// set route,blockAllowList,filter config
		doCnt := len(tableMigrateRuleMap[sourceCfg.SourceName])
		doDBs := make([]string, doCnt)
		doTables := make([]*filter.Table, doCnt)

		routeRules := []*router.TableRule{}
		filterRules := []*bf.BinlogEventRule{}
		for j, rule := range tableMigrateRuleMap[sourceCfg.SourceName] {
			// route
			if rule.Target != nil {
				routeRules = append(routeRules, &router.TableRule{
					SchemaPattern: rule.Source.Schema, TablePattern: rule.Source.Table,
					TargetSchema: rule.Target.Schema, TargetTable: rule.Target.Table,
				})
			}
			// filter
			if rule.BinlogFilterRule != nil {
				for _, name := range *rule.BinlogFilterRule {
					filterRule, ok := eventFilterTemplateMap[name] // NOTE: this return a copied value
					if !ok {
						return nil, terror.ErrOpenAPICommonError.Generatef("filter rule name %s not found.", name)
					}
					filterRule.SchemaPattern = rule.Source.Schema
					filterRule.TablePattern = rule.Source.Table
					filterRules = append(filterRules, &filterRule)
				}
			}
			// BlockAllowList
			doDBs[j] = rule.Source.Schema
			doTables[j] = &filter.Table{Schema: rule.Source.Schema, Name: rule.Source.Table}
		}
		subTaskCfg.RouteRules = routeRules
		subTaskCfg.FilterRules = filterRules
		subTaskCfg.BAList = &filter.Rules{DoDBs: removeDuplication(doDBs), DoTables: doTables}
		// adjust sub task config
		if err := subTaskCfg.Adjust(true); err != nil {
			return nil, terror.Annotatef(err, "source name %s", sourceCfg.SourceName)
		}
		subTaskCfgList[i] = *subTaskCfg
	}
	return subTaskCfgList, nil
}

// GetTargetDBCfgFromOpenAPITask gets target db config.
func GetTargetDBCfgFromOpenAPITask(task *openapi.Task) *DBConfig {
	toDBCfg := &DBConfig{
		Host:     task.TargetConfig.Host,
		Port:     task.TargetConfig.Port,
		User:     task.TargetConfig.User,
		Password: task.TargetConfig.Password,
	}
	if task.TargetConfig.Security != nil {
		var certAllowedCN []string
		if task.TargetConfig.Security.CertAllowedCn != nil {
			certAllowedCN = *task.TargetConfig.Security.CertAllowedCn
		}
		toDBCfg.Security = &Security{
			SSLCABytes:    []byte(task.TargetConfig.Security.SslCaContent),
			SSLKEYBytes:   []byte(task.TargetConfig.Security.SslKeyContent),
			SSLCertBytes:  []byte(task.TargetConfig.Security.SslCertContent),
			CertAllowedCN: certAllowedCN,
		}
	}
	return toDBCfg
}

func removeDuplication(in []string) []string {
	m := make(map[string]struct{}, len(in))
	j := 0
	for _, v := range in {
		_, ok := m[v]
		if ok {
			continue
		}
		m[v] = struct{}{}
		in[j] = v
		j++
	}
	return in[:j]
}
