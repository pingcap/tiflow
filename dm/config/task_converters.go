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
	"fmt"
	"strings"

	bf "github.com/pingcap/tidb-tools/pkg/binlog-filter"
	"github.com/pingcap/tidb-tools/pkg/column-mapping"
	"github.com/pingcap/tidb/pkg/util/filter"
	router "github.com/pingcap/tidb/pkg/util/table-router"
	"github.com/pingcap/tiflow/dm/config/dbconfig"
	"github.com/pingcap/tiflow/dm/config/security"
	"github.com/pingcap/tiflow/dm/openapi"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/storage"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"go.uber.org/zap"
)

// TaskConfigToSubTaskConfigs generates sub task configs by TaskConfig.
func TaskConfigToSubTaskConfigs(c *TaskConfig, sources map[string]dbconfig.DBConfig) ([]*SubTaskConfig, error) {
	cfgs := make([]*SubTaskConfig, len(c.MySQLInstances))
	for i, inst := range c.MySQLInstances {
		dbCfg, exist := sources[inst.SourceID]
		if !exist {
			return nil, terror.ErrConfigSourceIDNotFound.Generate(inst.SourceID)
		}

		cfg := NewSubTaskConfig()
		cfg.IsSharding = c.IsSharding
		cfg.ShardMode = c.ShardMode
		cfg.StrictOptimisticShardMode = c.StrictOptimisticShardMode
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
		cfg.Timezone = c.Timezone
		cfg.Meta = inst.Meta
		cfg.CollationCompatible = c.CollationCompatible
		cfg.Experimental = c.Experimental

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
		cfg.ValidatorCfg = inst.ContinuousValidator

		cfg.CleanDumpFile = c.CleanDumpFile

		if err := cfg.Adjust(true); err != nil {
			return nil, terror.Annotatef(err, "source %s", inst.SourceID)
		}
		cfgs[i] = cfg
	}
	if c.EnableHeartbeat {
		log.L().Warn("DM 2.0 does not support heartbeat feature, will overwrite it to false")
	}
	return cfgs, nil
}

// OpenAPITaskToSubTaskConfigs generates sub task configs by openapi.Task.
func OpenAPITaskToSubTaskConfigs(task *openapi.Task, toDBCfg *dbconfig.DBConfig, sourceCfgMap map[string]*SourceConfig) (
	[]*SubTaskConfig, error,
) {
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
	subTaskCfgList := make([]*SubTaskConfig, len(task.SourceConfig.SourceConf))
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
		subTaskCfg.MetaSchema = *task.MetaSchema
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

		// if there is no meta for incremental task, we print a warning log
		if subTaskCfg.Meta == nil && subTaskCfg.Mode == ModeIncrement {
			log.L().Warn("mysql-instance doesn't set meta for incremental mode, user should specify start_time to start task.", zap.String("sourceID", sourceCfg.SourceName))
		}

		// set shard config
		if task.ShardMode != nil {
			subTaskCfg.IsSharding = true
			mode := *task.ShardMode
			subTaskCfg.ShardMode = string(mode)
		} else {
			subTaskCfg.IsSharding = false
		}
		if task.StrictOptimisticShardMode != nil {
			subTaskCfg.StrictOptimisticShardMode = *task.StrictOptimisticShardMode
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
		// TODO ExprFilter
		// set full unit config
		subTaskCfg.MydumperConfig = DefaultMydumperConfig()
		subTaskCfg.LoaderConfig = DefaultLoaderConfig()
		if fullCfg := task.SourceConfig.FullMigrateConf; fullCfg != nil {
			if fullCfg.Analyze != nil {
				subTaskCfg.LoaderConfig.Analyze = PhysicalPostOpLevel(*fullCfg.Analyze)
			}
			if fullCfg.Checksum != nil {
				subTaskCfg.LoaderConfig.ChecksumPhysical = PhysicalPostOpLevel(*fullCfg.Checksum)
			}
			if fullCfg.CompressKvPairs != nil {
				subTaskCfg.CompressKVPairs = *fullCfg.CompressKvPairs
			}
			if fullCfg.Consistency != nil {
				subTaskCfg.MydumperConfig.ExtraArgs = fmt.Sprintf("--consistency %s", *fullCfg.Consistency)
			}
			if fullCfg.ExportThreads != nil {
				subTaskCfg.MydumperConfig.Threads = *fullCfg.ExportThreads
			}
			if fullCfg.ImportThreads != nil {
				subTaskCfg.LoaderConfig.PoolSize = *fullCfg.ImportThreads
			}
			if fullCfg.DataDir != nil {
				subTaskCfg.LoaderConfig.Dir = *fullCfg.DataDir
			}
			if fullCfg.DiskQuota != nil {
				if err := subTaskCfg.LoaderConfig.DiskQuotaPhysical.UnmarshalText([]byte(*fullCfg.DiskQuota)); err != nil {
					return nil, err
				}
			}
			if fullCfg.ImportMode != nil {
				subTaskCfg.LoaderConfig.ImportMode = LoadMode(*fullCfg.ImportMode)
			}
			if fullCfg.OnDuplicateLogical != nil {
				subTaskCfg.LoaderConfig.OnDuplicateLogical = LogicalDuplicateResolveType(*fullCfg.OnDuplicateLogical)
			}
			if fullCfg.OnDuplicatePhysical != nil {
				subTaskCfg.LoaderConfig.OnDuplicatePhysical = PhysicalDuplicateResolveType(*fullCfg.OnDuplicatePhysical)
			}
			if fullCfg.PdAddr != nil {
				subTaskCfg.LoaderConfig.PDAddr = *fullCfg.PdAddr
			}
			if fullCfg.RangeConcurrency != nil {
				subTaskCfg.LoaderConfig.RangeConcurrency = *fullCfg.RangeConcurrency
			}
			if fullCfg.SortingDir != nil {
				subTaskCfg.LoaderConfig.SortingDirPhysical = *fullCfg.SortingDir
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
		subTaskCfg.ValidatorCfg = defaultValidatorConfig()
		// set route,blockAllowList,filter config
		doDBs := []string{}
		doTables := []*filter.Table{}
		routeRules := []*router.TableRule{}
		filterRules := []*bf.BinlogEventRule{}
		for _, rule := range tableMigrateRuleMap[sourceCfg.SourceName] {
			// route
			if rule.Target != nil && (rule.Target.Schema != nil || rule.Target.Table != nil) {
				tableRule := &router.TableRule{SchemaPattern: rule.Source.Schema, TablePattern: rule.Source.Table}
				if rule.Target.Schema != nil {
					tableRule.TargetSchema = *rule.Target.Schema
				}
				if rule.Target.Table != nil {
					tableRule.TargetTable = *rule.Target.Table
				}
				routeRules = append(routeRules, tableRule)
			}
			// filter
			if rule.BinlogFilterRule != nil {
				for _, name := range *rule.BinlogFilterRule {
					filterRule, ok := eventFilterTemplateMap[name] // NOTE: this return a copied value
					if !ok {
						return nil, terror.ErrOpenAPICommonError.Generatef("filter rule name %s not found.", name)
					}
					filterRule.SchemaPattern = rule.Source.Schema
					if rule.Source.Table != "" {
						filterRule.TablePattern = rule.Source.Table
					}
					filterRules = append(filterRules, &filterRule)
				}
			}
			// BlockAllowList
			if rule.Source.Table != "" {
				doTables = append(doTables, &filter.Table{Schema: rule.Source.Schema, Name: rule.Source.Table})
			} else {
				doDBs = append(doDBs, rule.Source.Schema)
			}
		}
		subTaskCfg.RouteRules = routeRules
		subTaskCfg.FilterRules = filterRules
		if len(doDBs) > 0 || len(doTables) > 0 {
			bAList := &filter.Rules{}
			if len(doDBs) > 0 {
				bAList.DoDBs = removeDuplication(doDBs)
			}
			if len(doTables) > 0 {
				bAList.DoTables = doTables
			}
			subTaskCfg.BAList = bAList
		}
		if task.IgnoreCheckingItems != nil && len(*task.IgnoreCheckingItems) != 0 {
			subTaskCfg.IgnoreCheckingItems = *task.IgnoreCheckingItems
		}
		// adjust sub task config
		if err := subTaskCfg.Adjust(true); err != nil {
			return nil, terror.Annotatef(err, "source name %s", sourceCfg.SourceName)
		}
		subTaskCfgList[i] = subTaskCfg
	}
	return subTaskCfgList, nil
}

// GetTargetDBCfgFromOpenAPITask gets target db config.
func GetTargetDBCfgFromOpenAPITask(task *openapi.Task) *dbconfig.DBConfig {
	toDBCfg := &dbconfig.DBConfig{
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
		toDBCfg.Security = &security.Security{
			SSLCABytes:    []byte(task.TargetConfig.Security.SslCaContent),
			SSLKeyBytes:   []byte(task.TargetConfig.Security.SslKeyContent),
			SSLCertBytes:  []byte(task.TargetConfig.Security.SslCertContent),
			CertAllowedCN: certAllowedCN,
		}
	}
	return toDBCfg
}

// SubTaskConfigsToTaskConfig constructs task configs from a list of valid subtask configs.
func SubTaskConfigsToTaskConfig(stCfgs ...*SubTaskConfig) *TaskConfig {
	c := &TaskConfig{}
	// global configs.
	stCfg0 := stCfgs[0]
	c.Name = stCfg0.Name
	c.TaskMode = stCfg0.Mode
	c.IsSharding = stCfg0.IsSharding
	c.ShardMode = stCfg0.ShardMode
	c.StrictOptimisticShardMode = stCfg0.StrictOptimisticShardMode
	c.IgnoreCheckingItems = stCfg0.IgnoreCheckingItems
	c.MetaSchema = stCfg0.MetaSchema
	c.EnableHeartbeat = stCfg0.EnableHeartbeat
	c.HeartbeatUpdateInterval = stCfg0.HeartbeatUpdateInterval
	c.HeartbeatReportInterval = stCfg0.HeartbeatReportInterval
	c.Timezone = stCfg0.Timezone
	c.CaseSensitive = stCfg0.CaseSensitive
	c.TargetDB = &stCfg0.To // just ref
	c.OnlineDDL = stCfg0.OnlineDDL
	c.OnlineDDLScheme = stCfg0.OnlineDDLScheme
	c.CleanDumpFile = stCfg0.CleanDumpFile
	c.CollationCompatible = stCfg0.CollationCompatible
	c.MySQLInstances = make([]*MySQLInstance, 0, len(stCfgs))
	c.BAList = make(map[string]*filter.Rules)
	c.Routes = make(map[string]*router.TableRule)
	c.Filters = make(map[string]*bf.BinlogEventRule)
	c.ColumnMappings = make(map[string]*column.Rule)
	c.Mydumpers = make(map[string]*MydumperConfig)
	c.Loaders = make(map[string]*LoaderConfig)
	c.Syncers = make(map[string]*SyncerConfig)
	c.ExprFilter = make(map[string]*ExpressionFilter)
	c.Experimental = stCfg0.Experimental
	c.Validators = make(map[string]*ValidatorConfig)

	baListMap := make(map[string]string, len(stCfgs))
	routeMap := make(map[string]string, len(stCfgs))
	filterMap := make(map[string]string, len(stCfgs))
	dumpMap := make(map[string]string, len(stCfgs))
	loadMap := make(map[string]string, len(stCfgs))
	syncMap := make(map[string]string, len(stCfgs))
	cmMap := make(map[string]string, len(stCfgs))
	exprFilterMap := make(map[string]string, len(stCfgs))
	validatorMap := make(map[string]string, len(stCfgs))
	var baListIdx, routeIdx, filterIdx, dumpIdx, loadIdx, syncIdx, validateIdx, cmIdx, efIdx int
	var baListName, routeName, filterName, dumpName, loadName, syncName, validateName, cmName, efName string

	// NOTE:
	// - we choose to ref global configs for instances now.
	for _, stCfg := range stCfgs {
		baListName, baListIdx = getGenerateName(stCfg.BAList, baListIdx, "balist", baListMap)
		c.BAList[baListName] = stCfg.BAList

		routeNames := make([]string, 0, len(stCfg.RouteRules))
		for _, rule := range stCfg.RouteRules {
			routeName, routeIdx = getGenerateName(rule, routeIdx, "route", routeMap)
			routeNames = append(routeNames, routeName)
			c.Routes[routeName] = rule
		}

		filterNames := make([]string, 0, len(stCfg.FilterRules))
		for _, rule := range stCfg.FilterRules {
			filterName, filterIdx = getGenerateName(rule, filterIdx, "filter", filterMap)
			filterNames = append(filterNames, filterName)
			c.Filters[filterName] = rule
		}

		dumpName, dumpIdx = getGenerateName(stCfg.MydumperConfig, dumpIdx, "dump", dumpMap)
		c.Mydumpers[dumpName] = &stCfg.MydumperConfig

		loadName, loadIdx = getGenerateName(stCfg.LoaderConfig, loadIdx, "load", loadMap)
		loaderCfg := stCfg.LoaderConfig

		var dirSuffix string
		var err error
		if storage.IsS3Path(loaderCfg.Dir) {
			// we will dump files to s3 dir's subdirectory
			dirSuffix = "/" + c.Name + "." + stCfg.SourceID
		} else {
			// TODO we will dump local file to dir's subdirectory, but it may have risk of compatibility, we will fix in other pr
			dirSuffix = "." + c.Name
		}
		// if ends with the task name, we remove to get user input dir.
		loaderCfg.Dir, err = storage.TrimPath(loaderCfg.Dir, dirSuffix)
		// because dir comes form subtask, there should not have error.
		if err != nil {
			log.L().Warn("parse config comes from subtask error.", zap.Error(err))
		}

		c.Loaders[loadName] = &loaderCfg

		syncName, syncIdx = getGenerateName(stCfg.SyncerConfig, syncIdx, "sync", syncMap)
		c.Syncers[syncName] = &stCfg.SyncerConfig

		exprFilterNames := make([]string, 0, len(stCfg.ExprFilter))
		for _, f := range stCfg.ExprFilter {
			efName, efIdx = getGenerateName(f, efIdx, "expr-filter", exprFilterMap)
			exprFilterNames = append(exprFilterNames, efName)
			c.ExprFilter[efName] = f
		}

		validateName, validateIdx = getGenerateName(stCfg.ValidatorCfg, validateIdx, "validator", validatorMap)
		c.Validators[validateName] = &stCfg.ValidatorCfg

		cmNames := make([]string, 0, len(stCfg.ColumnMappingRules))
		for _, rule := range stCfg.ColumnMappingRules {
			cmName, cmIdx = getGenerateName(rule, cmIdx, "cm", cmMap)
			cmNames = append(cmNames, cmName)
			c.ColumnMappings[cmName] = rule
		}

		c.MySQLInstances = append(c.MySQLInstances, &MySQLInstance{
			SourceID:                      stCfg.SourceID,
			Meta:                          stCfg.Meta,
			FilterRules:                   filterNames,
			ColumnMappingRules:            cmNames,
			RouteRules:                    routeNames,
			BAListName:                    baListName,
			MydumperConfigName:            dumpName,
			LoaderConfigName:              loadName,
			SyncerConfigName:              syncName,
			ExpressionFilters:             exprFilterNames,
			ContinuousValidatorConfigName: validateName,
		})
	}
	if c.CollationCompatible == "" {
		c.CollationCompatible = LooseCollationCompatible
	}
	return c
}

// SubTaskConfigsToOpenAPITaskList gets openapi task from sub task configs.
// subTaskConfigMap: taskName -> sourceName -> SubTaskConfig.
func SubTaskConfigsToOpenAPITaskList(subTaskConfigMap map[string]map[string]*SubTaskConfig) []*openapi.Task {
	taskList := []*openapi.Task{}
	for _, subTaskConfigM := range subTaskConfigMap {
		subTaskConfigList := make([]*SubTaskConfig, 0, len(subTaskConfigM))
		for sourceName := range subTaskConfigM {
			subTaskConfigList = append(subTaskConfigList, subTaskConfigM[sourceName])
		}
		taskList = append(taskList, SubTaskConfigsToOpenAPITask(subTaskConfigList))
	}
	return taskList
}

// SubTaskConfigsToOpenAPITask gets openapi task from sub task configs.
func SubTaskConfigsToOpenAPITask(subTaskConfigList []*SubTaskConfig) *openapi.Task {
	oneSubtaskConfig := subTaskConfigList[0] // need this to get target db config
	taskSourceConfig := openapi.TaskSourceConfig{}
	sourceConfList := []openapi.TaskSourceConf{}
	// source name -> filter rule list
	filterMap := make(map[string][]*bf.BinlogEventRule)
	// source name -> route rule list
	routeMap := make(map[string][]*router.TableRule)

	for _, cfg := range subTaskConfigList {
		sourceName := cfg.SourceID
		oneConf := openapi.TaskSourceConf{
			SourceName: cfg.SourceID,
		}
		if meta := cfg.Meta; meta != nil {
			oneConf.BinlogGtid = &meta.BinLogGTID
			oneConf.BinlogName = &meta.BinLogName
			pos := int(meta.BinLogPos)
			oneConf.BinlogPos = &pos
		}
		sourceConfList = append(sourceConfList, oneConf)
		if len(cfg.FilterRules) > 0 {
			filterMap[sourceName] = cfg.FilterRules
		}
		if len(cfg.RouteRules) > 0 {
			routeMap[sourceName] = cfg.RouteRules
		}
	}
	taskSourceConfig.SourceConf = sourceConfList

	var dirSuffix string
	var err error
	if storage.IsS3Path(oneSubtaskConfig.LoaderConfig.Dir) {
		// we will dump files to s3 dir's subdirectory
		dirSuffix = "/" + oneSubtaskConfig.Name + "." + oneSubtaskConfig.SourceID
	} else {
		// TODO we will dump local file to dir's subdirectory, but it may have risk of compatibility, we will fix in other pr
		dirSuffix = "." + oneSubtaskConfig.Name
	}
	// if ends with the task name, we remove to get user input dir.
	oneSubtaskConfig.LoaderConfig.Dir, err = storage.TrimPath(oneSubtaskConfig.LoaderConfig.Dir, dirSuffix)
	// because dir comes form subtask, there should not have error.
	if err != nil {
		log.L().Warn("parse config comes from subtask error.", zap.Error(err))
	}

	taskSourceConfig.FullMigrateConf = &openapi.TaskFullMigrateConf{
		ExportThreads: &oneSubtaskConfig.MydumperConfig.Threads,
		DataDir:       &oneSubtaskConfig.LoaderConfig.Dir,
		ImportThreads: &oneSubtaskConfig.LoaderConfig.PoolSize,
	}
	consistencyInTask := oneSubtaskConfig.MydumperConfig.ExtraArgs
	consistency := strings.Replace(consistencyInTask, "--consistency ", "", 1)
	if consistency != "" {
		taskSourceConfig.FullMigrateConf.Consistency = &consistency
	}
	taskSourceConfig.IncrMigrateConf = &openapi.TaskIncrMigrateConf{
		ReplBatch:   &oneSubtaskConfig.SyncerConfig.Batch,
		ReplThreads: &oneSubtaskConfig.SyncerConfig.WorkerCount,
	}
	// set filter rules
	filterRuleMap := openapi.Task_BinlogFilterRule{}
	for sourceName, ruleList := range filterMap {
		for idx, rule := range ruleList {
			binlogFilterRule := openapi.TaskBinLogFilterRule{}
			var events []string
			for _, event := range rule.Events {
				events = append(events, string(event))
			}
			if len(events) > 0 {
				binlogFilterRule.IgnoreEvent = &events
			}
			var ignoreSQL []string
			ignoreSQL = append(ignoreSQL, rule.SQLPattern...)
			if len(ignoreSQL) > 0 {
				binlogFilterRule.IgnoreSql = &ignoreSQL
			}
			filterRuleMap.Set(genFilterRuleName(sourceName, idx), binlogFilterRule)
		}
	}
	// set table migrate rules
	tableMigrateRuleList := []openapi.TaskTableMigrateRule{}
	// used to remove repeated rules
	ruleMap := map[string]struct{}{}
	appendOneRule := func(sourceName, schemaPattern, tablePattern, targetSchema, targetTable string) {
		tableMigrateRule := openapi.TaskTableMigrateRule{
			Source: struct {
				Schema     string `json:"schema"`
				SourceName string `json:"source_name"`
				Table      string `json:"table"`
			}{
				Schema:     schemaPattern,
				SourceName: sourceName,
				Table:      tablePattern,
			},
		}
		if targetSchema != "" {
			tableMigrateRule.Target = &struct {
				Schema *string `json:"schema,omitempty"`
				Table  *string `json:"table,omitempty"`
			}{
				Schema: &targetSchema,
			}
			if targetTable != "" {
				tableMigrateRule.Target.Table = &targetTable
			}
		}
		if filterRuleList, ok := filterMap[sourceName]; ok {
			ruleNameList := make([]string, len(filterRuleList))
			for idx := range filterRuleList {
				ruleNameList[idx] = genFilterRuleName(sourceName, idx)
			}
			tableMigrateRule.BinlogFilterRule = &ruleNameList
		}
		ruleKey := strings.Join([]string{sourceName, schemaPattern, tablePattern}, "-")
		if _, ok := ruleMap[ruleKey]; ok {
			return
		}
		ruleMap[ruleKey] = struct{}{}
		tableMigrateRuleList = append(tableMigrateRuleList, tableMigrateRule)
	}
	// gen migrate rules by route
	for sourceName, ruleList := range routeMap {
		for _, rule := range ruleList {
			appendOneRule(sourceName, rule.SchemaPattern, rule.TablePattern, rule.TargetSchema, rule.TargetTable)
		}
	}

	// gen migrate rules by BAList
	for _, cfg := range subTaskConfigList {
		if cfg.BAList != nil {
			for idx := range cfg.BAList.DoDBs {
				schemaPattern := cfg.BAList.DoDBs[idx]
				appendOneRule(cfg.SourceID, schemaPattern, "", "", "")
			}
			for idx := range cfg.BAList.DoTables {
				schemaPattern := cfg.BAList.DoTables[idx].Schema
				tablePattern := cfg.BAList.DoTables[idx].Name
				appendOneRule(cfg.SourceID, schemaPattern, tablePattern, "", "")
			}
		}
	}

	// set basic global config
	task := openapi.Task{
		Name:                      oneSubtaskConfig.Name,
		TaskMode:                  openapi.TaskTaskMode(oneSubtaskConfig.Mode),
		EnhanceOnlineSchemaChange: oneSubtaskConfig.OnlineDDL,
		MetaSchema:                &oneSubtaskConfig.MetaSchema,
		OnDuplicate:               openapi.TaskOnDuplicate(oneSubtaskConfig.LoaderConfig.OnDuplicateLogical),
		SourceConfig:              taskSourceConfig,
		TargetConfig: openapi.TaskTargetDataBase{
			Host:     oneSubtaskConfig.To.Host,
			Port:     oneSubtaskConfig.To.Port,
			User:     oneSubtaskConfig.To.User,
			Password: oneSubtaskConfig.To.Password,
		},
	}
	if oneSubtaskConfig.ShardMode != "" {
		taskShardMode := openapi.TaskShardMode(oneSubtaskConfig.ShardMode)
		task.ShardMode = &taskShardMode
	}
	task.StrictOptimisticShardMode = &oneSubtaskConfig.StrictOptimisticShardMode
	if len(filterMap) > 0 {
		task.BinlogFilterRule = &filterRuleMap
	}
	task.TableMigrateRule = tableMigrateRuleList
	if len(oneSubtaskConfig.IgnoreCheckingItems) != 0 {
		ignoreItems := oneSubtaskConfig.IgnoreCheckingItems
		task.IgnoreCheckingItems = &ignoreItems
	}
	return &task
}

// TaskConfigToOpenAPITask converts TaskConfig to an openapi task.
func TaskConfigToOpenAPITask(c *TaskConfig, sourceCfgMap map[string]*SourceConfig) (*openapi.Task, error) {
	cfgs := make(map[string]dbconfig.DBConfig)
	for _, source := range c.MySQLInstances {
		if cfg, ok := sourceCfgMap[source.SourceID]; ok {
			cfgs[source.SourceID] = cfg.From
		}
	}

	// different sources can have different configurations in TaskConfig
	// but currently OpenAPI formatted tasks do not support this
	// user submitted TaskConfig will only set the configuration in TaskConfig.Syncers
	// but setting this will not affect the configuration in TaskConfig.MySQLInstances
	// so it needs to be handled in a special way
	for _, cfg := range c.MySQLInstances {
		if cfg.Mydumper != nil {
			cfg.Mydumper = c.Mydumpers[cfg.MydumperConfigName]
		}
		if cfg.Loader != nil {
			cfg.Loader = c.Loaders[cfg.LoaderConfigName]
		}
		if cfg.Syncer != nil {
			cfg.Syncer = c.Syncers[cfg.SyncerConfigName]
		}
	}
	SubTaskConfigList, err := TaskConfigToSubTaskConfigs(c, cfgs)
	if err != nil {
		return nil, err
	}

	task := SubTaskConfigsToOpenAPITask(SubTaskConfigList)
	if err := task.Adjust(); err != nil {
		return nil, err
	}
	return task, nil
}

// OpenAPITaskToTaskConfig converts an openapi task to TaskConfig.
func OpenAPITaskToTaskConfig(task *openapi.Task, sourceCfgMap map[string]*SourceConfig) (*TaskConfig, error) {
	toDBCfg := GetTargetDBCfgFromOpenAPITask(task)
	subTaskConfigList, err := OpenAPITaskToSubTaskConfigs(task, toDBCfg, sourceCfgMap)
	if err != nil {
		return nil, err
	}
	cfg := SubTaskConfigsToTaskConfig(subTaskConfigList...)
	if err := cfg.Adjust(); err != nil {
		return nil, err
	}
	return cfg, nil
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

func genFilterRuleName(sourceName string, idx int) string {
	// NOTE that we don't have user input filter rule name in sub task config, so we make one by ourself
	return fmt.Sprintf("%s-filter-rule-%d", sourceName, idx)
}

func OpenAPIStartTaskReqToTaskCliArgs(req openapi.StartTaskRequest) (*TaskCliArgs, error) {
	if req.StartTime == nil && req.SafeModeTimeDuration == nil {
		return nil, nil
	}
	cliArgs := &TaskCliArgs{}
	if req.StartTime != nil {
		cliArgs.StartTime = *req.StartTime
	}
	if req.SafeModeTimeDuration != nil {
		cliArgs.SafeModeDuration = *req.SafeModeTimeDuration
	}

	if err := cliArgs.Verify(); err != nil {
		return nil, err
	}
	return cliArgs, nil
}

func OpenAPIStopTaskReqToTaskCliArgs(req openapi.StopTaskRequest) (*TaskCliArgs, error) {
	if req.TimeoutDuration == nil {
		return nil, nil
	}
	cliArgs := &TaskCliArgs{
		WaitTimeOnStop: *req.TimeoutDuration,
	}
	if err := cliArgs.Verify(); err != nil {
		return nil, err
	}
	return cliArgs, nil
}
