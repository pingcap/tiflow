// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.
package config

import (
	"fmt"
	"testing"

	"github.com/pingcap/check"
	bf "github.com/pingcap/tidb-tools/pkg/binlog-filter"
	"github.com/pingcap/tidb/pkg/util/filter"
	"github.com/pingcap/tiflow/dm/config/dbconfig"
	"github.com/pingcap/tiflow/dm/openapi"
	"github.com/pingcap/tiflow/dm/openapi/fixtures"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/stretchr/testify/require"
)

func (t *testConfig) TestTaskGetTargetDBCfg(c *check.C) {
	certAllowedCn := []string{"test"}
	task := &openapi.Task{
		TargetConfig: openapi.TaskTargetDataBase{
			Host:     "root",
			Password: "123456",
			Port:     4000,
			User:     "root",
			Security: &openapi.Security{CertAllowedCn: &certAllowedCn},
		},
	}
	dbCfg := GetTargetDBCfgFromOpenAPITask(task)
	c.Assert(dbCfg.Host, check.Equals, task.TargetConfig.Host)
	c.Assert(dbCfg.Password, check.Equals, task.TargetConfig.Password)
	c.Assert(dbCfg.Port, check.Equals, task.TargetConfig.Port)
	c.Assert(dbCfg.User, check.Equals, task.TargetConfig.User)
	c.Assert(dbCfg.Security, check.NotNil)
	c.Assert([]string{dbCfg.Security.CertAllowedCN[0]}, check.DeepEquals, certAllowedCn)
}

func (t *testConfig) TestOpenAPITaskToSubTaskConfigs(c *check.C) {
	testNoShardTaskToSubTaskConfigs(c)
	testShardAndFilterTaskToSubTaskConfigs(c)
}

func testNoShardTaskToSubTaskConfigs(c *check.C) {
	task, err := fixtures.GenNoShardOpenAPITaskForTest()
	c.Assert(err, check.IsNil)
	sourceCfg1, err := SourceCfgFromYamlAndVerify(SampleSourceConfig)
	c.Assert(err, check.IsNil)
	source1Name := task.SourceConfig.SourceConf[0].SourceName
	sourceCfg1.SourceID = task.SourceConfig.SourceConf[0].SourceName
	sourceCfgMap := map[string]*SourceConfig{source1Name: sourceCfg1}
	toDBCfg := &dbconfig.DBConfig{
		Host:     task.TargetConfig.Host,
		Port:     task.TargetConfig.Port,
		User:     task.TargetConfig.User,
		Password: task.TargetConfig.Password,
	}
	// change meta
	newMeta := "new_dm_meta"
	task.MetaSchema = &newMeta
	subTaskConfigList, err := OpenAPITaskToSubTaskConfigs(&task, toDBCfg, sourceCfgMap)
	c.Assert(err, check.IsNil)
	c.Assert(subTaskConfigList, check.HasLen, 1)
	subTaskConfig := subTaskConfigList[0]
	// check task name and mode
	c.Assert(subTaskConfig.Name, check.Equals, task.Name)
	// check task meta
	c.Assert(subTaskConfig.MetaSchema, check.Equals, *task.MetaSchema)
	c.Assert(subTaskConfig.Meta, check.IsNil)
	// check shard config
	c.Assert(subTaskConfig.ShardMode, check.Equals, "")
	// check online schema change
	c.Assert(subTaskConfig.OnlineDDL, check.Equals, true)
	// check case sensitive
	c.Assert(subTaskConfig.CaseSensitive, check.Equals, sourceCfg1.CaseSensitive)
	// check from
	c.Assert(subTaskConfig.From.Host, check.Equals, sourceCfg1.From.Host)
	// check to
	c.Assert(subTaskConfig.To.Host, check.Equals, toDBCfg.Host)
	// check dumpling loader syncer config
	c.Assert(subTaskConfig.MydumperConfig.Threads, check.Equals, *task.SourceConfig.FullMigrateConf.ExportThreads)
	c.Assert(subTaskConfig.LoaderConfig.Dir, check.Equals, fmt.Sprintf(
		"%s.%s", *task.SourceConfig.FullMigrateConf.DataDir, task.Name))
	c.Assert(subTaskConfig.LoaderConfig.PoolSize, check.Equals, *task.SourceConfig.FullMigrateConf.ImportThreads)
	c.Assert(subTaskConfig.SyncerConfig.WorkerCount, check.Equals, *task.SourceConfig.IncrMigrateConf.ReplThreads)
	c.Assert(subTaskConfig.SyncerConfig.Batch, check.Equals, *task.SourceConfig.IncrMigrateConf.ReplBatch)
	// check route
	c.Assert(subTaskConfig.RouteRules, check.HasLen, 1)
	rule := subTaskConfig.RouteRules[0]

	sourceSchema := task.TableMigrateRule[0].Source.Schema
	sourceTable := task.TableMigrateRule[0].Source.Table
	tartgetSchema := task.TableMigrateRule[0].Target.Schema
	tartgetTable := task.TableMigrateRule[0].Target.Table

	c.Assert(rule.SchemaPattern, check.Equals, sourceSchema)
	c.Assert(rule.TablePattern, check.Equals, sourceTable)
	c.Assert(rule.TargetSchema, check.Equals, *tartgetSchema)
	c.Assert(rule.TargetTable, check.Equals, *tartgetTable)
	// check filter
	c.Assert(subTaskConfig.FilterRules, check.HasLen, 0)
	// check balist
	c.Assert(subTaskConfig.BAList, check.NotNil)
	bAListFromOpenAPITask := &filter.Rules{
		DoTables: []*filter.Table{{Schema: sourceSchema, Name: sourceTable}},
	}
	c.Assert(subTaskConfig.BAList, check.DeepEquals, bAListFromOpenAPITask)
	// check ignore check items
	c.Assert(subTaskConfig.IgnoreCheckingItems, check.IsNil)
}

func testShardAndFilterTaskToSubTaskConfigs(c *check.C) {
	task, err := fixtures.GenShardAndFilterOpenAPITaskForTest()
	c.Assert(err, check.IsNil)
	sourceCfg1, err := SourceCfgFromYamlAndVerify(SampleSourceConfig)
	c.Assert(err, check.IsNil)
	source1Name := task.SourceConfig.SourceConf[0].SourceName
	sourceCfg1.SourceID = source1Name
	sourceCfg2, err := SourceCfgFromYamlAndVerify(SampleSourceConfig)
	c.Assert(err, check.IsNil)
	source2Name := task.SourceConfig.SourceConf[1].SourceName
	sourceCfg2.SourceID = source2Name

	toDBCfg := &dbconfig.DBConfig{
		Host:     task.TargetConfig.Host,
		Port:     task.TargetConfig.Port,
		User:     task.TargetConfig.User,
		Password: task.TargetConfig.Password,
	}
	sourceCfgMap := map[string]*SourceConfig{source1Name: sourceCfg1, source2Name: sourceCfg2}
	subTaskConfigList, err := OpenAPITaskToSubTaskConfigs(&task, toDBCfg, sourceCfgMap)
	c.Assert(err, check.IsNil)
	c.Assert(subTaskConfigList, check.HasLen, 2)

	// check sub task 1
	subTask1Config := subTaskConfigList[0]
	// check task name and mode
	c.Assert(subTask1Config.Name, check.Equals, task.Name)
	// check task meta
	c.Assert(subTask1Config.MetaSchema, check.Equals, *task.MetaSchema)
	c.Assert(subTask1Config.Meta, check.NotNil)
	c.Assert(subTask1Config.Meta.BinLogGTID, check.Equals, *task.SourceConfig.SourceConf[0].BinlogGtid)
	c.Assert(subTask1Config.Meta.BinLogName, check.Equals, *task.SourceConfig.SourceConf[0].BinlogName)
	c.Assert(subTask1Config.Meta.BinLogPos, check.Equals, uint32(*task.SourceConfig.SourceConf[0].BinlogPos))

	// check shard config
	c.Assert(subTask1Config.ShardMode, check.Equals, string(openapi.TaskShardModeOptimistic))
	c.Assert(subTask1Config.StrictOptimisticShardMode, check.IsTrue)
	// check online schema change
	c.Assert(subTask1Config.OnlineDDL, check.Equals, true)
	// check case sensitive
	c.Assert(subTask1Config.CaseSensitive, check.Equals, sourceCfg1.CaseSensitive)
	// check from
	c.Assert(subTask1Config.From.Host, check.Equals, sourceCfg1.From.Host)
	// check to
	c.Assert(subTask1Config.To.Host, check.Equals, toDBCfg.Host)
	// check dumpling loader syncer config
	c.Assert(subTask1Config.MydumperConfig.Threads, check.Equals, *task.SourceConfig.FullMigrateConf.ExportThreads)
	c.Assert(subTask1Config.LoaderConfig.Dir, check.Equals, fmt.Sprintf(
		"%s.%s", *task.SourceConfig.FullMigrateConf.DataDir, task.Name))
	c.Assert(subTask1Config.LoaderConfig.PoolSize, check.Equals, *task.SourceConfig.FullMigrateConf.ImportThreads)
	c.Assert(subTask1Config.SyncerConfig.WorkerCount, check.Equals, *task.SourceConfig.IncrMigrateConf.ReplThreads)
	c.Assert(subTask1Config.SyncerConfig.Batch, check.Equals, *task.SourceConfig.IncrMigrateConf.ReplBatch)
	// check route
	c.Assert(subTask1Config.RouteRules, check.HasLen, 1)
	rule := subTask1Config.RouteRules[0]
	source1Schema := task.TableMigrateRule[0].Source.Schema
	source1Table := task.TableMigrateRule[0].Source.Table
	tartgetSchema := task.TableMigrateRule[0].Target.Schema
	tartgetTable := task.TableMigrateRule[0].Target.Table
	c.Assert(rule.SchemaPattern, check.Equals, source1Schema)
	c.Assert(rule.TablePattern, check.Equals, source1Table)
	c.Assert(rule.TargetSchema, check.Equals, *tartgetSchema)
	c.Assert(rule.TargetTable, check.Equals, *tartgetTable)
	// check filter
	filterARule, ok := task.BinlogFilterRule.Get("filterA")
	c.Assert(ok, check.IsTrue)
	filterIgnoreEvents := *filterARule.IgnoreEvent
	c.Assert(filterIgnoreEvents, check.HasLen, 1)
	filterEvents := []bf.EventType{bf.EventType(filterIgnoreEvents[0])}
	filterRulesFromOpenAPITask := &bf.BinlogEventRule{
		Action:        bf.Ignore,
		Events:        filterEvents,
		SQLPattern:    *filterARule.IgnoreSql,
		SchemaPattern: source1Schema,
		TablePattern:  source1Table,
	}
	c.Assert(filterRulesFromOpenAPITask.Valid(), check.IsNil)
	c.Assert(subTask1Config.FilterRules, check.HasLen, 1)
	c.Assert(subTask1Config.FilterRules[0], check.DeepEquals, filterRulesFromOpenAPITask)

	// check balist
	c.Assert(subTask1Config.BAList, check.NotNil)
	bAListFromOpenAPITask := &filter.Rules{
		DoTables: []*filter.Table{{Schema: source1Schema, Name: source1Table}},
	}
	c.Assert(subTask1Config.BAList, check.DeepEquals, bAListFromOpenAPITask)
	// check ignore check items
	c.Assert(subTask1Config.IgnoreCheckingItems, check.IsNil)

	// check sub task 2
	subTask2Config := subTaskConfigList[1]
	// check task name and mode
	c.Assert(subTask2Config.Name, check.Equals, task.Name)
	// check task meta
	c.Assert(subTask2Config.MetaSchema, check.Equals, *task.MetaSchema)
	c.Assert(subTask2Config.Meta, check.NotNil)
	c.Assert(subTask2Config.Meta.BinLogGTID, check.Equals, *task.SourceConfig.SourceConf[1].BinlogGtid)
	c.Assert(subTask2Config.Meta.BinLogName, check.Equals, *task.SourceConfig.SourceConf[1].BinlogName)
	c.Assert(subTask2Config.Meta.BinLogPos, check.Equals, uint32(*task.SourceConfig.SourceConf[1].BinlogPos))
	// check shard config
	c.Assert(subTask2Config.ShardMode, check.Equals, string(openapi.TaskShardModeOptimistic))
	// check online schema change
	c.Assert(subTask2Config.OnlineDDL, check.Equals, true)
	// check case sensitive
	c.Assert(subTask2Config.CaseSensitive, check.Equals, sourceCfg2.CaseSensitive)
	// check from
	c.Assert(subTask2Config.From.Host, check.Equals, sourceCfg2.From.Host)
	// check to
	c.Assert(subTask2Config.To.Host, check.Equals, toDBCfg.Host)
	// check dumpling loader syncer config
	c.Assert(subTask2Config.MydumperConfig.Threads, check.Equals, *task.SourceConfig.FullMigrateConf.ExportThreads)
	c.Assert(subTask2Config.LoaderConfig.Dir, check.Equals, fmt.Sprintf(
		"%s.%s", *task.SourceConfig.FullMigrateConf.DataDir, task.Name))
	c.Assert(subTask2Config.LoaderConfig.PoolSize, check.Equals, *task.SourceConfig.FullMigrateConf.ImportThreads)
	c.Assert(subTask2Config.SyncerConfig.WorkerCount, check.Equals, *task.SourceConfig.IncrMigrateConf.ReplThreads)
	c.Assert(subTask2Config.SyncerConfig.Batch, check.Equals, *task.SourceConfig.IncrMigrateConf.ReplBatch)
	// check route
	c.Assert(subTask2Config.RouteRules, check.HasLen, 1)
	rule = subTask2Config.RouteRules[0]
	source2Schema := task.TableMigrateRule[1].Source.Schema
	source2Table := task.TableMigrateRule[1].Source.Table
	c.Assert(rule.SchemaPattern, check.Equals, source2Schema)
	c.Assert(rule.TablePattern, check.Equals, source2Table)
	c.Assert(rule.TargetSchema, check.Equals, *tartgetSchema)
	c.Assert(rule.TargetTable, check.Equals, *tartgetTable)
	// check filter
	_, ok = task.BinlogFilterRule.Get("filterB")
	c.Assert(ok, check.IsFalse)
	c.Assert(subTask2Config.FilterRules, check.HasLen, 0)
	// check balist
	c.Assert(subTask2Config.BAList, check.NotNil)
	bAListFromOpenAPITask = &filter.Rules{
		DoTables: []*filter.Table{{Schema: source2Schema, Name: source2Table}},
	}
	c.Assert(subTask2Config.BAList, check.DeepEquals, bAListFromOpenAPITask)
	// check ignore check items
	c.Assert(subTask2Config.IgnoreCheckingItems, check.IsNil)
}

func (t *testConfig) TestSubTaskConfigsToOpenAPITask(c *check.C) {
	testNoShardSubTaskConfigsToOpenAPITask(c)
	testShardAndFilterSubTaskConfigsToOpenAPITask(c)
}

func testNoShardSubTaskConfigsToOpenAPITask(c *check.C) {
	task, err := fixtures.GenNoShardOpenAPITaskForTest()
	c.Assert(err, check.IsNil)
	sourceCfg1, err := SourceCfgFromYamlAndVerify(SampleSourceConfig)
	c.Assert(err, check.IsNil)
	source1Name := task.SourceConfig.SourceConf[0].SourceName
	sourceCfg1.SourceID = task.SourceConfig.SourceConf[0].SourceName
	sourceCfgMap := map[string]*SourceConfig{source1Name: sourceCfg1}
	toDBCfg := &dbconfig.DBConfig{
		Host:     task.TargetConfig.Host,
		Port:     task.TargetConfig.Port,
		User:     task.TargetConfig.User,
		Password: task.TargetConfig.Password,
	}
	subTaskConfigList, err := OpenAPITaskToSubTaskConfigs(&task, toDBCfg, sourceCfgMap)
	c.Assert(err, check.IsNil)
	c.Assert(subTaskConfigList, check.HasLen, 1)

	// prepare sub task config
	subTaskConfigMap := make(map[string]map[string]*SubTaskConfig)
	subTaskConfigMap[task.Name] = make(map[string]*SubTaskConfig)
	subTaskConfigMap[task.Name][source1Name] = subTaskConfigList[0]

	taskList := SubTaskConfigsToOpenAPITaskList(subTaskConfigMap)
	c.Assert(taskList, check.HasLen, 1)
	newTask := taskList[0]
	c.Assert(&task, check.DeepEquals, newTask)
}

func testShardAndFilterSubTaskConfigsToOpenAPITask(c *check.C) {
	task, err := fixtures.GenShardAndFilterOpenAPITaskForTest()
	c.Assert(err, check.IsNil)
	sourceCfg1, err := SourceCfgFromYamlAndVerify(SampleSourceConfig)
	c.Assert(err, check.IsNil)
	source1Name := task.SourceConfig.SourceConf[0].SourceName
	sourceCfg1.SourceID = source1Name
	sourceCfg2, err := SourceCfgFromYamlAndVerify(SampleSourceConfig)
	c.Assert(err, check.IsNil)
	source2Name := task.SourceConfig.SourceConf[1].SourceName
	sourceCfg2.SourceID = source2Name

	toDBCfg := &dbconfig.DBConfig{
		Host:     task.TargetConfig.Host,
		Port:     task.TargetConfig.Port,
		User:     task.TargetConfig.User,
		Password: task.TargetConfig.Password,
	}
	sourceCfgMap := map[string]*SourceConfig{source1Name: sourceCfg1, source2Name: sourceCfg2}
	subTaskConfigList, err := OpenAPITaskToSubTaskConfigs(&task, toDBCfg, sourceCfgMap)
	c.Assert(err, check.IsNil)
	c.Assert(subTaskConfigList, check.HasLen, 2)

	// prepare sub task config
	subTaskConfigMap := make(map[string]map[string]*SubTaskConfig)
	subTaskConfigMap[task.Name] = make(map[string]*SubTaskConfig)
	subTaskConfigMap[task.Name][source1Name] = subTaskConfigList[0]
	subTaskConfigMap[task.Name][source2Name] = subTaskConfigList[1]

	taskList := SubTaskConfigsToOpenAPITaskList(subTaskConfigMap)
	c.Assert(taskList, check.HasLen, 1)
	newTask := taskList[0]

	// because subtask config not have filter-rule-name, so we need to add it manually
	oldRuleName := "filterA"
	newRuleName := genFilterRuleName(source1Name, 0)
	oldRule, ok := task.BinlogFilterRule.Get(oldRuleName)
	c.Assert(ok, check.IsTrue)
	newRule := openapi.Task_BinlogFilterRule{}
	newRule.Set(newRuleName, oldRule)
	task.BinlogFilterRule = &newRule
	task.TableMigrateRule[0].BinlogFilterRule = &[]string{newRuleName}

	// because map key is not sorted, so generated array (source_conf and table_migrate_rule) order may not same with old one.
	// so we need to fix it manually.
	if task.SourceConfig.SourceConf[0].SourceName != newTask.SourceConfig.SourceConf[0].SourceName {
		task.SourceConfig.SourceConf[0], task.SourceConfig.SourceConf[1] = task.SourceConfig.SourceConf[1], task.SourceConfig.SourceConf[0]
	}
	if task.TableMigrateRule[0].Source.SourceName != newTask.TableMigrateRule[0].Source.SourceName {
		task.TableMigrateRule[0], task.TableMigrateRule[1] = task.TableMigrateRule[1], task.TableMigrateRule[0]
	}

	c.Assert(&task, check.DeepEquals, newTask)
}

func TestConvertWithIgnoreCheckItems(t *testing.T) {
	task, err := fixtures.GenNoShardOpenAPITaskForTest()
	require.NoError(t, err)
	ignoreCheckingItems := []string{DumpPrivilegeChecking, VersionChecking}
	task.IgnoreCheckingItems = &ignoreCheckingItems
	sourceCfg1, err := SourceCfgFromYamlAndVerify(SampleSourceConfig)
	require.NoError(t, err)
	source1Name := task.SourceConfig.SourceConf[0].SourceName
	sourceCfg1.SourceID = task.SourceConfig.SourceConf[0].SourceName
	sourceCfgMap := map[string]*SourceConfig{source1Name: sourceCfg1}
	toDBCfg := &dbconfig.DBConfig{
		Host:     task.TargetConfig.Host,
		Port:     task.TargetConfig.Port,
		User:     task.TargetConfig.User,
		Password: task.TargetConfig.Password,
	}
	subTaskConfigList, err := OpenAPITaskToSubTaskConfigs(&task, toDBCfg, sourceCfgMap)
	require.NoError(t, err)
	require.Equal(t, 1, len(subTaskConfigList))

	// prepare sub task config
	subTaskConfigMap := make(map[string]map[string]*SubTaskConfig)
	subTaskConfigMap[task.Name] = make(map[string]*SubTaskConfig)
	subTaskConfigMap[task.Name][source1Name] = subTaskConfigList[0]

	taskList := SubTaskConfigsToOpenAPITaskList(subTaskConfigMap)
	require.Equal(t, 1, len(taskList))
	newTask := taskList[0]
	require.Equal(t, *newTask.IgnoreCheckingItems, ignoreCheckingItems)
	require.Equal(t, *newTask, task)
}

func TestConvertBetweenOpenAPITaskAndTaskConfig(t *testing.T) {
	// one source task
	task, err := fixtures.GenNoShardOpenAPITaskForTest()
	require.NoError(t, err)

	sourceCfg1, err := SourceCfgFromYamlAndVerify(SampleSourceConfig)
	require.NoError(t, err)
	source1Name := task.SourceConfig.SourceConf[0].SourceName
	sourceCfg1.SourceID = source1Name
	sourceCfgMap := map[string]*SourceConfig{source1Name: sourceCfg1}

	taskCfg, err := OpenAPITaskToTaskConfig(&task, sourceCfgMap)
	require.NoError(t, err)
	require.NotNil(t, taskCfg)
	task1, err := TaskConfigToOpenAPITask(taskCfg, sourceCfgMap)
	require.NoError(t, err)
	require.NotNil(t, task1)
	require.EqualValues(t, task1, &task)

	// test update some fields in task
	{
		batch := 1000
		task.SourceConfig.IncrMigrateConf.ReplBatch = &batch
		taskCfg2, err2 := OpenAPITaskToTaskConfig(&task, sourceCfgMap)
		require.NoError(t, err2)
		require.Equal(t, batch, taskCfg2.MySQLInstances[0].Syncer.Batch)
		for _, cfg := range taskCfg2.Syncers {
			require.Equal(t, batch, cfg.Batch)
		}

		// test update some fields in taskConfig
		batch = 1
		for _, cfg := range taskCfg2.Syncers {
			cfg.Batch = batch
		}
		task2, err3 := TaskConfigToOpenAPITask(taskCfg2, sourceCfgMap)
		require.NoError(t, err3)
		require.Equal(t, batch, *task2.SourceConfig.IncrMigrateConf.ReplBatch)
	}

	// test update route
	{
		require.Len(t, task.TableMigrateRule, 1)
		sourceSchema := task.TableMigrateRule[0].Source.Schema
		targetSchema := *task.TableMigrateRule[0].Target.Schema
		// only route schema
		task.TableMigrateRule[0].Source = struct {
			Schema     string `json:"schema"`
			SourceName string `json:"source_name"`
			Table      string `json:"table"`
		}{
			SourceName: source1Name,
			Schema:     sourceSchema,
		}
		task.TableMigrateRule[0].Target = &struct {
			Schema *string `json:"schema,omitempty"`
			Table  *string `json:"table,omitempty"`
		}{
			Schema: &targetSchema,
		}
		taskCfg, err = OpenAPITaskToTaskConfig(&task, sourceCfgMap)
		require.NoError(t, err)
		require.Len(t, taskCfg.Routes, 1)
		var routeKey string
		for k := range taskCfg.Routes {
			routeKey = k
		}

		// validate route rule in taskCfg
		require.Equal(t, sourceSchema, taskCfg.Routes[routeKey].SchemaPattern)
		require.Equal(t, "", taskCfg.Routes[routeKey].TablePattern)
		require.Equal(t, targetSchema, taskCfg.Routes[routeKey].TargetSchema)
		require.Equal(t, "", taskCfg.Routes[routeKey].TargetTable)

		// validate baList in taskCfg, just do DBs because there is no table route rule
		require.Len(t, taskCfg.BAList, 1)
		require.Len(t, taskCfg.MySQLInstances, 1)
		baListName := taskCfg.MySQLInstances[0].BAListName
		require.Len(t, taskCfg.BAList[baListName].DoTables, 0)
		require.Len(t, taskCfg.BAList[baListName].DoDBs, 1)
		require.Equal(t, sourceSchema, taskCfg.BAList[baListName].DoDBs[0])

		// convert back to openapi.Task
		taskAfterConvert, err2 := TaskConfigToOpenAPITask(taskCfg, sourceCfgMap)
		require.NoError(t, err2)
		require.NotNil(t, taskAfterConvert)
		require.EqualValues(t, taskAfterConvert, &task)

		// only route table will meet error
		sourceTable := "tb"
		targetTable := "tb1"
		task.TableMigrateRule[0].Source = struct {
			Schema     string `json:"schema"`
			SourceName string `json:"source_name"`
			Table      string `json:"table"`
		}{
			SourceName: source1Name,
			Schema:     sourceSchema,
			Table:      sourceTable,
		}
		task.TableMigrateRule[0].Target = &struct {
			Schema *string `json:"schema,omitempty"`
			Table  *string `json:"table,omitempty"`
		}{
			Table: &targetTable,
		}
		_, err = OpenAPITaskToTaskConfig(&task, sourceCfgMap)
		require.True(t, terror.ErrConfigGenTableRouter.Equal(err))

		// route both
		task.TableMigrateRule[0].Source = struct {
			Schema     string `json:"schema"`
			SourceName string `json:"source_name"`
			Table      string `json:"table"`
		}{
			SourceName: source1Name,
			Schema:     sourceSchema,
			Table:      sourceTable,
		}
		task.TableMigrateRule[0].Target = &struct {
			Schema *string `json:"schema,omitempty"`
			Table  *string `json:"table,omitempty"`
		}{
			Schema: &targetSchema,
			Table:  &targetTable,
		}
		taskCfg, err = OpenAPITaskToTaskConfig(&task, sourceCfgMap)
		require.NoError(t, err)

		// validate route rule in taskCfg
		require.Equal(t, sourceSchema, taskCfg.Routes[routeKey].SchemaPattern)
		require.Equal(t, sourceTable, taskCfg.Routes[routeKey].TablePattern)
		require.Equal(t, targetSchema, taskCfg.Routes[routeKey].TargetSchema)
		require.Equal(t, targetTable, taskCfg.Routes[routeKey].TargetTable)

		// validate baList in taskCfg, just do Tables because there is a table route rule
		require.Len(t, taskCfg.BAList[baListName].DoDBs, 0)
		require.Len(t, taskCfg.BAList[baListName].DoTables, 1)
		require.Equal(t, sourceSchema, taskCfg.BAList[baListName].DoTables[0].Schema)
		require.Equal(t, sourceTable, taskCfg.BAList[baListName].DoTables[0].Name)

		// convert back to openapi.Task
		taskAfterConvert, err = TaskConfigToOpenAPITask(taskCfg, sourceCfgMap)
		require.NoError(t, err)
		require.NotNil(t, taskAfterConvert)
		require.EqualValues(t, taskAfterConvert, &task)

		// no route and only sync one table
		task.TableMigrateRule[0].Target = nil
		taskCfg, err = OpenAPITaskToTaskConfig(&task, sourceCfgMap)
		require.NoError(t, err)
		require.Len(t, taskCfg.Routes, 0)

		// validate baList in taskCfg, just do Tables because there is a table route rule
		require.Len(t, taskCfg.BAList[baListName].DoDBs, 0)
		require.Len(t, taskCfg.BAList[baListName].DoTables, 1)
		require.Equal(t, sourceSchema, taskCfg.BAList[baListName].DoTables[0].Schema)
		require.Equal(t, sourceTable, taskCfg.BAList[baListName].DoTables[0].Name)

		taskAfterConvert, err = TaskConfigToOpenAPITask(taskCfg, sourceCfgMap)
		require.NoError(t, err)
		require.NotNil(t, taskAfterConvert)
		require.EqualValues(t, taskAfterConvert, &task)

		// no route and sync one schema
		task.TableMigrateRule[0].Source = struct {
			Schema     string `json:"schema"`
			SourceName string `json:"source_name"`
			Table      string `json:"table"`
		}{
			SourceName: source1Name,
			Schema:     sourceSchema,
			Table:      "",
		}
		taskCfg, err = OpenAPITaskToTaskConfig(&task, sourceCfgMap)
		require.NoError(t, err)
		require.Len(t, taskCfg.Routes, 0)

		// validate baList in taskCfg, just do DBs because there is no table route rule
		require.Len(t, taskCfg.BAList[baListName].DoTables, 0)
		require.Len(t, taskCfg.BAList[baListName].DoDBs, 1)
		require.Equal(t, sourceSchema, taskCfg.BAList[baListName].DoDBs[0])

		taskAfterConvert, err = TaskConfigToOpenAPITask(taskCfg, sourceCfgMap)
		require.NoError(t, err)
		require.NotNil(t, taskAfterConvert)
		require.EqualValues(t, taskAfterConvert, &task)
	}

	// test update filter
	{
		// no filter no change
		require.Nil(t, task.BinlogFilterRule)
		taskCfg, err = OpenAPITaskToTaskConfig(&task, sourceCfgMap)
		require.NoError(t, err)
		taskAfterConvert, err := TaskConfigToOpenAPITask(taskCfg, sourceCfgMap)
		require.NoError(t, err)
		require.NotNil(t, taskAfterConvert)
		require.EqualValues(t, taskAfterConvert, &task)

		// filter both
		sourceSchema := task.TableMigrateRule[0].Source.Schema
		sourceTable := task.TableMigrateRule[0].Source.Table
		ignoreEvent := []string{"drop database"}
		ignoreSQL := []string{"^Drop"}
		ruleName := genFilterRuleName(source1Name, 0)
		ruleNameList := []string{ruleName}
		rule := openapi.TaskBinLogFilterRule{IgnoreEvent: &ignoreEvent, IgnoreSql: &ignoreSQL}
		ruleM := &openapi.Task_BinlogFilterRule{}
		ruleM.Set(ruleName, rule)
		task.BinlogFilterRule = ruleM
		task.TableMigrateRule[0].BinlogFilterRule = &ruleNameList

		taskCfg, err = OpenAPITaskToTaskConfig(&task, sourceCfgMap)
		require.NoError(t, err)
		require.Len(t, taskCfg.Filters, 1)
		require.Len(t, taskCfg.MySQLInstances[0].FilterRules, 1)
		filterName := taskCfg.MySQLInstances[0].FilterRules[0]
		require.Equal(t, bf.Ignore, taskCfg.Filters[filterName].Action)
		require.Equal(t, sourceSchema, taskCfg.Filters[filterName].SchemaPattern)
		require.Equal(t, sourceTable, taskCfg.Filters[filterName].TablePattern)
		require.Len(t, taskCfg.Filters[filterName].SQLPattern, 1)
		require.Equal(t, ignoreSQL[0], taskCfg.Filters[filterName].SQLPattern[0])
		require.Len(t, taskCfg.Filters[filterName].Events, 1)
		require.Equal(t, ignoreEvent[0], string(taskCfg.Filters[filterName].Events[0]))

		// convert back to openapi.Task
		taskAfterConvert, err = TaskConfigToOpenAPITask(taskCfg, sourceCfgMap)
		require.NoError(t, err)
		require.NotNil(t, taskAfterConvert)
		require.EqualValues(t, taskAfterConvert, &task)

		// only filter events
		rule = openapi.TaskBinLogFilterRule{IgnoreEvent: &ignoreEvent}
		ruleM.Set(ruleName, rule)
		taskCfg, err = OpenAPITaskToTaskConfig(&task, sourceCfgMap)
		require.NoError(t, err)
		require.Len(t, taskCfg.Filters[filterName].SQLPattern, 0)

		taskAfterConvert, err = TaskConfigToOpenAPITask(taskCfg, sourceCfgMap)
		ruleMAfterConvert := taskAfterConvert.BinlogFilterRule
		ruleAfterConvert, ok := ruleMAfterConvert.Get(ruleName)
		require.True(t, ok)
		require.Nil(t, ruleAfterConvert.IgnoreSql)
		require.NoError(t, err)
		require.NotNil(t, taskAfterConvert)
		require.EqualValues(t, taskAfterConvert, &task)
	}
}
