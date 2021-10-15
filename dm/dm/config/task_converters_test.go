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

	"github.com/pingcap/check"
	bf "github.com/pingcap/tidb-tools/pkg/binlog-filter"
	"github.com/pingcap/tidb-tools/pkg/filter"

	"github.com/pingcap/dm/openapi"
	"github.com/pingcap/dm/openapi/fixtures"
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
	sourceCfg1, err := LoadFromFile(sourceSampleFile)
	c.Assert(err, check.IsNil)
	source1Name := task.SourceConfig.SourceConf[0].SourceName
	sourceCfg1.SourceID = task.SourceConfig.SourceConf[0].SourceName
	sourceCfgMap := map[string]*SourceConfig{source1Name: sourceCfg1}
	toDBCfg := &DBConfig{
		Host:     task.TargetConfig.Host,
		Port:     task.TargetConfig.Port,
		User:     task.TargetConfig.User,
		Password: task.TargetConfig.Password,
	}
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
	c.Assert(rule.TargetSchema, check.Equals, tartgetSchema)
	c.Assert(rule.TargetTable, check.Equals, tartgetTable)
	// check filter
	c.Assert(subTaskConfig.FilterRules, check.HasLen, 0)
	// check balist
	c.Assert(subTaskConfig.BAList, check.NotNil)
	bAListFromOpenAPITask := &filter.Rules{
		DoDBs:    []string{sourceSchema},
		DoTables: []*filter.Table{{Schema: sourceSchema, Name: sourceTable}},
	}
	c.Assert(subTaskConfig.BAList, check.DeepEquals, bAListFromOpenAPITask)
}

func testShardAndFilterTaskToSubTaskConfigs(c *check.C) {
	task, err := fixtures.GenShardAndFilterOpenAPITaskForTest()
	c.Assert(err, check.IsNil)
	sourceCfg1, err := LoadFromFile(sourceSampleFile)
	c.Assert(err, check.IsNil)
	source1Name := task.SourceConfig.SourceConf[0].SourceName
	sourceCfg1.SourceID = source1Name
	sourceCfg2, err := LoadFromFile(sourceSampleFile)
	c.Assert(err, check.IsNil)
	source2Name := task.SourceConfig.SourceConf[1].SourceName
	sourceCfg2.SourceID = source2Name

	toDBCfg := &DBConfig{
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
	c.Assert(subTask1Config.ShardMode, check.Equals, string(openapi.TaskShardModePessimistic))
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
	c.Assert(rule.TargetSchema, check.Equals, tartgetSchema)
	c.Assert(rule.TargetTable, check.Equals, tartgetTable)
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
		DoDBs:    []string{source1Schema},
		DoTables: []*filter.Table{{Schema: source1Schema, Name: source1Table}},
	}
	c.Assert(subTask1Config.BAList, check.DeepEquals, bAListFromOpenAPITask)

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
	c.Assert(subTask2Config.ShardMode, check.Equals, string(openapi.TaskShardModePessimistic))
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
	c.Assert(rule.TargetSchema, check.Equals, tartgetSchema)
	c.Assert(rule.TargetTable, check.Equals, tartgetTable)
	// check filter
	_, ok = task.BinlogFilterRule.Get("filterB")
	c.Assert(ok, check.IsFalse)
	c.Assert(subTask2Config.FilterRules, check.HasLen, 0)
	// check balist
	c.Assert(subTask2Config.BAList, check.NotNil)
	bAListFromOpenAPITask = &filter.Rules{
		DoDBs:    []string{source2Schema},
		DoTables: []*filter.Table{{Schema: source2Schema, Name: source2Table}},
	}
	c.Assert(subTask2Config.BAList, check.DeepEquals, bAListFromOpenAPITask)
}
