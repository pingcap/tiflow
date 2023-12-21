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
	"os"
	"path"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/coreos/go-semver/semver"
	bf "github.com/pingcap/tidb-tools/pkg/binlog-filter"
	"github.com/pingcap/tidb/pkg/util/filter"
	router "github.com/pingcap/tidb/pkg/util/table-router"
	"github.com/pingcap/tiflow/dm/config/dbconfig"
	"github.com/pingcap/tiflow/dm/config/security"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/stretchr/testify/require"
)

var correctTaskConfig = `---
name: test
task-mode: all
shard-mode: "pessimistic"
meta-schema: "dm_meta"
case-sensitive: false
online-ddl: true
clean-dump-file: true

target-database:
  host: "127.0.0.1"
  port: 4000
  user: "root"
  password: ""

routes:
  route-rule-1:
    schema-pattern: "test_*"
    target-schema: "test"
  route-rule-2:
    schema-pattern: "test_*"
    target-schema: "test"

filters:
  filter-rule-1:
    schema-pattern: "test_*"
    events: ["truncate table", "drop table"]
    action: Ignore
  filter-rule-2:
    schema-pattern: "test_*"
    events: ["all dml"]
    action: Do

mydumpers:
  global1:
    threads: 4
    chunk-filesize: 64
    skip-tz-utc: true
    extra-args: "--consistency none"
  global2:
    threads: 8
    chunk-filesize: 128
    skip-tz-utc: true
    extra-args: "--consistency none"

loaders:
  global1:
    pool-size: 16
    dir: "./dumped_data1"
  global2:
    pool-size: 8
    dir: "./dumped_data2"

syncers:
  global1:
    worker-count: 16
    batch: 100
    enable-ansi-quotes: true
    safe-mode: false
  global2:
    worker-count: 32
    batch: 100
    enable-ansi-quotes: true
    safe-mode: false

expression-filter:
  expr-1:
    schema: "db"
    table: "tbl"
    insert-value-expr: "a > 1"

mysql-instances:
  - source-id: "mysql-replica-01"
    route-rules: ["route-rule-2"]
    filter-rules: ["filter-rule-2"]
    mydumper-config-name: "global1"
    loader-config-name: "global1"
    syncer-config-name: "global1"
    expression-filters: ["expr-1"]

  - source-id: "mysql-replica-02"
    route-rules: ["route-rule-1"]
    filter-rules: ["filter-rule-1"]
    mydumper-config-name: "global2"
    loader-config-name: "global2"
    syncer-config-name: "global2"
`

func TestUnusedTaskConfig(t *testing.T) {
	t.Parallel()

	taskConfig := NewTaskConfig()
	err := taskConfig.Decode(correctTaskConfig)
	require.NoError(t, err)
	errorTaskConfig := `---
name: test
task-mode: all
shard-mode: "pessimistic"
meta-schema: "dm_meta"
case-sensitive: false
online-ddl: true
clean-dump-file: true

target-database:
  host: "127.0.0.1"
  port: 4000
  user: "root"
  password: ""

routes:
  route-rule-1:
    schema-pattern: "test_*"
    target-schema: "test"
  route-rule-2:
    schema-pattern: "test_*"
    target-schema: "test"

filters:
  filter-rule-1:
    schema-pattern: "test_*"
    table-pattern: "t_*"
    events: ["truncate table", "drop table"]
    action: Ignore
  filter-rule-2:
    schema-pattern: "test_*"
    events: ["all dml"]
    action: Do

mydumpers:
  global1:
    threads: 4
    chunk-filesize: 64
    skip-tz-utc: true
    extra-args: "--consistency none"
  global2:
    threads: 8
    chunk-filesize: 128
    skip-tz-utc: true
    extra-args: "--consistency none"

loaders:
  global1:
    pool-size: 16
    dir: "./dumped_data1"
  global2:
    pool-size: 8
    dir: "./dumped_data2"

syncers:
  global1:
    worker-count: 16
    batch: 100
    enable-ansi-quotes: true
    safe-mode: false
  global2:
    worker-count: 32
    batch: 100
    enable-ansi-quotes: true
    safe-mode: false

expression-filter:
  expr-1:
    schema: "db"
    table: "tbl"
    insert-value-expr: "a > 1"

mysql-instances:
  - source-id: "mysql-replica-01"
    route-rules: ["route-rule-1"]
    filter-rules: ["filter-rule-1"]
    mydumper-config-name: "global1"
    loader-config-name: "global1"
    syncer-config-name: "global1"

  - source-id: "mysql-replica-02"
    route-rules: ["route-rule-1"]
    filter-rules: ["filter-rule-1"]
    mydumper-config-name: "global2"
    loader-config-name: "global2"
    syncer-config-name: "global2"
`
	taskConfig = NewTaskConfig()
	err = taskConfig.Decode(errorTaskConfig)
	require.ErrorContains(t, err, "The configurations as following [expr-1 filter-rule-2 route-rule-2] are set in global configuration")
}

func TestName(t *testing.T) {
	t.Parallel()

	errorTaskConfig1 := `---
name: test
task-mode: all
is-sharding: true
meta-schema: "dm_meta"
timezone: "Asia/Shanghai"
enable-heartbeat: true
ignore-checking-items: ["all"]

target-database:
  host: "127.0.0.1"
  port: 4000
  user: "root"
  password: ""

mysql-instances:
  - source-id: "mysql-replica-01"
    server-id: 101
    block-allow-list:  "instance"
    route-rules: ["sharding-route-rules-table", "sharding-route-rules-schema"]
    mydumper-config-name: "global"
    loader-config-name: "global"
    syncer-config-name: "global"
`
	errorTaskConfig2 := `---
name: test
name: test1
task-mode: all
is-sharding: true
meta-schema: "dm_meta"
enable-heartbeat: true
timezone: "Asia/Shanghai"
ignore-checking-items: ["all"]

target-database:
  host: "127.0.0.1"
  port: 4000
  user: "root"
  password: ""

mysql-instances:
  - source-id: "mysql-replica-01"
    block-allow-list:  "instance"
    route-rules: ["sharding-route-rules-table", "sharding-route-rules-schema"]
    mydumper-config-name: "global"
    loader-config-name: "global"
    syncer-config-name: "global"
`
	taskConfig := NewTaskConfig()
	err := taskConfig.Decode(errorTaskConfig1)
	// field server-id is not a member of TaskConfig
	require.ErrorContains(t, err, "line 18: field server-id not found in type config.MySQLInstance")

	err = taskConfig.Decode(errorTaskConfig2)
	// field name duplicate
	require.ErrorContains(t, err, "line 3: field name already set in type config.TaskConfig")

	filepath := path.Join(t.TempDir(), "test_invalid_task.yaml")
	configContent := []byte(`---
aaa: xxx
name: test
task-mode: all
is-sharding: true
meta-schema: "dm_meta"
enable-heartbeat: true
ignore-checking-items: ["all"]
`)
	err = os.WriteFile(filepath, configContent, 0o644)
	require.NoError(t, err)
	taskConfig = NewTaskConfig()
	err = taskConfig.DecodeFile(filepath)
	require.ErrorContains(t, err, "line 2: field aaa not found in type config.TaskConfig")

	configContent = []byte(`---
name: test
task-mode: all
task-mode: all
is-sharding: true
meta-schema: "dm_meta"
enable-heartbeat: true
ignore-checking-items: ["all"]
`)
	err = os.WriteFile(filepath, configContent, 0o644)
	require.NoError(t, err)
	taskConfig = NewTaskConfig()
	err = taskConfig.DecodeFile(filepath)
	require.ErrorContains(t, err, "line 4: field task-mode already set in type config.TaskConfig")

	configContent = []byte(`---
name: test
is-sharding: true
meta-schema: "dm_meta"
enable-heartbeat: true
ignore-checking-items: ["all"]
`)
	err = os.WriteFile(filepath, configContent, 0o644)
	require.NoError(t, err)
	taskConfig = NewTaskConfig()
	err = taskConfig.DecodeFile(filepath)
	require.True(t, terror.ErrConfigInvalidTaskMode.Equal(err))

	// test valid task config
	configContent = []byte(`---
name: test
task-mode: all
is-sharding: true
meta-schema: "dm_meta"
enable-heartbeat: true
heartbeat-update-interval: 1
heartbeat-report-interval: 1

target-database:
  host: "127.0.0.1"
  port: 4000
  user: "root"
  password: ""

mysql-instances:
  - source-id: "mysql-replica-01"
    block-allow-list:  "instance"
    mydumper-thread: 11
    mydumper-config-name: "global"
    loader-thread: 22
    loader-config-name: "global"
    syncer-thread: 33
    syncer-config-name: "global"

  - source-id: "mysql-replica-02"
    block-allow-list:  "instance"
    mydumper-config-name: "global"
    loader-config-name: "global"
    syncer-config-name: "global"

  - source-id: "mysql-replica-03"
    block-allow-list:  "instance"
    mydumper-thread: 44
    loader-thread: 55
    syncer-thread: 66

block-allow-list:
  instance:
    do-dbs: ["test"]

mydumpers:
  global:
    threads: 4
    chunk-filesize: 64
    skip-tz-utc: true
    extra-args: "-B test"

loaders:
  global:
    pool-size: 16
    dir: "./dumped_data"

syncers:
  global:
    worker-count: 16
    batch: 100
`)

	err = os.WriteFile(filepath, configContent, 0o644)
	require.NoError(t, err)
	taskConfig = NewTaskConfig()
	err = taskConfig.DecodeFile(filepath)
	require.NoError(t, err)
	require.True(t, taskConfig.IsSharding)
	require.Equal(t, ShardPessimistic, taskConfig.ShardMode)
	require.Equal(t, 11, taskConfig.MySQLInstances[0].Mydumper.Threads)
	require.Equal(t, 22, taskConfig.MySQLInstances[0].Loader.PoolSize)
	require.Equal(t, 33, taskConfig.MySQLInstances[0].Syncer.WorkerCount)
	require.Equal(t, 4, taskConfig.MySQLInstances[1].Mydumper.Threads)
	require.Equal(t, 16, taskConfig.MySQLInstances[1].Loader.PoolSize)
	require.Equal(t, 16, taskConfig.MySQLInstances[1].Syncer.WorkerCount)
	require.Equal(t, 44, taskConfig.MySQLInstances[2].Mydumper.Threads)
	require.Equal(t, 55, taskConfig.MySQLInstances[2].Loader.PoolSize)
	require.Equal(t, 66, taskConfig.MySQLInstances[2].Syncer.WorkerCount)

	configContent = []byte(`---
name: test
task-mode: all
is-sharding: true
shard-mode: "optimistic"
meta-schema: "dm_meta"
enable-heartbeat: true
heartbeat-update-interval: 1
heartbeat-report-interval: 1

target-database:
  host: "127.0.0.1"
  port: 4000
  user: "root"
  password: ""

mysql-instances:
  - source-id: "mysql-replica-01"
  - source-id: "mysql-replica-02"
  - source-id: "mysql-replica-03"

block-allow-list:
  instance:
    do-dbs: ["test"]

routes:
  route-rule-1:
  route-rule-2:
  route-rule-3:
  route-rule-4:

filters:
  filter-rule-1:
  filter-rule-2:
  filter-rule-3:
  filter-rule-4:
`)

	err = os.WriteFile(filepath, configContent, 0o644)
	require.NoError(t, err)
	taskConfig = NewTaskConfig()
	err = taskConfig.DecodeFile(filepath)
	require.Error(t, err)
	require.True(t, taskConfig.IsSharding)
	require.Equal(t, ShardOptimistic, taskConfig.ShardMode)
	taskConfig.MySQLInstances[0].RouteRules = []string{"route-rule-1", "route-rule-2", "route-rule-1", "route-rule-2"}
	taskConfig.MySQLInstances[1].FilterRules = []string{"filter-rule-1", "filter-rule-2", "filter-rule-3", "filter-rule-2"}
	err = taskConfig.adjust()
	require.True(t, terror.ErrConfigDuplicateCfgItem.Equal(err))
	require.ErrorContains(t, err, "mysql-instance(0)'s route-rules: route-rule-1, route-rule-2")
	require.ErrorContains(t, err, "mysql-instance(1)'s filter-rules: filter-rule-2")
}

func TestCheckDuplicateString(t *testing.T) {
	t.Parallel()

	a := []string{"a", "b", "c", "d"}
	dupeStrings := checkDuplicateString(a)
	require.Len(t, dupeStrings, 0)
	a = []string{"a", "a", "b", "b", "c", "c"}
	dupeStrings = checkDuplicateString(a)
	require.Len(t, dupeStrings, 3)
	sort.Strings(dupeStrings)
	require.Equal(t, []string{"a", "b", "c"}, dupeStrings)
}

func TestTaskBlockAllowList(t *testing.T) {
	t.Parallel()

	filterRules1 := &filter.Rules{
		DoDBs: []string{"s1"},
	}

	filterRules2 := &filter.Rules{
		DoDBs: []string{"s2"},
	}

	cfg := &TaskConfig{
		Name:           "test",
		TaskMode:       ModeFull,
		TargetDB:       &dbconfig.DBConfig{},
		MySQLInstances: []*MySQLInstance{{SourceID: "source-1"}},
		BWList:         map[string]*filter.Rules{"source-1": filterRules1},
	}

	// BAList is nil, will set BAList = BWList
	err := cfg.adjust()
	require.NoError(t, err)
	require.Equal(t, filterRules1, cfg.BAList["source-1"])

	// BAList is not nil, will not update it
	cfg.BAList = map[string]*filter.Rules{"source-1": filterRules2}
	err = cfg.adjust()
	require.NoError(t, err)
	require.Equal(t, filterRules2, cfg.BAList["source-1"])
}

func wordCount(s string) map[string]int {
	words := strings.Fields(s)
	wordCount := make(map[string]int)
	for i := range words {
		wordCount[words[i]]++
	}

	return wordCount
}

func TestGenAndFromSubTaskConfigs(t *testing.T) {
	t.Parallel()

	var (
		shardMode           = ShardOptimistic
		onlineDDL           = true
		name                = "from-sub-tasks"
		taskMode            = "incremental"
		ignoreCheckingItems = []string{VersionChecking, BinlogRowImageChecking}
		source1             = "mysql-replica-01"
		source2             = "mysql-replica-02"
		metaSchema          = "meta-sub-tasks"
		heartbeatUI         = 12
		heartbeatRI         = 21
		maxAllowedPacket    = 10244201
		fromSession         = map[string]string{
			"sql_mode":  " NO_AUTO_VALUE_ON_ZERO,ANSI_QUOTES",
			"time_zone": "+00:00",
		}
		toSession = map[string]string{
			"sql_mode":  " NO_AUTO_VALUE_ON_ZERO,ANSI_QUOTES",
			"time_zone": "+00:00",
		}
		security = security.Security{
			SSLCA:         "/path/to/ca",
			SSLCert:       "/path/to/cert",
			SSLKey:        "/path/to/key",
			CertAllowedCN: []string{"allowed-cn"},
		}
		rawDBCfg = dbconfig.RawDBConfig{
			MaxIdleConns: 333,
			ReadTimeout:  "2m",
			WriteTimeout: "1m",
		}
		routeRule1 = router.TableRule{
			SchemaPattern: "db*",
			TargetSchema:  "db",
		}
		routeRule2 = router.TableRule{
			SchemaPattern: "db*",
			TablePattern:  "tbl*",
			TargetSchema:  "db",
			TargetTable:   "tbl",
		}
		routeRule3 = router.TableRule{
			SchemaPattern: "schema*",
			TargetSchema:  "schema",
		}
		routeRule4 = router.TableRule{
			SchemaPattern: "schema*",
			TablePattern:  "tbs*",
			TargetSchema:  "schema",
			TargetTable:   "tbs",
		}

		filterRule1 = bf.BinlogEventRule{
			SchemaPattern: "db*",
			TablePattern:  "tbl1*",
			Events:        []bf.EventType{bf.CreateIndex, bf.AlterTable},
			Action:        bf.Do,
		}
		filterRule2 = bf.BinlogEventRule{
			SchemaPattern: "db*",
			TablePattern:  "tbl2",
			SQLPattern:    []string{"^DROP\\s+PROCEDURE", "^CREATE\\s+PROCEDURE"},
			Action:        bf.Ignore,
		}
		baList1 = filter.Rules{
			DoDBs: []string{"db1", "db2"},
			DoTables: []*filter.Table{
				{Schema: "db1", Name: "tbl1"},
				{Schema: "db2", Name: "tbl2"},
			},
		}
		baList2 = filter.Rules{
			IgnoreDBs: []string{"bd1", "bd2"},
			IgnoreTables: []*filter.Table{
				{Schema: "bd1", Name: "lbt1"},
				{Schema: "bd2", Name: "lbt2"},
			},
		}
		exprFilter1 = ExpressionFilter{
			Schema:          "db",
			Table:           "tbl",
			DeleteValueExpr: "state = 1",
		}
		validatorCfg = ValidatorConfig{Mode: ValidationNone}
		source1DBCfg = dbconfig.DBConfig{
			Host:             "127.0.0.1",
			Port:             3306,
			User:             "user_from_1",
			Password:         "123",
			MaxAllowedPacket: &maxAllowedPacket,
			Session:          fromSession,
			Security:         &security,
			RawDBCfg:         &rawDBCfg,
		}
		source2DBCfg = dbconfig.DBConfig{
			Host:             "127.0.0.1",
			Port:             3307,
			User:             "user_from_2",
			Password:         "abc",
			MaxAllowedPacket: &maxAllowedPacket,
			Session:          fromSession,
			Security:         &security,
			RawDBCfg:         &rawDBCfg,
		}

		stCfg1 = &SubTaskConfig{
			IsSharding:              true,
			ShardMode:               shardMode,
			OnlineDDL:               onlineDDL,
			ShadowTableRules:        []string{DefaultShadowTableRules},
			TrashTableRules:         []string{DefaultTrashTableRules},
			CaseSensitive:           true,
			Name:                    name,
			Mode:                    taskMode,
			IgnoreCheckingItems:     ignoreCheckingItems,
			SourceID:                source1,
			MetaSchema:              metaSchema,
			HeartbeatUpdateInterval: heartbeatUI,
			HeartbeatReportInterval: heartbeatRI,
			EnableHeartbeat:         true,
			CollationCompatible:     LooseCollationCompatible,
			Meta: &Meta{
				BinLogName: "mysql-bin.000123",
				BinLogPos:  456,
				BinLogGTID: "1-1-12,4-4-4",
			},
			From: source1DBCfg,
			To: dbconfig.DBConfig{
				Host:             "127.0.0.1",
				Port:             4000,
				User:             "user_to",
				Password:         "abc",
				MaxAllowedPacket: &maxAllowedPacket,
				Session:          toSession,
				Security:         &security,
				RawDBCfg:         &rawDBCfg,
			},
			RouteRules:  []*router.TableRule{&routeRule2, &routeRule1, &routeRule3},
			FilterRules: []*bf.BinlogEventRule{&filterRule1, &filterRule2},
			BAList:      &baList1,
			MydumperConfig: MydumperConfig{
				MydumperPath:  "",
				Threads:       16,
				ChunkFilesize: "64",
				StatementSize: 1000000,
				Rows:          1024,
				Where:         "",
				SkipTzUTC:     true,
				ExtraArgs:     "--escape-backslash",
			},
			LoaderConfig: LoaderConfig{
				PoolSize:            32,
				Dir:                 "./dumped_data",
				SortingDirPhysical:  "./dumped_data",
				ImportMode:          LoadModePhysical,
				OnDuplicateLogical:  OnDuplicateReplace,
				OnDuplicatePhysical: OnDuplicateNone,
				ChecksumPhysical:    OpLevelRequired,
				Analyze:             OpLevelOptional,
				PDAddr:              "http://test:2379",
				RangeConcurrency:    32,
				CompressKVPairs:     "gzip",
			},
			SyncerConfig: SyncerConfig{
				WorkerCount:             32,
				Batch:                   100,
				QueueSize:               512,
				CheckpointFlushInterval: 15,
				MaxRetry:                10,
				AutoFixGTID:             true,
				EnableGTID:              true,
				SafeMode:                true,
				SafeModeDuration:        "60s",
			},
			ValidatorCfg:     validatorCfg,
			CleanDumpFile:    true,
			EnableANSIQuotes: true,
		}
	)

	stCfg1.Experimental.AsyncCheckpointFlush = true
	stCfg2, err := stCfg1.Clone()
	require.NoError(t, err)
	stCfg2.SourceID = source2
	stCfg2.Meta = &Meta{
		BinLogName: "mysql-bin.000321",
		BinLogPos:  123,
		BinLogGTID: "1-1-21,2-2-2",
	}
	stCfg2.From = source2DBCfg
	stCfg2.BAList = &baList2
	stCfg2.RouteRules = []*router.TableRule{&routeRule4, &routeRule1, &routeRule2}
	stCfg2.ExprFilter = []*ExpressionFilter{&exprFilter1}
	stCfg2.ValidatorCfg.Mode = ValidationFast

	cfg := SubTaskConfigsToTaskConfig(stCfg1, stCfg2)

	cfg2 := TaskConfig{
		Name:                    name,
		TaskMode:                taskMode,
		IsSharding:              stCfg1.IsSharding,
		ShardMode:               shardMode,
		IgnoreCheckingItems:     ignoreCheckingItems,
		MetaSchema:              metaSchema,
		EnableHeartbeat:         stCfg1.EnableHeartbeat,
		HeartbeatUpdateInterval: heartbeatUI,
		HeartbeatReportInterval: heartbeatRI,
		CaseSensitive:           stCfg1.CaseSensitive,
		TargetDB:                &stCfg1.To,
		CollationCompatible:     LooseCollationCompatible,
		MySQLInstances: []*MySQLInstance{
			{
				SourceID:           source1,
				Meta:               stCfg1.Meta,
				FilterRules:        []string{"filter-01", "filter-02"},
				ColumnMappingRules: []string{},
				RouteRules:         []string{"route-01", "route-02", "route-03"},
				BWListName:         "",
				BAListName:         "balist-01",
				MydumperConfigName: "dump-01",
				Mydumper:           nil,
				MydumperThread:     0,
				LoaderConfigName:   "load-01",
				Loader:             nil,
				LoaderThread:       0,
				SyncerConfigName:   "sync-01",
				Syncer:             nil,
				SyncerThread:       0,

				ContinuousValidatorConfigName: "validator-01",
			},
			{
				SourceID:           source2,
				Meta:               stCfg2.Meta,
				FilterRules:        []string{"filter-01", "filter-02"},
				ColumnMappingRules: []string{},
				RouteRules:         []string{"route-01", "route-02", "route-04"},
				BWListName:         "",
				BAListName:         "balist-02",
				MydumperConfigName: "dump-01",
				Mydumper:           nil,
				MydumperThread:     0,
				LoaderConfigName:   "load-01",
				Loader:             nil,
				LoaderThread:       0,
				SyncerConfigName:   "sync-01",
				Syncer:             nil,
				SyncerThread:       0,
				ExpressionFilters:  []string{"expr-filter-01"},

				ContinuousValidatorConfigName: "validator-02",
			},
		},
		OnlineDDL: onlineDDL,
		Routes: map[string]*router.TableRule{
			"route-01": &routeRule1,
			"route-02": &routeRule2,
			"route-03": &routeRule3,
			"route-04": &routeRule4,
		},
		Filters: map[string]*bf.BinlogEventRule{
			"filter-01": &filterRule1,
			"filter-02": &filterRule2,
		},
		ColumnMappings: nil,
		BWList:         nil,
		BAList: map[string]*filter.Rules{
			"balist-01": &baList1,
			"balist-02": &baList2,
		},
		Mydumpers: map[string]*MydumperConfig{
			"dump-01": &stCfg1.MydumperConfig,
		},
		Loaders: map[string]*LoaderConfig{
			"load-01": &stCfg1.LoaderConfig,
		},
		Syncers: map[string]*SyncerConfig{
			"sync-01": &stCfg1.SyncerConfig,
		},
		ExprFilter: map[string]*ExpressionFilter{
			"expr-filter-01": &exprFilter1,
		},
		Validators: map[string]*ValidatorConfig{
			"validator-01": &validatorCfg,
			"validator-02": {
				Mode: ValidationFast,
			},
		},
		CleanDumpFile: stCfg1.CleanDumpFile,
	}
	cfg2.Experimental.AsyncCheckpointFlush = true

	require.Equal(t, wordCount(cfg.String()), wordCount(cfg2.String())) // since rules are unordered, so use wordCount to compare

	require.NoError(t, cfg.adjust())
	stCfgs, err := TaskConfigToSubTaskConfigs(cfg, map[string]dbconfig.DBConfig{source1: source1DBCfg, source2: source2DBCfg})
	require.NoError(t, err)
	// revert ./dumpped_data.from-sub-tasks
	stCfgs[0].LoaderConfig.Dir = stCfg1.LoaderConfig.Dir
	stCfgs[1].LoaderConfig.Dir = stCfg2.LoaderConfig.Dir
	// fix empty list and nil
	require.Len(t, stCfgs[0].ColumnMappingRules, 0)
	require.Len(t, stCfg1.ColumnMappingRules, 0)
	require.Len(t, stCfgs[1].ColumnMappingRules, 0)
	require.Len(t, stCfg2.ColumnMappingRules, 0)
	require.Len(t, stCfgs[0].ExprFilter, 0)
	require.Len(t, stCfg1.ExprFilter, 0)
	stCfgs[0].ColumnMappingRules = stCfg1.ColumnMappingRules
	stCfgs[1].ColumnMappingRules = stCfg2.ColumnMappingRules
	stCfgs[0].ExprFilter = stCfg1.ExprFilter
	// deprecated config will not recover
	stCfgs[0].EnableANSIQuotes = stCfg1.EnableANSIQuotes
	stCfgs[1].EnableANSIQuotes = stCfg2.EnableANSIQuotes
	// some features are disabled
	require.True(t, stCfg1.EnableHeartbeat)
	require.True(t, stCfg2.EnableHeartbeat)
	stCfg1.EnableHeartbeat = false
	stCfg2.EnableHeartbeat = false
	require.Equal(t, stCfg1.String(), stCfgs[0].String())
	require.Equal(t, stCfg2.String(), stCfgs[1].String())
	// adjust loader config
	stCfg1.Mode = "full"
	require.NoError(t, stCfg1.Adjust(false))
	require.Equal(t, stCfgs[0].SortingDirPhysical, stCfg1.SortingDirPhysical)
}

func TestMetaVerify(t *testing.T) {
	t.Parallel()

	var m *Meta
	require.NoError(t, m.Verify()) // nil meta is fine (for not incremental task mode)

	// none
	m = &Meta{}
	require.True(t, terror.ErrConfigMetaInvalid.Equal(m.Verify()))

	// only `binlog-name`.
	m = &Meta{
		BinLogName: "mysql-bin.000123",
	}
	require.NoError(t, m.Verify())

	// only `binlog-pos`.
	m = &Meta{
		BinLogPos: 456,
	}
	require.True(t, terror.ErrConfigMetaInvalid.Equal(m.Verify()))

	// only `binlog-gtid`.
	m = &Meta{
		BinLogGTID: "1-1-12,4-4-4",
	}
	require.NoError(t, m.Verify())

	// all
	m = &Meta{
		BinLogName: "mysql-bin.000123",
		BinLogPos:  456,
		BinLogGTID: "1-1-12,4-4-4",
	}
	require.NoError(t, m.Verify())
}

func TestMySQLInstance(t *testing.T) {
	t.Parallel()

	var m *MySQLInstance
	cfgName := "test"
	err := m.VerifyAndAdjust()
	require.True(t, terror.ErrConfigMySQLInstNotFound.Equal(err))

	m = &MySQLInstance{}
	err = m.VerifyAndAdjust()
	require.True(t, terror.ErrConfigEmptySourceID.Equal(err))
	m.SourceID = "123"

	m.Mydumper = &MydumperConfig{}
	m.MydumperConfigName = cfgName
	err = m.VerifyAndAdjust()
	require.True(t, terror.ErrConfigMydumperCfgConflict.Equal(err))
	m.MydumperConfigName = ""

	m.Loader = &LoaderConfig{}
	m.LoaderConfigName = cfgName
	err = m.VerifyAndAdjust()
	require.True(t, terror.ErrConfigLoaderCfgConflict.Equal(err))
	m.Loader = nil

	m.Syncer = &SyncerConfig{}
	m.SyncerConfigName = cfgName
	err = m.VerifyAndAdjust()
	require.True(t, terror.ErrConfigSyncerCfgConflict.Equal(err))
	m.SyncerConfigName = ""

	require.NoError(t, m.VerifyAndAdjust())
}

func TestAdjustTargetDBConfig(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		dbConfig dbconfig.DBConfig
		result   dbconfig.DBConfig
		version  *semver.Version
	}{
		{
			dbconfig.DBConfig{},
			dbconfig.DBConfig{Session: map[string]string{}},
			semver.New("0.0.0"),
		},
		{
			dbconfig.DBConfig{Session: map[string]string{"SQL_MODE": "ANSI_QUOTES"}},
			dbconfig.DBConfig{Session: map[string]string{"sql_mode": "ANSI_QUOTES"}},
			semver.New("2.0.7"),
		},
		{
			dbconfig.DBConfig{},
			dbconfig.DBConfig{Session: map[string]string{tidbTxnMode: tidbTxnOptimistic}},
			semver.New("3.0.1"),
		},
		{
			dbconfig.DBConfig{Session: map[string]string{"SQL_MODE": "", tidbTxnMode: "pessimistic"}},
			dbconfig.DBConfig{Session: map[string]string{"sql_mode": "", tidbTxnMode: "pessimistic"}},
			semver.New("4.0.0-beta.2"),
		},
	}

	for _, tc := range testCases {
		AdjustTargetDBSessionCfg(&tc.dbConfig, tc.version)
		require.Equal(t, tc.result, tc.dbConfig)
	}
}

func TestDefaultConfig(t *testing.T) {
	t.Parallel()

	cfg := NewTaskConfig()
	cfg.Name = "test"
	cfg.TaskMode = ModeAll
	cfg.TargetDB = &dbconfig.DBConfig{}
	cfg.MySQLInstances = append(cfg.MySQLInstances, &MySQLInstance{SourceID: "source1"})
	require.NoError(t, cfg.adjust())
	require.Equal(t, DefaultMydumperConfig(), *cfg.MySQLInstances[0].Mydumper)

	cfg.MySQLInstances[0].Mydumper = &MydumperConfig{MydumperPath: "test"}
	require.NoError(t, cfg.adjust())
	require.Equal(t, defaultChunkFilesize, cfg.MySQLInstances[0].Mydumper.ChunkFilesize)
}

func TestExclusiveAndWrongExprFilterFields(t *testing.T) {
	t.Parallel()

	cfg := NewTaskConfig()
	cfg.Name = "test"
	cfg.TaskMode = ModeAll
	cfg.TargetDB = &dbconfig.DBConfig{}
	cfg.MySQLInstances = append(cfg.MySQLInstances, &MySQLInstance{SourceID: "source1"})
	require.NoError(t, cfg.adjust())

	cfg.ExprFilter["test-insert"] = &ExpressionFilter{
		Schema:          "db",
		Table:           "tbl",
		InsertValueExpr: "a > 1",
	}
	cfg.ExprFilter["test-update-only-old"] = &ExpressionFilter{
		Schema:             "db",
		Table:              "tbl",
		UpdateOldValueExpr: "a > 1",
	}
	cfg.ExprFilter["test-update-only-new"] = &ExpressionFilter{
		Schema:             "db",
		Table:              "tbl",
		UpdateNewValueExpr: "a > 1",
	}
	cfg.ExprFilter["test-update"] = &ExpressionFilter{
		Schema:             "db",
		Table:              "tbl",
		UpdateOldValueExpr: "a > 1",
		UpdateNewValueExpr: "a > 1",
	}
	cfg.ExprFilter["test-delete"] = &ExpressionFilter{
		Schema:          "db",
		Table:           "tbl",
		DeleteValueExpr: "a > 1",
	}
	cfg.MySQLInstances[0].ExpressionFilters = []string{
		"test-insert",
		"test-update-only-old",
		"test-update-only-new",
		"test-update",
		"test-delete",
	}
	require.NoError(t, cfg.adjust())

	cfg.ExprFilter["both-field"] = &ExpressionFilter{
		Schema:          "db",
		Table:           "tbl",
		InsertValueExpr: "a > 1",
		DeleteValueExpr: "a > 1",
	}
	cfg.MySQLInstances[0].ExpressionFilters = append(cfg.MySQLInstances[0].ExpressionFilters, "both-field")
	err := cfg.adjust()
	require.True(t, terror.ErrConfigExprFilterManyExpr.Equal(err))

	delete(cfg.ExprFilter, "both-field")
	cfg.ExprFilter["wrong"] = &ExpressionFilter{
		Schema:          "db",
		Table:           "tbl",
		DeleteValueExpr: "a >",
	}
	length := len(cfg.MySQLInstances[0].ExpressionFilters)
	cfg.MySQLInstances[0].ExpressionFilters[length-1] = "wrong"
	err = cfg.adjust()
	require.True(t, terror.ErrConfigExprFilterWrongGrammar.Equal(err))
}

func TestTaskConfigForDowngrade(t *testing.T) {
	t.Parallel()

	cfg := NewTaskConfig()
	err := cfg.Decode(correctTaskConfig)
	require.NoError(t, err)

	cfgForDowngrade := NewTaskConfigForDowngrade(cfg)

	// make sure all new field were added
	cfgReflect := reflect.Indirect(reflect.ValueOf(cfg))
	cfgForDowngradeReflect := reflect.Indirect(reflect.ValueOf(cfgForDowngrade))
	// without flag, collation_compatible, experimental, validator
	require.Equal(t, cfgForDowngradeReflect.NumField()+4, cfgReflect.NumField())

	// make sure all field were copied
	cfgForClone := &TaskConfigForDowngrade{}
	Clone(cfgForClone, cfg)
	require.Equal(t, cfgForClone, cfgForDowngrade)
}

// Clone clones src to dest.
func Clone(dest, src interface{}) {
	cloneValues(reflect.ValueOf(dest), reflect.ValueOf(src))
}

// cloneValues clone src to dest recursively.
// Note: pointer still use shallow copy.
func cloneValues(dest, src reflect.Value) {
	destType := dest.Type()
	srcType := src.Type()
	if destType.Kind() == reflect.Ptr {
		destType = destType.Elem()
	}
	if srcType.Kind() == reflect.Ptr {
		srcType = srcType.Elem()
	}

	if destType.Kind() == reflect.Map {
		destMap := reflect.MakeMap(destType)
		for _, k := range src.MapKeys() {
			if src.MapIndex(k).Type().Kind() == reflect.Ptr {
				newVal := reflect.New(destType.Elem().Elem())
				cloneValues(newVal, src.MapIndex(k))
				destMap.SetMapIndex(k, newVal)
			} else {
				cloneValues(destMap.MapIndex(k).Addr(), src.MapIndex(k).Addr())
			}
		}
		dest.Set(destMap)
		return
	}

	if destType.Kind() == reflect.Slice {
		slice := reflect.MakeSlice(destType, src.Len(), src.Cap())
		for i := 0; i < src.Len(); i++ {
			if slice.Index(i).Type().Kind() == reflect.Ptr {
				newVal := reflect.New(slice.Index(i).Type().Elem())
				cloneValues(newVal, src.Index(i))
				slice.Index(i).Set(newVal)
			} else {
				cloneValues(slice.Index(i).Addr(), src.Index(i).Addr())
			}
		}
		dest.Set(slice)
		return
	}

	destFieldsMap := map[string]int{}
	for i := 0; i < destType.NumField(); i++ {
		destFieldsMap[destType.Field(i).Name] = i
	}
	for i := 0; i < srcType.NumField(); i++ {
		if j, ok := destFieldsMap[srcType.Field(i).Name]; ok {
			destField := dest.Elem().Field(j)
			srcField := src.Elem().Field(i)
			destFieldType := destField.Type()
			srcFieldType := srcField.Type()
			if destFieldType.Kind() == reflect.Ptr {
				destFieldType = destFieldType.Elem()
			}
			if srcFieldType.Kind() == reflect.Ptr {
				srcFieldType = srcFieldType.Elem()
			}
			if destFieldType != srcFieldType {
				cloneValues(destField, srcField)
			} else {
				destField.Set(srcField)
			}
		}
	}
}

func TestLoadConfigAdjust(t *testing.T) {
	t.Parallel()

	cfg := &LoaderConfig{}
	require.NoError(t, cfg.adjust())
	require.Equal(t, &LoaderConfig{
		PoolSize:            16,
		Dir:                 "",
		SQLMode:             "",
		ImportMode:          "logical",
		OnDuplicate:         "",
		OnDuplicateLogical:  "replace",
		OnDuplicatePhysical: "none",
		ChecksumPhysical:    "required",
		Analyze:             "optional",
	}, cfg)

	// test deprecated OnDuplicate will write to OnDuplicateLogical
	cfg.OnDuplicate = "replace"
	cfg.OnDuplicateLogical = ""
	require.NoError(t, cfg.adjust())
	require.Equal(t, OnDuplicateReplace, cfg.OnDuplicateLogical)

	// test wrong value
	cfg.OnDuplicatePhysical = "wrong"
	err := cfg.adjust()
	require.True(t, terror.ErrConfigInvalidPhysicalDuplicateResolution.Equal(err))
}
