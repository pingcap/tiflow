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

package cmd

import (
	"io/ioutil"
	"path/filepath"
	"testing"

	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-tools/pkg/filter"

	"github.com/pingcap/ticdc/pkg/config"

	"github.com/pingcap/check"
)

func TestSuite(t *testing.T) { check.TestingT(t) }

type decodeFileSuite struct{}

var _ = check.Suite(&decodeFileSuite{})

func (s *decodeFileSuite) TestCanDecodeTOML(c *check.C) {
	dir := c.MkDir()
	path := filepath.Join(dir, "config.toml")
	content := `
case-sensitive = false

[filter]
ignore-txn-start-ts = [1, 2]
ddl-white-list = [1, 2]
ignore-dbs = ["test", "sys"]
do-dbs = ["test1", "sys1"]
do-tables = [
	{db-name = "test", tbl-name = "tbl1"},
	{db-name = "test", tbl-name = "tbl2"},
]
ignore-tables = [
	{db-name = "test", tbl-name = "tbl3"},
	{db-name = "test", tbl-name = "tbl4"},
]

[mounter]
worker-num = 64

[sink]
dispatch-rules = [
	{db-name = "test", tbl-name = "tbl3", rule = "ts"},
	{db-name = "test", tbl-name = "tbl4", rule = "rowid"},
]

[cyclic-replication]
enable = true
replica-id = 1
filter-replica-ids = [2,3]
id-buckets = 4
sync-ddl = true
`
	err := ioutil.WriteFile(path, []byte(content), 0644)
	c.Assert(err, check.IsNil)

	cfg := config.GetDefaultReplicaConfig()
	err = strictDecodeFile(path, "cdc", &cfg)
	c.Assert(err, check.IsNil)

	c.Assert(cfg.CaseSensitive, check.IsFalse)
	c.Assert(cfg.Filter, check.DeepEquals, &config.FilterConfig{
		IgnoreTxnStartTs: []uint64{1, 2},
		DDLWhitelist:     []model.ActionType{1, 2},
		Rules: &filter.Rules{
			IgnoreDBs: []string{"test", "sys"},
			DoDBs:     []string{"test1", "sys1"},
			DoTables: []*filter.Table{
				{Schema: "test", Name: "tbl1"},
				{Schema: "test", Name: "tbl2"},
			},
			IgnoreTables: []*filter.Table{
				{Schema: "test", Name: "tbl3"},
				{Schema: "test", Name: "tbl4"},
			},
		},
	})
	c.Assert(cfg.Mounter, check.DeepEquals, &config.MounterConfig{
		WorkerNum: 64,
	})
	c.Assert(cfg.Sink, check.DeepEquals, &config.SinkConfig{
		DispatchRules: []*config.DispatchRule{
			{Rule: "ts", Table: filter.Table{Schema: "test", Name: "tbl3"}},
			{Rule: "rowid", Table: filter.Table{Schema: "test", Name: "tbl4"}},
		},
	})
	c.Assert(cfg.Cyclic, check.DeepEquals, &config.CyclicConfig{
		Enable:          true,
		ReplicaID:       1,
		FilterReplicaID: []uint64{2, 3},
		IDBuckets:       4,
		SyncDDL:         true,
	})
}

func (s *decodeFileSuite) TestAndWriteExampleTOML(c *check.C) {
	content := `
# 指定配置文件中涉及的库名、表名是否为大小写敏感的
# 该配置会同时影响 filter 和 sink 相关配置，默认为 true

# Specify whether the schema name and table name in this configuration file are case sensitive
# This configuration will affect both filter and sink related configurations, the default is true
case-sensitive = true

[filter]
# 忽略哪些 StartTs 的事务
# Transactions with the following StartTs will be ignored
ignore-txn-start-ts = [1, 2]

# 同步哪些库
# The following databases(schema) will be replicated
do-dbs = ["test1", "sys1"]

# 同步哪些表
# The following tables will be replicated
do-tables = [
	{db-name = "test", tbl-name = "tbl1"},
	{db-name = "test", tbl-name = "tbl2"},
]

# 忽略哪些库
# The following databases(schema) will be ignored
ignore-dbs = ["test", "sys"]

# 忽略哪些表
# The following tables will be ignored
ignore-tables = [
	{db-name = "test", tbl-name = "tbl3"},
	{db-name = "test", tbl-name = "tbl4"},
]

[mounter]
# mounter 线程数
# the thread number of the the mounter
worker-num = 16

[sink]
# 对于 MQ 类的 Sink，可以通过 dispatch-rules 配置 event 分发规则
# 分发规则支持 default, ts, rowid, table
# For MQ Sinks, you can configure event distribution rules through dispatch-rules
# Distribution rules support default, ts, rowid and table
dispatch-rules = [
	{db-name = "test", tbl-name = "tbl3", rule = "ts"},
	{db-name = "test", tbl-name = "tbl4", rule = "rowid"},
]

[cyclic-replication]
# 是否开启环形复制
# Whether to enable cyclic replication
enable = false
# 当前 CDC 的复制 ID
# The replica ID of this capture
replica-id = 1
# 需要过滤掉的复制 ID
# The replica ID should be ignored
filter-replica-ids = [2,3]
# 是否同步 DDL
# Whether to replicate DDL
sync-ddl = true
`
	err := ioutil.WriteFile("changefeed.toml", []byte(content), 0644)
	c.Assert(err, check.IsNil)

	cfg := config.GetDefaultReplicaConfig()
	err = strictDecodeFile("changefeed.toml", "cdc", &cfg)
	c.Assert(err, check.IsNil)

	c.Assert(cfg.CaseSensitive, check.IsTrue)
	c.Assert(cfg.Filter, check.DeepEquals, &config.FilterConfig{
		IgnoreTxnStartTs: []uint64{1, 2},
		Rules: &filter.Rules{
			IgnoreDBs: []string{"test", "sys"},
			DoDBs:     []string{"test1", "sys1"},
			DoTables: []*filter.Table{
				{Schema: "test", Name: "tbl1"},
				{Schema: "test", Name: "tbl2"},
			},
			IgnoreTables: []*filter.Table{
				{Schema: "test", Name: "tbl3"},
				{Schema: "test", Name: "tbl4"},
			},
		},
	})
	c.Assert(cfg.Mounter, check.DeepEquals, &config.MounterConfig{
		WorkerNum: 16,
	})
	c.Assert(cfg.Sink, check.DeepEquals, &config.SinkConfig{
		DispatchRules: []*config.DispatchRule{
			{Rule: "ts", Table: filter.Table{Schema: "test", Name: "tbl3"}},
			{Rule: "rowid", Table: filter.Table{Schema: "test", Name: "tbl4"}},
		},
	})
	c.Assert(cfg.Cyclic, check.DeepEquals, &config.CyclicConfig{
		Enable:          false,
		ReplicaID:       1,
		FilterReplicaID: []uint64{2, 3},
		SyncDDL:         true,
	})
}

func (s *decodeFileSuite) TestShouldReturnErrForUnknownCfgs(c *check.C) {
	dir := c.MkDir()
	path := filepath.Join(dir, "config.toml")
	content := `filter-case-insensitive = true`
	err := ioutil.WriteFile(path, []byte(content), 0644)
	c.Assert(err, check.IsNil)

	cfg := config.GetDefaultReplicaConfig()
	err = strictDecodeFile(path, "cdc", &cfg)
	c.Assert(err, check.NotNil)
	c.Assert(err, check.ErrorMatches, ".*unknown config.*")
}
