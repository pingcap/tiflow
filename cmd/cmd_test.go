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
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/pingcap/check"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/util/testleak"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/spf13/cobra"
	pd "github.com/tikv/pd/client"
)

func TestSuite(t *testing.T) { check.TestingT(t) }

type decodeFileSuite struct{}

var _ = check.Suite(&decodeFileSuite{})

func (s *decodeFileSuite) TestCanDecodeTOML(c *check.C) {
	defer testleak.AfterTest(c)()
	dir := c.MkDir()
	path := filepath.Join(dir, "config.toml")
	content := `
case-sensitive = false

[filter]
ignore-txn-start-ts = [1, 2]
ddl-allow-list = [1, 2]
rules = ['*.*', '!test.*']

[mounter]
worker-num = 64

[sink]
dispatchers = [
	{matcher = ['test1.*', 'test2.*'], dispatcher = "ts"},
	{matcher = ['test3.*', 'test4.*'], dispatcher = "rowid"},
]
protocol = "default"

[cyclic-replication]
enable = true
replica-id = 1
filter-replica-ids = [2,3]
id-buckets = 4
sync-ddl = true

[scheduler]
type = "manual"
polling-time = 5
`
	err := ioutil.WriteFile(path, []byte(content), 0o644)
	c.Assert(err, check.IsNil)

	cfg := config.GetDefaultReplicaConfig()
	err = strictDecodeFile(path, "cdc", &cfg)
	c.Assert(err, check.IsNil)

	c.Assert(cfg.CaseSensitive, check.IsFalse)
	c.Assert(cfg.Filter, check.DeepEquals, &config.FilterConfig{
		IgnoreTxnStartTs: []uint64{1, 2},
		DDLAllowlist:     []model.ActionType{1, 2},
		Rules:            []string{"*.*", "!test.*"},
	})
	c.Assert(cfg.Mounter, check.DeepEquals, &config.MounterConfig{
		WorkerNum: 64,
	})
	c.Assert(cfg.Sink, check.DeepEquals, &config.SinkConfig{
		DispatchRules: []*config.DispatchRule{
			{Dispatcher: "ts", Matcher: []string{"test1.*", "test2.*"}},
			{Dispatcher: "rowid", Matcher: []string{"test3.*", "test4.*"}},
		},
		Protocol: "default",
	})
	c.Assert(cfg.Cyclic, check.DeepEquals, &config.CyclicConfig{
		Enable:          true,
		ReplicaID:       1,
		FilterReplicaID: []uint64{2, 3},
		IDBuckets:       4,
		SyncDDL:         true,
	})
	c.Assert(cfg.Scheduler, check.DeepEquals, &config.SchedulerConfig{
		Tp:          "manual",
		PollingTime: 5,
	})
}

func (s *decodeFileSuite) TestAndWriteExampleTOML(c *check.C) {
	defer testleak.AfterTest(c)()
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

# 过滤器规则
# 过滤规则语法：https://docs.pingcap.com/zh/tidb/stable/table-filter#%E8%A1%A8%E5%BA%93%E8%BF%87%E6%BB%A4%E8%AF%AD%E6%B3%95
# The rules of the filter
# Filter rules syntax: https://docs.pingcap.com/tidb/stable/table-filter#syntax
rules = ['*.*', '!test.*']

[mounter]
# mounter 线程数
# the thread number of the the mounter
worker-num = 16

[sink]
# 对于 MQ 类的 Sink，可以通过 dispatchers 配置 event 分发器
# 分发器支持 default, ts, rowid, table 四种
# For MQ Sinks, you can configure event distribution rules through dispatchers
# Dispatchers support default, ts, rowid and table
dispatchers = [
	{matcher = ['test1.*', 'test2.*'], dispatcher = "ts"},
	{matcher = ['test3.*', 'test4.*'], dispatcher = "rowid"},
]
# 对于 MQ 类的 Sink，可以指定消息的协议格式
# 协议目前支持 default, canal, avro 和 maxwell 四种，default 为 ticdc-open-protocol
# For MQ Sinks, you can configure the protocol of the messages sending to MQ
# Currently the protocol support default, canal, avro and maxwell. Default is ticdc-open-protocol
protocol = "default"

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
	err := ioutil.WriteFile("changefeed.toml", []byte(content), 0o644)
	c.Assert(err, check.IsNil)

	cfg := config.GetDefaultReplicaConfig()
	err = strictDecodeFile("changefeed.toml", "cdc", &cfg)
	c.Assert(err, check.IsNil)

	c.Assert(cfg.CaseSensitive, check.IsTrue)
	c.Assert(cfg.Filter, check.DeepEquals, &config.FilterConfig{
		IgnoreTxnStartTs: []uint64{1, 2},
		Rules:            []string{"*.*", "!test.*"},
	})
	c.Assert(cfg.Mounter, check.DeepEquals, &config.MounterConfig{
		WorkerNum: 16,
	})
	c.Assert(cfg.Sink, check.DeepEquals, &config.SinkConfig{
		DispatchRules: []*config.DispatchRule{
			{Dispatcher: "ts", Matcher: []string{"test1.*", "test2.*"}},
			{Dispatcher: "rowid", Matcher: []string{"test3.*", "test4.*"}},
		},
		Protocol: "default",
	})
	c.Assert(cfg.Cyclic, check.DeepEquals, &config.CyclicConfig{
		Enable:          false,
		ReplicaID:       1,
		FilterReplicaID: []uint64{2, 3},
		SyncDDL:         true,
	})
}

func (s *decodeFileSuite) TestShouldReturnErrForUnknownCfgs(c *check.C) {
	defer testleak.AfterTest(c)()
	dir := c.MkDir()
	path := filepath.Join(dir, "config.toml")
	content := `filter-case-insensitive = true`
	err := ioutil.WriteFile(path, []byte(content), 0o644)
	c.Assert(err, check.IsNil)

	cfg := config.GetDefaultReplicaConfig()
	err = strictDecodeFile(path, "cdc", &cfg)
	c.Assert(err, check.NotNil)
	c.Assert(err, check.ErrorMatches, ".*unknown config.*")
}

type mockPDClient struct {
	pd.Client
	ts uint64
}

func (m *mockPDClient) GetTS(ctx context.Context) (int64, int64, error) {
	return oracle.ExtractPhysical(m.ts), 0, nil
}

type commonUtilSuite struct{}

var _ = check.Suite(&commonUtilSuite{})

func (s *commonUtilSuite) TestConfirmLargeDataGap(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := context.Background()
	currentTs := uint64(423482306736160769) // 2021-03-11 17:59:57.547
	startTs := uint64(423450030227042420)   // 2021-03-10 07:47:52.435
	pdCli = &mockPDClient{ts: currentTs}
	cmd := &cobra.Command{}

	// check start ts more than 1 day before current ts, and type N when confirming
	dir := c.MkDir()
	path := filepath.Join(dir, "confirm.txt")
	err := ioutil.WriteFile(path, []byte("n"), 0o644)
	c.Assert(err, check.IsNil)
	f, err := os.Open(path)
	c.Assert(err, check.IsNil)
	stdin := os.Stdin
	os.Stdin = f
	defer func() {
		os.Stdin = stdin
	}()
	err = confirmLargeDataGap(ctx, cmd, startTs)
	c.Assert(err, check.ErrorMatches, "abort changefeed create or resume")

	// check no confirm works
	originNoConfirm := noConfirm
	noConfirm = true
	defer func() {
		noConfirm = originNoConfirm
	}()
	err = confirmLargeDataGap(ctx, cmd, startTs)
	c.Assert(err, check.IsNil)
	noConfirm = false

	// check start ts more than 1 day before current ts, and type Y when confirming
	err = ioutil.WriteFile(path, []byte("Y"), 0o644)
	c.Assert(err, check.IsNil)
	f, err = os.Open(path)
	c.Assert(err, check.IsNil)
	os.Stdin = f
	err = confirmLargeDataGap(ctx, cmd, startTs)
	c.Assert(err, check.IsNil)

	// check start ts does not exceed threshold
	pdCli = &mockPDClient{ts: startTs}
	err = confirmLargeDataGap(ctx, cmd, startTs)
	c.Assert(err, check.IsNil)
}
