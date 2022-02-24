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
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/util/testleak"
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

func (s *decodeFileSuite) TestAndWriteExampleReplicaTOML(c *check.C) {
	defer testleak.AfterTest(c)()
	cfg := config.GetDefaultReplicaConfig()
	err := strictDecodeFile("changefeed.toml", "cdc", &cfg)
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

func (s *decodeFileSuite) TestAndWriteExampleServerTOML(c *check.C) {
	defer testleak.AfterTest(c)()
	cfg := config.GetDefaultServerConfig()
	err := strictDecodeFile("ticdc.toml", "cdc", &cfg)
	c.Assert(err, check.IsNil)
	defcfg := config.GetDefaultServerConfig()
	defcfg.AdvertiseAddr = "127.0.0.1:8300"
	defcfg.LogFile = "/tmp/ticdc/ticdc.log"
	c.Assert(cfg, check.DeepEquals, defcfg)
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

func (s *decodeFileSuite) TestVerifyReplicaConfig(c *check.C) {
	defer testleak.AfterTest(c)()

	dir := c.MkDir()
	path := filepath.Join(dir, "config.toml")
	content := `
	[filter]
	rules = ['*.*', '!test.*']`
	err := ioutil.WriteFile(path, []byte(content), 0o644)
	c.Assert(err, check.IsNil)

	cfg := config.GetDefaultReplicaConfig()
	err = verifyReplicaConfig(path, "cdc", cfg)
	c.Assert(err, check.IsNil)

	path = filepath.Join(dir, "config1.toml")
	content = `
	[filter]
	rules = ['*.*', '!test.*','rtest1']`
	err = ioutil.WriteFile(path, []byte(content), 0o644)
	c.Assert(err, check.IsNil)

	cfg = config.GetDefaultReplicaConfig()
	err = verifyReplicaConfig(path, "cdc", cfg)
	c.Assert(err, check.NotNil)
	c.Assert(err, check.ErrorMatches, ".*CDC:ErrFilterRuleInvalid.*")
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

// test cobra.NoArgs add in command can filter out some unexpected parameters
func (s *commonUtilSuite) TestNoArgs(c *check.C) {
	defer testleak.AfterTest(c)
	cmd := newCreateChangefeedCommand()
	// there is a DBC space before the flag -c
	flags := []string{"none", "--sink-uri=blackhole://", "　-c=", "aa"}
	os.Args = flags
	err := cmd.Execute()
	c.Assert(err, check.NotNil)
	c.Assert(err, check.ErrorMatches, ".*unknown command.*u3000-c.*for.*create.*")

	// there is an unknown flag "aa" after -c
	flags = []string{"none", "--sink-uri=blackhole://", "-c=test1", "aa"}
	os.Args = flags
	err = cmd.Execute()
	c.Assert(err, check.NotNil)
	c.Assert(err, check.ErrorMatches, ".*unknown command.*aa.*for.*create.*")

	// there is a "国" before the flag -start-ts
	flags = []string{"none", "--sink-uri=blackhole://", "-c=test1", "国--start-ts=0"}
	os.Args = flags
	err = cmd.Execute()
	c.Assert(err, check.NotNil)
	c.Assert(err, check.ErrorMatches, ".*unknown command.*国.*for.*create.*")
}
