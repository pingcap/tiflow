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

package dumpling

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/docker/go-units"
	"github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/dumpling/export"
	"github.com/pingcap/tidb/pkg/util/filter"
	tidbpromutil "github.com/pingcap/tidb/pkg/util/promutil"
	tfilter "github.com/pingcap/tidb/pkg/util/table-filter"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/engine/pkg/promutil"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

var _ = check.Suite(&testDumplingSuite{})

const (
	testDumplingSchemaName = "INFORMATION_SCHEMA"
	testDumplingTableName  = "TABLES"
)

func TestSuite(t *testing.T) {
	check.TestingT(t)
}

type testDumplingSuite struct {
	cfg *config.SubTaskConfig
}

func (t *testDumplingSuite) SetUpSuite(c *check.C) {
	dir := c.MkDir()
	t.cfg = &config.SubTaskConfig{
		Name:     "dumpling_ut",
		Timezone: "UTC",
		From:     config.GetDBConfigForTest(),
		LoaderConfig: config.LoaderConfig{
			Dir: dir,
		},
		BAList: &filter.Rules{
			DoDBs: []string{testDumplingSchemaName},
			DoTables: []*filter.Table{{
				Schema: testDumplingSchemaName,
				Name:   testDumplingTableName,
			}},
		},
	}
	c.Assert(log.InitLogger(&log.Config{}), check.IsNil)
}

func (t *testDumplingSuite) TestDumpling(c *check.C) {
	dumpling := NewDumpling(t.cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := dumpling.Init(ctx)
	c.Assert(err, check.IsNil)
	resultCh := make(chan pb.ProcessResult, 1)

	c.Assert(failpoint.Enable("github.com/pingcap/tiflow/dm/dumpling/dumpUnitProcessNoError", `return(true)`), check.IsNil)
	//nolint:errcheck
	defer failpoint.Disable("github.com/pingcap/tiflow/dm/dumpling/dumpUnitProcessNoError")

	dumpling.Process(ctx, resultCh)
	c.Assert(len(resultCh), check.Equals, 1)
	result := <-resultCh
	c.Assert(result.IsCanceled, check.IsFalse)
	c.Assert(len(result.Errors), check.Equals, 0)
	//nolint:errcheck
	failpoint.Disable("github.com/pingcap/tiflow/dm/dumpling/dumpUnitProcessNoError")

	c.Assert(failpoint.Enable("github.com/pingcap/tiflow/dm/dumpling/dumpUnitProcessWithError", `return("unknown error")`), check.IsNil)
	// nolint:errcheck
	defer failpoint.Disable("github.com/pingcap/tiflow/dm/dumpling/dumpUnitProcessWithError")

	// return error
	dumpling.Process(ctx, resultCh)
	c.Assert(len(resultCh), check.Equals, 1)
	result = <-resultCh
	c.Assert(result.IsCanceled, check.IsFalse)
	c.Assert(len(result.Errors), check.Equals, 1)
	c.Assert(result.Errors[0].Message, check.Equals, "unknown error")
	// nolint:errcheck
	failpoint.Disable("github.com/pingcap/tiflow/dm/dumpling/dumpUnitProcessWithError")

	// cancel
	c.Assert(failpoint.Enable("github.com/pingcap/tiflow/dm/dumpling/dumpUnitProcessCancel", `return("unknown error")`), check.IsNil)
	// nolint:errcheck
	defer failpoint.Disable("github.com/pingcap/tiflow/dm/dumpling/dumpUnitProcessCancel")
	c.Assert(failpoint.Enable("github.com/pingcap/tiflow/dm/dumpling/dumpUnitProcessForever", `return(true)`), check.IsNil)
	//nolint:errcheck
	defer failpoint.Disable("github.com/pingcap/tiflow/dm/dumpling/dumpUnitProcessForever")

	ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel2()
	dumpling.Process(ctx2, resultCh)
	c.Assert(len(resultCh), check.Equals, 1)
	result = <-resultCh
	c.Assert(result.IsCanceled, check.IsTrue)
	c.Assert(len(result.Errors), check.Equals, 1)
	c.Assert(result.Errors[0].String(), check.Matches, ".*context deadline exceeded.*")
}

func (t *testDumplingSuite) TestDefaultConfig(c *check.C) {
	dumpling := NewDumpling(t.cfg)
	ctx := context.Background()
	c.Assert(dumpling.Init(ctx), check.IsNil)
	c.Assert(dumpling.dumpConfig.StatementSize, check.Not(check.Equals), export.UnspecifiedSize)
	c.Assert(dumpling.dumpConfig.Rows, check.Not(check.Equals), export.UnspecifiedSize)
}

func TestCallStatus(t *testing.T) {
	cfg := genDumpCfg(t)
	m := NewDumpling(cfg)
	m.metricProxies = defaultMetricProxies
	ctx := context.Background()

	dumpConf := export.DefaultConfig()
	dumpConf.PromFactory = promutil.NewWrappingFactory(
		promutil.NewPromFactory(),
		"",
		prometheus.Labels{
			"task": m.cfg.Name, "source_id": m.cfg.SourceID,
		},
	)
	dumpConf.PromRegistry = tidbpromutil.NewDefaultRegistry()

	s := m.Status(nil).(*pb.DumpStatus)
	require.Equal(t, s.CompletedTables, float64(0))
	require.Equal(t, s.FinishedBytes, float64(0))
	require.Equal(t, s.FinishedRows, float64(0))
	require.Equal(t, s.EstimateTotalRows, float64(0))
	require.Equal(t, s.Progress, "")
	require.Equal(t, s.Bps, int64(0))

	// NewDumper is the only way we can set conf to Dumper, but it will return error. so we just ignore the error
	dumpling, _ := export.NewDumper(ctx, dumpConf)
	m.core = dumpling

	m.Close()
	s = m.Status(nil).(*pb.DumpStatus)
	require.Equal(t, s.CompletedTables, float64(0))
	require.Equal(t, s.FinishedBytes, float64(0))
	require.Equal(t, s.FinishedRows, float64(0))
	require.Equal(t, s.EstimateTotalRows, float64(0))
	require.Equal(t, s.Progress, "")
	require.Equal(t, s.Bps, int64(0))
}

func (t *testDumplingSuite) TestParseArgsWontOverwrite(c *check.C) {
	cfg := &config.SubTaskConfig{
		Timezone: "UTC",
	}
	cfg.ChunkFilesize = "1"
	rules := &filter.Rules{
		DoDBs: []string{"unit_test"},
	}
	cfg.BAList = rules
	// make sure we enter `parseExtraArgs`
	cfg.ExtraArgs = "-s=4000 --consistency lock"

	d := NewDumpling(cfg)
	exportCfg, err := d.constructArgs(context.Background())
	c.Assert(err, check.IsNil)

	c.Assert(exportCfg.StatementSize, check.Equals, uint64(4000))
	c.Assert(exportCfg.FileSize, check.Equals, uint64(1*units.MiB))

	f, err2 := tfilter.ParseMySQLReplicationRules(rules)
	c.Assert(err2, check.IsNil)
	c.Assert(exportCfg.TableFilter, check.DeepEquals, tfilter.CaseInsensitive(f))

	c.Assert(exportCfg.Consistency, check.Equals, "lock")
}

func (t *testDumplingSuite) TestConstructArgs(c *check.C) {
	ctx := context.Background()

	mock := conn.InitMockDB(c)
	mock.ExpectQuery("SELECT cast\\(TIMEDIFF\\(NOW\\(6\\), UTC_TIMESTAMP\\(6\\)\\) as time\\);").
		WillReturnRows(sqlmock.NewRows([]string{""}).AddRow("01:00:00"))

	cfg := &config.SubTaskConfig{}
	cfg.ExtraArgs = `--statement-size=100 --where "t>10" --threads 8 -F 50B`
	d := NewDumpling(cfg)
	exportCfg, err := d.constructArgs(ctx)
	c.Assert(err, check.IsNil)
	c.Assert(exportCfg.StatementSize, check.Equals, uint64(100))
	c.Assert(exportCfg.Where, check.Equals, "t>10")
	c.Assert(exportCfg.Threads, check.Equals, 8)
	c.Assert(exportCfg.FileSize, check.Equals, uint64(50))
	c.Assert(exportCfg.SessionParams, check.NotNil)
	c.Assert(exportCfg.SessionParams["time_zone"], check.Equals, "+01:00")
}

func genDumpCfg(t *testing.T) *config.SubTaskConfig {
	t.Helper()

	dir := t.TempDir()
	return &config.SubTaskConfig{
		Name:     "dumpling_ut",
		Timezone: "UTC",
		From:     config.GetDBConfigForTest(),
		LoaderConfig: config.LoaderConfig{
			Dir: dir,
		},
		BAList: &filter.Rules{
			DoDBs: []string{testDumplingSchemaName},
			DoTables: []*filter.Table{{
				Schema: testDumplingSchemaName,
				Name:   testDumplingTableName,
			}},
		},
	}
}
