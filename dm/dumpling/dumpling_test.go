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
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/dumpling/export"
	"github.com/pingcap/tidb/pkg/util/filter"
	tidbpromutil "github.com/pingcap/tidb/pkg/util/promutil"
	tfilter "github.com/pingcap/tidb/pkg/util/table-filter"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	dutils "github.com/pingcap/tiflow/dm/pkg/dumpling"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/engine/pkg/promutil"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

const (
	testDumplingSchemaName = "INFORMATION_SCHEMA"
	testDumplingTableName  = "TABLES"
)

func TestSuite(t *testing.T) {
	suite.Run(t, new(testDumplingSuite))
}

type testDumplingSuite struct {
	suite.Suite
	cfg *config.SubTaskConfig
}

func (t *testDumplingSuite) SetupSuite() {
	dir := t.T().TempDir()
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
	t.Require().NoError(log.InitLogger(&log.Config{}))
}

func (t *testDumplingSuite) TestDumpling() {
	dumpling := NewDumpling(t.cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := dumpling.Init(ctx)
	t.Require().NoError(err)
	resultCh := make(chan pb.ProcessResult, 1)

	t.Require().NoError(failpoint.Enable("github.com/pingcap/tiflow/dm/dumpling/dumpUnitProcessNoError", `return(true)`))
	//nolint:errcheck
	defer failpoint.Disable("github.com/pingcap/tiflow/dm/dumpling/dumpUnitProcessNoError")

	dumpling.Process(ctx, resultCh)
	t.Require().Equal(1, len(resultCh))
	result := <-resultCh
	t.Require().False(result.IsCanceled)
	t.Require().Equal(0, len(result.Errors))
	//nolint:errcheck
	failpoint.Disable("github.com/pingcap/tiflow/dm/dumpling/dumpUnitProcessNoError")

	t.Require().NoError(failpoint.Enable("github.com/pingcap/tiflow/dm/dumpling/dumpUnitProcessWithError", `return("unknown error")`))
	// nolint:errcheck
	defer failpoint.Disable("github.com/pingcap/tiflow/dm/dumpling/dumpUnitProcessWithError")

	// return error
	dumpling.Process(ctx, resultCh)
	t.Require().Equal(1, len(resultCh))
	result = <-resultCh
	t.Require().False(result.IsCanceled)
	t.Require().Equal(1, len(result.Errors))
	t.Require().Equal("unknown error", result.Errors[0].Message)
	// nolint:errcheck
	failpoint.Disable("github.com/pingcap/tiflow/dm/dumpling/dumpUnitProcessWithError")

	// cancel
	t.Require().NoError(failpoint.Enable("github.com/pingcap/tiflow/dm/dumpling/dumpUnitProcessCancel", `return("unknown error")`))
	// nolint:errcheck
	defer failpoint.Disable("github.com/pingcap/tiflow/dm/dumpling/dumpUnitProcessCancel")
	t.Require().NoError(failpoint.Enable("github.com/pingcap/tiflow/dm/dumpling/dumpUnitProcessForever", `return(true)`))
	//nolint:errcheck
	defer failpoint.Disable("github.com/pingcap/tiflow/dm/dumpling/dumpUnitProcessForever")

	ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel2()
	dumpling.Process(ctx2, resultCh)
	t.Require().Equal(1, len(resultCh))
	result = <-resultCh
	t.Require().True(result.IsCanceled)
	t.Require().Equal(1, len(result.Errors))
	t.Require().Regexp(".*context deadline exceeded.*", result.Errors[0].String())
}

func (t *testDumplingSuite) TestDefaultConfig() {
	dumpling := NewDumpling(t.cfg)
	ctx := context.Background()
	t.Require().NoError(dumpling.Init(ctx))
	t.Require().NotEqual(export.UnspecifiedSize, dumpling.dumpConfig.StatementSize)
	t.Require().NotEqual(export.UnspecifiedSize, dumpling.dumpConfig.Rows)
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

func (t *testDumplingSuite) TestParseArgsWontOverwrite() {
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
	t.Require().NoError(err)

	t.Require().Equal(uint64(4000), exportCfg.StatementSize)
	t.Require().Equal(uint64(1*units.MiB), exportCfg.FileSize)

	f, err2 := tfilter.ParseMySQLReplicationRules(rules)
	t.Require().NoError(err2)
	t.Require().Equal(tfilter.CaseInsensitive(f), exportCfg.TableFilter)

	t.Require().Equal("lock", exportCfg.Consistency)
}

func (t *testDumplingSuite) TestConstructArgs() {
	ctx := context.Background()

	mock := conn.InitMockDB(t.T())
	mock.ExpectQuery("SELECT cast\\(TIMEDIFF\\(NOW\\(6\\), UTC_TIMESTAMP\\(6\\)\\) as time\\);").
		WillReturnRows(sqlmock.NewRows([]string{""}).AddRow("01:00:00"))

	cfg := &config.SubTaskConfig{}
	cfg.ExtraArgs = `--statement-size=100 --where "t>10" --threads 8 -F 50B`
	d := NewDumpling(cfg)
	exportCfg, err := d.constructArgs(ctx)
	t.Require().NoError(err)
	t.Require().Equal(uint64(100), exportCfg.StatementSize)
	t.Require().Equal("t>10", exportCfg.Where)
	t.Require().Equal(8, exportCfg.Threads)
	t.Require().Equal(uint64(50), exportCfg.FileSize)
	t.Require().NotNil(exportCfg.SessionParams)
	t.Require().Equal("+01:00", exportCfg.SessionParams["time_zone"])
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

func TestBAlist(t *testing.T) {
	ctx := context.Background()

	// case sensitive and set block-allow-list
	cfg := genDumpCfg(t)
	cfg.CaseSensitive = true
	m := NewDumpling(cfg)
	err := m.Init(ctx)
	require.NoError(t, err)
	tableFilter, err := tfilter.ParseMySQLReplicationRules(cfg.BAList)
	require.NoError(t, err)
	require.Equal(t, tableFilter, m.dumpConfig.TableFilter)

	// case insensitive and set block-allow-list
	cfg = genDumpCfg(t)
	cfg.CaseSensitive = false
	m = NewDumpling(cfg)
	err = m.Init(ctx)
	require.NoError(t, err)
	tableFilter, err = tfilter.ParseMySQLReplicationRules(cfg.BAList)
	require.NoError(t, err)
	tableFilter = tfilter.CaseInsensitive(tableFilter)
	require.Equal(t, tableFilter, m.dumpConfig.TableFilter)

	// case sensitive and not set block-allow-list
	cfg = genDumpCfg(t)
	cfg.BAList = nil
	cfg.CaseSensitive = true
	m = NewDumpling(cfg)
	err = m.Init(ctx)
	require.NoError(t, err)
	tableFilter, err = tfilter.Parse(dutils.DefaultTableFilter)
	require.NoError(t, err)
	require.Equal(t, tableFilter, m.dumpConfig.TableFilter)

	// case insensitive and not set block-allow-list
	cfg = genDumpCfg(t)
	cfg.BAList = nil
	cfg.CaseSensitive = false
	m = NewDumpling(cfg)
	err = m.Init(ctx)
	require.NoError(t, err)
	tableFilter, err = tfilter.Parse(dutils.DefaultTableFilter)
	require.NoError(t, err)
	tableFilter = tfilter.CaseInsensitive(tableFilter)
	require.Equal(t, tableFilter, m.dumpConfig.TableFilter)
}
