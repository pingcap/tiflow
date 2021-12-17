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
	"github.com/pingcap/tidb-tools/pkg/filter"
	tfilter "github.com/pingcap/tidb-tools/pkg/table-filter"
	"github.com/pingcap/tidb/dumpling/export"

	"github.com/pingcap/ticdc/dm/dm/config"
	"github.com/pingcap/ticdc/dm/dm/pb"
	"github.com/pingcap/ticdc/dm/pkg/conn"
	"github.com/pingcap/ticdc/dm/pkg/log"

	. "github.com/pingcap/check"
)

var _ = Suite(&testDumplingSuite{})

const (
	testDumplingSchemaName = "INFORMATION_SCHEMA"
	testDumplingTableName  = "TABLES"
)

func TestSuite(t *testing.T) {
	TestingT(t)
}

type testDumplingSuite struct {
	cfg *config.SubTaskConfig
}

func (t *testDumplingSuite) SetUpSuite(c *C) {
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
	c.Assert(log.InitLogger(&log.Config{}), IsNil)
}

func (t *testDumplingSuite) TestDumpling(c *C) {
	dumpling := NewDumpling(t.cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := dumpling.Init(ctx)
	c.Assert(err, IsNil)
	resultCh := make(chan pb.ProcessResult, 1)

	c.Assert(failpoint.Enable("github.com/pingcap/ticdc/dm/dumpling/dumpUnitProcessNoError", `return(true)`), IsNil)
	//nolint:errcheck
	defer failpoint.Disable("github.com/pingcap/ticdc/dm/dumpling/dumpUnitProcessNoError")

	dumpling.Process(ctx, resultCh)
	c.Assert(len(resultCh), Equals, 1)
	result := <-resultCh
	c.Assert(result.IsCanceled, IsFalse)
	c.Assert(len(result.Errors), Equals, 0)
	//nolint:errcheck
	failpoint.Disable("github.com/pingcap/ticdc/dm/dumpling/dumpUnitProcessNoError")

	c.Assert(failpoint.Enable("github.com/pingcap/ticdc/dm/dumpling/dumpUnitProcessWithError", `return("unknown error")`), IsNil)
	// nolint:errcheck
	defer failpoint.Disable("github.com/pingcap/ticdc/dm/dumpling/dumpUnitProcessWithError")

	// return error
	dumpling.Process(ctx, resultCh)
	c.Assert(len(resultCh), Equals, 1)
	result = <-resultCh
	c.Assert(result.IsCanceled, IsFalse)
	c.Assert(len(result.Errors), Equals, 1)
	c.Assert(result.Errors[0].Message, Equals, "unknown error")
	// nolint:errcheck
	failpoint.Disable("github.com/pingcap/ticdc/dm/dumpling/dumpUnitProcessWithError")

	// cancel
	c.Assert(failpoint.Enable("github.com/pingcap/ticdc/dm/dumpling/dumpUnitProcessCancel", `return("unknown error")`), IsNil)
	// nolint:errcheck
	defer failpoint.Disable("github.com/pingcap/ticdc/dm/dumpling/dumpUnitProcessCancel")
	c.Assert(failpoint.Enable("github.com/pingcap/ticdc/dm/dumpling/dumpUnitProcessForever", `return(true)`), IsNil)
	//nolint:errcheck
	defer failpoint.Disable("github.com/pingcap/ticdc/dm/dumpling/dumpUnitProcessForever")

	ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel2()
	dumpling.Process(ctx2, resultCh)
	c.Assert(len(resultCh), Equals, 1)
	result = <-resultCh
	c.Assert(result.IsCanceled, IsTrue)
	c.Assert(len(result.Errors), Equals, 1)
	c.Assert(result.Errors[0].String(), Matches, ".*context deadline exceeded.*")
}

func (t *testDumplingSuite) TestDefaultConfig(c *C) {
	dumpling := NewDumpling(t.cfg)
	ctx := context.Background()
	c.Assert(dumpling.Init(ctx), IsNil)
	c.Assert(dumpling.dumpConfig.StatementSize, Not(Equals), export.UnspecifiedSize)
	c.Assert(dumpling.dumpConfig.Rows, Not(Equals), export.UnspecifiedSize)
}

func (t *testDumplingSuite) TestParseArgsWontOverwrite(c *C) {
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
	c.Assert(err, IsNil)

	c.Assert(exportCfg.StatementSize, Equals, uint64(4000))
	c.Assert(exportCfg.FileSize, Equals, uint64(1*units.MiB))

	f, err2 := tfilter.ParseMySQLReplicationRules(rules)
	c.Assert(err2, IsNil)
	c.Assert(exportCfg.TableFilter, DeepEquals, tfilter.CaseInsensitive(f))

	c.Assert(exportCfg.Consistency, Equals, "lock")
}

func (t *testDumplingSuite) TestConstructArgs(c *C) {
	ctx := context.Background()

	mock := conn.InitMockDB(c)
	mock.ExpectQuery("SELECT cast\\(TIMEDIFF\\(NOW\\(6\\), UTC_TIMESTAMP\\(6\\)\\) as time\\);").
		WillReturnRows(sqlmock.NewRows([]string{""}).AddRow("01:00:00"))

	cfg := &config.SubTaskConfig{}
	cfg.ExtraArgs = `--statement-size=100 --where "t>10" --threads 8 -F 50B`
	d := NewDumpling(cfg)
	exportCfg, err := d.constructArgs(ctx)
	c.Assert(err, IsNil)
	c.Assert(exportCfg.StatementSize, Equals, uint64(100))
	c.Assert(exportCfg.Where, Equals, "t>10")
	c.Assert(exportCfg.Threads, Equals, 8)
	c.Assert(exportCfg.FileSize, Equals, uint64(50))
	c.Assert(exportCfg.SessionParams, NotNil)
	c.Assert(exportCfg.SessionParams["time_zone"], Equals, "+01:00")
}
