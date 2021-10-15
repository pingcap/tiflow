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
	"strings"

	"github.com/docker/go-units"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-tools/pkg/filter"
	tfilter "github.com/pingcap/tidb-tools/pkg/table-filter"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/pkg/log"
)

func (d *testDumplingSuite) TestParseArgs(c *C) {
	logger := log.L()

	cfg := &config.SubTaskConfig{}
	cfg.ExtraArgs = `--statement-size=100 --where "t>10" --threads 8 -F 50B`
	dumpling := NewDumpling(cfg)
	exportCfg, err := dumpling.constructArgs()
	c.Assert(err, IsNil)
	c.Assert(exportCfg.StatementSize, Equals, uint64(100))
	c.Assert(exportCfg.Where, Equals, "t>10")
	c.Assert(exportCfg.Threads, Equals, 8)
	c.Assert(exportCfg.FileSize, Equals, uint64(50))

	extraArgs := `--threads 16 --skip-tz-utc`
	err = parseExtraArgs(&logger, exportCfg, strings.Fields(extraArgs))
	c.Assert(err, NotNil)
	c.Assert(exportCfg.Threads, Equals, 16)
	c.Assert(exportCfg.StatementSize, Equals, uint64(100))

	// no `--tables-list` or `--filter` specified, match anything
	c.Assert(exportCfg.TableFilter.MatchTable("foo", "bar"), IsTrue)
	c.Assert(exportCfg.TableFilter.MatchTable("bar", "foo"), IsTrue)

	// specify `--tables-list`.
	extraArgs = `--threads 16 --tables-list=foo.bar`
	err = parseExtraArgs(&logger, exportCfg, strings.Fields(extraArgs))
	c.Assert(err, IsNil)
	c.Assert(exportCfg.TableFilter.MatchTable("foo", "bar"), IsTrue)
	c.Assert(exportCfg.TableFilter.MatchTable("bar", "foo"), IsFalse)

	// specify `--tables-list` and `--filter`
	extraArgs = `--threads 16 --tables-list=foo.bar --filter=*.foo`
	err = parseExtraArgs(&logger, exportCfg, strings.Fields(extraArgs))
	c.Assert(err, ErrorMatches, ".*cannot pass --tables-list and --filter together.*")

	// only specify `--filter`.
	extraArgs = `--threads 16 --filter=*.foo`
	err = parseExtraArgs(&logger, exportCfg, strings.Fields(extraArgs))
	c.Assert(err, IsNil)
	c.Assert(exportCfg.TableFilter.MatchTable("foo", "bar"), IsFalse)
	c.Assert(exportCfg.TableFilter.MatchTable("bar", "foo"), IsTrue)

	// compatibility for `--no-locks`
	extraArgs = `--no-locks`
	err = parseExtraArgs(&logger, exportCfg, strings.Fields(extraArgs))
	c.Assert(err, IsNil)
	c.Assert(exportCfg.Consistency, Equals, "none")

	// compatibility for `--no-locks`
	extraArgs = `--no-locks --consistency none`
	err = parseExtraArgs(&logger, exportCfg, strings.Fields(extraArgs))
	c.Assert(err, IsNil)
	c.Assert(exportCfg.Consistency, Equals, "none")

	extraArgs = `--consistency lock`
	err = parseExtraArgs(&logger, exportCfg, strings.Fields(extraArgs))
	c.Assert(err, IsNil)
	c.Assert(exportCfg.Consistency, Equals, "lock")

	// compatibility for `--no-locks`
	extraArgs = `--no-locks --consistency lock`
	err = parseExtraArgs(&logger, exportCfg, strings.Fields(extraArgs))
	c.Assert(err.Error(), Equals, "cannot both specify `--no-locks` and `--consistency` other than `none`")
}

func (d *testDumplingSuite) TestParseArgsWontOverwrite(c *C) {
	cfg := &config.SubTaskConfig{}
	cfg.ChunkFilesize = "1"
	rules := &filter.Rules{
		DoDBs: []string{"unit_test"},
	}
	cfg.BAList = rules
	// make sure we enter `parseExtraArgs`
	cfg.ExtraArgs = "-s=4000 --consistency lock"

	dumpling := NewDumpling(cfg)
	exportCfg, err := dumpling.constructArgs()
	c.Assert(err, IsNil)

	c.Assert(exportCfg.StatementSize, Equals, uint64(4000))
	c.Assert(exportCfg.FileSize, Equals, uint64(1*units.MiB))

	f, err2 := tfilter.ParseMySQLReplicationRules(rules)
	c.Assert(err2, IsNil)
	c.Assert(exportCfg.TableFilter, DeepEquals, tfilter.CaseInsensitive(f))

	c.Assert(exportCfg.Consistency, Equals, "lock")
}
