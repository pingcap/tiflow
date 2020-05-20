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

	"github.com/pingcap/check"
	"github.com/pingcap/parser/model"
	cdcfilter "github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/tidb-tools/pkg/filter"
)

func TestSuite(t *testing.T) { check.TestingT(t) }

type decodeFileSuite struct{}

var _ = check.Suite(&decodeFileSuite{})

func (s *decodeFileSuite) TestCanDecodeTOML(c *check.C) {
	dir := c.MkDir()
	path := filepath.Join(dir, "config.toml")
	content := `
filter-case-sensitive = false
ignore-txn-commit-ts = [1, 2]
ddl-white-list = [1, 2]
mounter-worker-num = 64

[filter-rules]
ignore-dbs = ["test", "sys"]

[[filter-rules.do-tables]]
db-name = "sns"
tbl-name = "user"

[[filter-rules.do-tables]]
db-name = "sns"
tbl-name = "following"

[[sink-dispatch-rules]]
db-name = "sns"
tbl-name = "user"
rule = "ts"

[[sink-dispatch-rules]]
db-name = "sns"
tbl-name = "following"
rule = "rowid"
`
	err := ioutil.WriteFile(path, []byte(content), 0644)
	c.Assert(err, check.IsNil)

	cfg := new(cdcfilter.ReplicaConfig)
	err = strictDecodeFile(path, "cdc", &cfg)
	c.Assert(err, check.IsNil)

	c.Assert(cfg.FilterCaseSensitive, check.IsFalse)
	c.Assert(cfg.MounterWorkerNum, check.Equals, 64)
	c.Assert(cfg.IgnoreTxnCommitTs, check.DeepEquals, []uint64{1, 2})
	c.Assert(cfg.DDLWhitelist, check.DeepEquals, []model.ActionType{1, 2})
	c.Assert(cfg.FilterRules.IgnoreDBs, check.DeepEquals, []string{"test", "sys"})
	c.Assert(cfg.FilterRules.DoTables, check.DeepEquals, []*filter.Table{
		{Schema: "sns", Name: "user"},
		{Schema: "sns", Name: "following"},
	})
	c.Assert(cfg.SinkDispatchRules, check.DeepEquals, []*cdcfilter.DispatchRule{
		{Table: filter.Table{Schema: "sns", Name: "user"}, Rule: "ts"},
		{Table: filter.Table{Schema: "sns", Name: "following"}, Rule: "rowid"},
	})
}

func (s *decodeFileSuite) TestAndWriteExampleTOML(c *check.C) {
	// TODO add comment to config file
	content := `
filter-case-sensitive = false
ignore-txn-commit-ts = []

[filter-rules]
ignore-dbs = ["test", "sys"]

[[filter-rules.do-tables]]
db-name = "sns"
tbl-name = "user"

[[filter-rules.do-tables]]
db-name = "sns"
tbl-name = "following"

[[sink-dispatch-rules]]
db-name = "sns"
tbl-name = "user"
rule = "ts"

[[sink-dispatch-rules]]
db-name = "sns"
tbl-name = "following"
rule = "rowid"
`
	err := ioutil.WriteFile("changefeed.toml", []byte(content), 0644)
	c.Assert(err, check.IsNil)

	cfg := new(cdcfilter.ReplicaConfig)
	err = strictDecodeFile("changefeed.toml", "cdc", &cfg)
	c.Assert(err, check.IsNil)

	c.Assert(cfg.FilterCaseSensitive, check.IsFalse)
	c.Assert(cfg.IgnoreTxnCommitTs, check.DeepEquals, []uint64{})
	c.Assert(cfg.DDLWhitelist, check.IsNil)
	c.Assert(cfg.FilterRules.IgnoreDBs, check.DeepEquals, []string{"test", "sys"})
	c.Assert(cfg.FilterRules.DoTables, check.DeepEquals, []*filter.Table{
		{Schema: "sns", Name: "user"},
		{Schema: "sns", Name: "following"},
	})
	c.Assert(cfg.SinkDispatchRules, check.DeepEquals, []*cdcfilter.DispatchRule{
		{Table: filter.Table{Schema: "sns", Name: "user"}, Rule: "ts"},
		{Table: filter.Table{Schema: "sns", Name: "following"}, Rule: "rowid"},
	})
}

func (s *decodeFileSuite) TestShouldReturnErrForUnknownCfgs(c *check.C) {
	dir := c.MkDir()
	path := filepath.Join(dir, "config.toml")
	content := `filter-case-insensitive = true`
	err := ioutil.WriteFile(path, []byte(content), 0644)
	c.Assert(err, check.IsNil)

	cfg := new(cdcfilter.ReplicaConfig)
	err = strictDecodeFile(path, "cdc", &cfg)
	c.Assert(err, check.NotNil)
	c.Assert(err, check.ErrorMatches, ".*unknown config.*")
}
