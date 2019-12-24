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

package cdc

import (
	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/cdc/model"
)

type FilterSuite struct{}

var _ = check.Suite(&FilterSuite{})

func (s *FilterSuite) TestFilterDMLs(c *check.C) {
	t := model.Txn{
		DMLs: []*model.DML{
			{Database: "INFORMATIOn_SCHEmA"},
			{Database: "test"},
			{Database: "test_mysql"},
			{Database: "mysql"},
		},
		Ts: 213,
	}
	filterBySchemaAndTable(&t)
	c.Assert(t.Ts, check.Equals, uint64(213))
	c.Assert(t.DDL, check.IsNil)
	c.Assert(t.DMLs, check.HasLen, 2)
	c.Assert(t.DMLs[0].Database, check.Equals, "test")
	c.Assert(t.DMLs[1].Database, check.Equals, "test_mysql")
}

func (s *FilterSuite) TestFilterDDL(c *check.C) {
	t := model.Txn{
		DDL: &model.DDL{Database: "performance_schema"},
		Ts:  10234,
	}
	filterBySchemaAndTable(&t)
	c.Assert(t.Ts, check.Equals, uint64((10234)))
	c.Assert(t.DMLs, check.HasLen, 0)
	c.Assert(t.DDL, check.IsNil)
}
