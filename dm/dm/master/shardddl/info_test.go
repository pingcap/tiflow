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

package shardddl

import (
	. "github.com/pingcap/check"

	"github.com/pingcap/dm/pkg/shardddl/pessimism"
)

func (t *testPessimist) TestInfoSlice(c *C) {
	var (
		task          = "task"
		source1       = "mysql-replica-1"
		source2       = "mysql-replica-2"
		source3       = "mysql-replica-3"
		schema, table = "foo", "bar"
		DDLs          = []string{"ALTER TABLE bar ADD COLUMN c1 INT"}
		ifm           = map[string]pessimism.Info{
			source3: pessimism.NewInfo(task, source3, schema, table, DDLs),
			source2: pessimism.NewInfo(task, source2, schema, table, DDLs),
			source1: pessimism.NewInfo(task, source1, schema, table, DDLs),
		}
	)

	ifs := pessimismInfoMapToSlice(ifm)
	c.Assert(ifs, HasLen, 3)
	c.Assert(ifs[0], DeepEquals, ifm[source1])
	c.Assert(ifs[1], DeepEquals, ifm[source2])
	c.Assert(ifs[2], DeepEquals, ifm[source3])
}
