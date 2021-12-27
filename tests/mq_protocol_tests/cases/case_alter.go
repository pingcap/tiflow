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

package cases

import (
	"fmt"
	"math/rand"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/tests/mq_protocol_tests/framework"
)

// AlterCase is base impl of test case for alter operation
type AlterCase struct {
	framework.Task
}

// NewAlterCase create a test case which contains alter ddls
func NewAlterCase(task framework.Task) *AlterCase {
	return &AlterCase{
		Task: task,
	}
}

// Name impl framework.Task interface
func (c *AlterCase) Name() string {
	return "Alter"
}

// Run impl framework.Task interface
func (c *AlterCase) Run(ctx *framework.TaskContext) error {
	_, err := ctx.Upstream.ExecContext(ctx.Ctx, "create table test (id int primary key)")
	if err != nil {
		return err
	}

	for i := 0; i < 20; i++ {
		_, err := ctx.Upstream.ExecContext(ctx.Ctx, fmt.Sprintf("alter table test add column (value%d int)", i))
		if err != nil {
			return err
		}

		table := ctx.SQLHelper().GetTable("test")
		reqs := make([]framework.Awaitable, 0)
		for j := 0; j < 1000; j++ {
			rowData := make(map[string]interface{}, i+1)
			rowData["id"] = i*1000 + j
			for k := 0; k <= i; k++ {
				rowData[fmt.Sprintf("value%d", k)] = rand.Int31()
			}
			awaitable := table.Insert(rowData).Send()
			reqs = append(reqs, awaitable)
		}

		err = framework.All(ctx.SQLHelper(), reqs).Wait().Check()
		if err != nil {
			return errors.AddStack(err)
		}
	}

	return nil
}
