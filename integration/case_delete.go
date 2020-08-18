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

package main

import "github.com/pingcap/ticdc/integration/framework"

//nolint:unused
type deleteCase struct {
	framework.AvroSingleTableTask
}

func newDeleteCase() *deleteCase {
	deleteCase := new(deleteCase)
	deleteCase.AvroSingleTableTask.TableName = "test"
	return deleteCase
}

func (c *deleteCase) Name() string {
	return "Delete"
}

func (c *deleteCase) Run(ctx *framework.TaskContext) error {
	_, err := ctx.Upstream.ExecContext(ctx.Ctx, "create table test (id int primary key)")
	if err != nil {
		return err
	}

	return nil
}
