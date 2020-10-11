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

package tests

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/integration/framework"
)

// CompositePKeyCase is base impl of test case for composite primary keys
type CompositePKeyCase struct {
	framework.Task
}

// NewCompositePKeyCase create a test case which have composite primary key
func NewCompositePKeyCase(task framework.Task) *CompositePKeyCase {
	return &CompositePKeyCase{
		Task: task,
	}
}

// Name impl framework.Task interface
func (s *CompositePKeyCase) Name() string {
	return "Composite Primary Key"
}

// Run impl framework.Task interface
func (s *CompositePKeyCase) Run(ctx *framework.TaskContext) error {
	_, err := ctx.Upstream.ExecContext(ctx.Ctx, "create table test (id1 int, id2 int, value int, primary key (id1, id2))")
	if err != nil {
		return err
	}

	// Get a handle of an existing table
	table := ctx.SQLHelper().GetTable("test")
	// Create an SQL request, send it to the upstream, wait for completion and check the correctness of replication
	err = table.Insert(map[string]interface{}{
		"id1":   0,
		"id2":   1,
		"value": 0,
	}).Send().Wait().Check()
	if err != nil {
		return errors.AddStack(err)
	}

	err = table.Upsert(map[string]interface{}{
		"id1":   0,
		"id2":   1,
		"value": 1,
	}).Send().Wait().Check()
	if err != nil {
		return err
	}

	err = table.Delete(map[string]interface{}{
		"id1": 0,
		"id2": 1,
	}).Send().Wait().Check()
	return err
}
