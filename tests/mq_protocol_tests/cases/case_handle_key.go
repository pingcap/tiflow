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
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/tests/mq_protocol_tests/framework"
)

// HandleKeyCase is base impl of test case for non primary handle keys
type HandleKeyCase struct {
	framework.Task
}

// NewHandleKeyCase create a test case which have non primary handle keys
func NewHandleKeyCase(task framework.Task) *HandleKeyCase {
	return &HandleKeyCase{
		Task: task,
	}
}

// Name impl framework.Task interface
func (s *HandleKeyCase) Name() string {
	return "Handle Key"
}

// Run impl framework.Task interface
func (s *HandleKeyCase) Run(ctx *framework.TaskContext) error {
	_, err := ctx.Upstream.ExecContext(ctx.Ctx, "create table test (id int not null unique, value int)")
	if err != nil {
		return err
	}

	// Get a handle of an existing table
	table := ctx.SQLHelper().GetTable("test")
	// Create an SQL request, send it to the upstream, wait for completion and check the correctness of replication
	err = table.Insert(map[string]interface{}{
		"id":    0,
		"value": 0,
	}).Send().Wait().Check()
	if err != nil {
		return errors.AddStack(err)
	}

	err = table.Upsert(map[string]interface{}{
		"id":    0,
		"value": 1,
	}).Send().Wait().Check()
	if err != nil {
		return err
	}

	err = table.Delete(map[string]interface{}{
		"id": 0,
	}).Send().Wait().Check()
	return err
}
