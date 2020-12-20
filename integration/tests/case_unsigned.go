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
	"github.com/pingcap/ticdc/integration/framework"
)

// UnsignedCase is base impl of test case for unsigned int type data
type UnsignedCase struct {
	framework.Task
}

// NewUnsignedCase create a test case to check the correction of unsigned integer
func NewUnsignedCase(task framework.Task) *UnsignedCase {
	return &UnsignedCase{
		Task: task,
	}
}

// Name impl framework.Task interface
func (s *UnsignedCase) Name() string {
	return "Unsigned"
}

// Run impl framework.Task interface
func (s *UnsignedCase) Run(ctx *framework.TaskContext) error {
	createDBQuery := `create table test (
		id          INT,
		t_int       INT UNSIGNED,
		t_bigint    BIGINT UNSIGNED,
		t_bit       BIT(64),
		PRIMARY KEY (id)
	)
    `
	_, err := ctx.Upstream.ExecContext(ctx.Ctx, createDBQuery)
	if err != nil {
		return err
	}

	_, err = ctx.Downstream.ExecContext(ctx.Ctx, "drop table if exists test")
	if err != nil {
		return err
	}

	_, err = ctx.Downstream.ExecContext(ctx.Ctx, createDBQuery)
	if err != nil {
		return err
	}

	// Get a handle of an existing table
	table := ctx.SQLHelper().GetTable("test")
	return table.Insert(map[string]interface{}{
		"id":       0,
		"t_int":    0xFEEDBEEF,
		"t_bigint": uint64(0xFEEDBEEFFEEDBEEF),
		"t_bit":    uint64(0xFFFFFFFFFFFFFFFA),
	}).Send().Wait().Check()
}
