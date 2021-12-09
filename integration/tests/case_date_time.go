// Copyright 2021 PingCAP, Inc.
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
	"time"

	"github.com/pingcap/ticdc/integration/framework"
	"github.com/pingcap/ticdc/integration/framework/avro"
)

// DateTimeCase is base impl of test case for different types data
type DateTimeCase struct {
	framework.Task
}

// NewDateTimeCase create a test case which has many types
func NewDateTimeCase(task framework.Task) *DateTimeCase {
	return &DateTimeCase{
		Task: task,
	}
}

// Name impl framework.Task interface
func (s *DateTimeCase) Name() string {
	return "Date Time"
}

// Run impl framework.Task interface
func (s *DateTimeCase) Run(ctx *framework.TaskContext) error {
	createDBQuery := `create table test (
						id          INT,
						t_date      DATE,
						t_datetime  DATETIME,
						t_timestamp TIMESTAMP NULL,
						PRIMARY KEY (id)
					)`

	_, err := ctx.Upstream.ExecContext(ctx.Ctx, createDBQuery)
	if err != nil {
		return err
	}
	if _, ok := s.Task.(*avro.SingleTableTask); ok {
		_, err = ctx.Downstream.ExecContext(ctx.Ctx, "drop table if exists test")
		if err != nil {
			return err
		}

		_, err = ctx.Downstream.ExecContext(ctx.Ctx, createDBQuery)
		if err != nil {
			return err
		}
	}

	// Get a handle of an existing table
	table := ctx.SQLHelper().GetTable("test")

	// Zero value case
	zeroValue := time.Unix(0, 0)
	data := map[string]interface{}{
		"id":          0,
		"t_date":      zeroValue,
		"t_datetime":  zeroValue,
		"t_timestamp": zeroValue.Add(time.Second),
	}
	err = table.Insert(data).Send().Wait().Check()
	if err != nil {
		return err
	}

	_, err = ctx.Upstream.ExecContext(ctx.Ctx, "alter table test add t_date_1 date not null")
	if err != nil {
		return err
	}

	// Get a handle of an existing table
	table = ctx.SQLHelper().GetTable("test")
	data = map[string]interface{}{
		"id":          1,
		"t_date":      zeroValue,
		"t_datetime":  zeroValue,
		"t_timestamp": zeroValue.Add(time.Second),
		"t_date_1":    zeroValue.AddDate(15, 0, 1),
	}

	err = table.Insert(data).Send().Wait().Check()
	if err != nil {
		return err
	}

	// Ancient date case. We DO NOT support it.
	// TODO investigate why and find out a solution
	/* ancientTime := time.Date(960, 1, 1, 15, 33, 0, 0, time.UTC)
	data = map[string]interface{}{
		"id":          1,
		"t_date":      ancientTime,
		"t_datetime":  ancientTime,
		"t_timestamp": zeroValue.Add(time.Second),  // Timestamp does not support the Zero value of `time.Time`, so we test the Unix epoch instead
	}
	err = table.Insert(data).Send().Wait().Check()
	if err != nil {
		return err
	}
	*/

	return nil
}
