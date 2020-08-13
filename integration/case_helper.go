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

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/integration/framework"
)

//nolint:unused
type helperCase struct {
	framework.AvroSingleTableTask
}

/*
func newHelperCase() *helperCase {
	helperCase := new(helperCase)
	helperCase.AvroSingleTableTask.TableName = "test"
	return helperCase
}
*/

func (s *helperCase) Name() string {
	return "Helper"
}

func (s *helperCase) Run(ctx *framework.TaskContext) error {
	_, err := ctx.Upstream.ExecContext(ctx.Ctx, "create table test (id int primary key, value int)")
	if err != nil {
		return err
	}

	table := ctx.SQLHelper().GetTable("test")
	err = table.Insert(map[string]interface{}{
		"id":    0,
		"value": 0,
	}).Send().Wait().Check()
	if err != nil {
		return errors.AddStack(err)
	}

	reqs := make([]framework.Awaitable, 0)
	for i := 1; i < 1000; i++ {
		req := table.Insert(map[string]interface{}{
			"id":    i,
			"value": i,
		}).Send()
		reqs = append(reqs, req)
	}

	return framework.All(ctx.SQLHelper(), reqs).Wait().Check()
}
