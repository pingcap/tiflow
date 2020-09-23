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

package avro

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/integration/framework"
	"github.com/pingcap/ticdc/integration/framework/avro"
)

//nolint:unused
type simpleCase struct {
	avro.SingleTableTask
}

// NewSimpleCase create a test case which has some simple dmls, ddls
func NewSimpleCase() *simpleCase {
	simpleCase := new(simpleCase)
	simpleCase.SingleTableTask.TableName = "test"
	return simpleCase
}

func (s *simpleCase) Name() string {
	return "Simple"
}

func (s *simpleCase) Run(ctx *framework.TaskContext) error {
	_, err := ctx.Upstream.ExecContext(ctx.Ctx, "create table test (id int primary key, value int)")
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

	// To wait on a batch of SQL requests, create a slice of Awaitables
	reqs := make([]framework.Awaitable, 0)
	for i := 1; i < 1000; i++ {
		// Only send, do not wait
		req := table.Insert(map[string]interface{}{
			"id":    i,
			"value": i,
		}).Send()
		reqs = append(reqs, req)
	}

	// Wait on SQL requests in batch and check the correctness
	err = framework.All(ctx.SQLHelper(), reqs).Wait().Check()
	if err != nil {
		return err
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
