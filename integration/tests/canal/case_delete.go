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

package canal

import (
	"errors"
	"github.com/pingcap/ticdc/integration/framework/canal"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/integration/framework"
	"go.uber.org/zap"
)

//nolint:unused
type deleteCase struct {
	canal.SingleTableTask
}

// NewDeleteCase create a test case which contains delete ddls
func NewDeleteCase() *deleteCase {
	deleteCase := new(deleteCase)
	deleteCase.SingleTableTask.TableName = "test"
	return deleteCase
}

func (c *deleteCase) Name() string {
	return "Delete"
}

func (c *deleteCase) Run(ctx *framework.TaskContext) error {
	_, err := ctx.Upstream.ExecContext(ctx.Ctx, "create table test (id int primary key, value int)")
	if err != nil {
		return err
	}

	table := ctx.SQLHelper().GetTable("test")

	// To wait on a batch of SQL requests, create a slice of Awaitables
	reqs := make([]framework.Awaitable, 0)
	for i := 0; i < 1000; i++ {
		// Only send, do not wait
		req := table.Insert(map[string]interface{}{
			"id":    i,
			"value": i,
		}).Send()
		reqs = append(reqs, req)
	}

	err = framework.All(ctx.SQLHelper(), reqs).Wait().Check()
	if err != nil {
		return err
	}

	deletes := make([]framework.Awaitable, 0, 1000)
	for i := 0; i < 1000; i++ {
		req := table.Delete(map[string]interface{}{
			"id": i,
		}).Send()
		deletes = append(deletes, req)
	}

	for _, req := range deletes {
		err := req.Wait().Check()
		if err != nil {
			return err
		}
	}

	count := 0
	err = ctx.Downstream.QueryRowContext(ctx.Ctx, "select count(*) from test").Scan(&count)
	if err != nil {
		return err
	}

	if count != 0 {
		log.Warn("table is not empty", zap.Int("count", count))
		return errors.New("table is not empty")
	}

	return nil
}
