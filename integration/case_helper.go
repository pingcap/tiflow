package main

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/integration/framework"
)

type HelperCase struct {
	framework.AvroSingleTableTask
}

func NewHelperCase() *HelperCase {
	helperCase := new(HelperCase)
	helperCase.AvroSingleTableTask.TableName = "test"
	return helperCase
}

func (s *HelperCase) Name() string {
	return "Helper"
}

func (s *HelperCase) Run(ctx *framework.TaskContext) error {
	_, err := ctx.Upstream.ExecContext(ctx.Ctx, "create table test (id int primary key, value int)")
	if err != nil {
		return err
	}

	table := ctx.SqlHelper().GetTable("test")
	err = table.Insert(map[string]interface{}{
		"id": 0,
		"value": 0,
	}).Send().Wait().Check()
	if err != nil {
		return errors.AddStack(err)
	}

	reqs := make([]framework.Awaitable, 0)
	for i := 1; i < 1000; i++ {
		req := table.Insert(map[string]interface{}{
			"id": i,
			"value": i,
		}).Send()
		reqs = append(reqs, req)
	}

	return framework.All(ctx.SqlHelper(), reqs).Wait().Check()
}