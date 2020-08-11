package main

import (
	"fmt"
	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/integration/framework"
	"math/rand"
)

type AlterCase struct {
	framework.AvroSingleTableTask
}

func NewAlterCase() *AlterCase {
	alterCase := new(AlterCase)
	alterCase.AvroSingleTableTask.TableName = "test"
	return alterCase
}

func (c *AlterCase) Name() string {
	return "Alter"
}

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

		table := ctx.SqlHelper().GetTable("test")
		reqs := make([]framework.Awaitable, 0)
		for j := 0; j < 1000; j++ {
			rowData := make(map[string]interface{}, i + 1)
			rowData["id"] = i * 1000 + j
			for k := 0; k <= i; k++ {
				rowData[fmt.Sprintf("value%d", k)] = rand.Int31()
			}
			awaitable := table.Insert(rowData).Send()
			reqs = append(reqs, awaitable)
		}

		err = framework.All(ctx.SqlHelper(), reqs).Wait().Check()
		if err != nil {
			return errors.AddStack(err)
		}
	}

	return nil
}