package main

import (
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/integration/framework"
	"go.uber.org/zap"
)

type Simple struct {
	framework.AvroSingleTableTask
}

func NewSimple() *Simple {
	simple := new(Simple)
	simple.AvroSingleTableTask.TableName = "test"
	return simple
}

func (s *Simple) Name() string {
	return "Simple"
}

func (s *Simple) Run(ctx *framework.TaskContext) error {
	_, err := ctx.Upstream.ExecContext(ctx.Ctx, "create table test (id int primary key, value int)")
	if err != nil {
		return err
	}

	for i := 0; i < 100; i++ {
		_, err := ctx.Upstream.ExecContext(ctx.Ctx, "insert into test (id, value) values (?, ?)", i, i * 10)
		if err != nil {
			return err
		}
	}

	time.Sleep(10 * time.Second)
	rows, err := ctx.Downstream.QueryContext(ctx.Ctx, "select * from test")
	if err != nil {
		return err
	}
	defer rows.Close()

	counter := 0
	for rows.Next() {
		var (
			id int
			value int
		)

		err := rows.Scan(&id, &value)
		if err != nil {
			return err
		}

		if value != id * 10 {
			log.Fatal("Check failed", zap.Int("id", id), zap.Int("value", value))
		}
		counter ++
	}

	if counter != 100 {
		log.Fatal("Check failed", zap.Int("counter", counter))
	}

	return nil
}


