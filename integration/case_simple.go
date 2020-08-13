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
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/integration/framework"
	"go.uber.org/zap"
)

//nolint:unused
type simple struct {
	framework.AvroSingleTableTask
}

/*
func newSimple() *simple {
	simple := new(simple)
	simple.AvroSingleTableTask.TableName = "test"
	return simple
}
*/

func (s *simple) Name() string {
	return "simple"
}

func (s *simple) Run(ctx *framework.TaskContext) error {
	_, err := ctx.Upstream.ExecContext(ctx.Ctx, "create table test (id int primary key, value int)")
	if err != nil {
		return err
	}

	for i := 0; i < 100; i++ {
		_, err := ctx.Upstream.ExecContext(ctx.Ctx, "insert into test (id, value) values (?, ?)", i, i*10)
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
			id    int
			value int
		)

		err := rows.Scan(&id, &value)
		if err != nil {
			return err
		}

		if value != id*10 {
			log.Fatal("Check failed", zap.Int("id", id), zap.Int("value", value))
		}
		counter++
	}

	if counter != 100 {
		log.Fatal("Check failed", zap.Int("counter", counter))
	}

	return nil
}
