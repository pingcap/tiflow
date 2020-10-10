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
	"math"
	"time"

	"github.com/pingcap/ticdc/integration/framework"
	"github.com/pingcap/ticdc/integration/framework/canal"
)

//nolint:unused
type canalAlterCase struct {
	AlterCase
}

// NewCanalAlterCase construct alter case for canal
func NewCanalAlterCase() *canalAlterCase {
	return &canalAlterCase{
		AlterCase: NewAlterCase(&canal.SingleTableTask{TableName: "test"}),
	}
}

//nolint:unused
type canalCompositePKeyCase struct {
	CompositePKeyCase
}

// NewCanalCompositePKeyCase construct composite primary key case for canal
func NewCanalCompositePKeyCase() *canalCompositePKeyCase {
	return &canalCompositePKeyCase{
		CompositePKeyCase: NewCompositePKeyCase(&canal.SingleTableTask{TableName: "test"}),
	}
}

//nolint:unused
type canalDeleteCase struct {
	DeleteCase
}

// NewCanalDeleteCase construct delete case for canal
func NewCanalDeleteCase() *canalDeleteCase {
	return &canalDeleteCase{
		DeleteCase: NewDeleteCase(&canal.SingleTableTask{TableName: "test"}),
	}
}

//nolint:unused
type canalManyTypesCase struct {
	ManyTypesCase
}

// NewCanalManyTypesCase construct many types case for canal
func NewCanalManyTypesCase() *canalManyTypesCase {
	return &canalManyTypesCase{
		ManyTypesCase: NewManyTypesCase(&canal.SingleTableTask{TableName: "test"}),
	}
}

func (s *canalManyTypesCase) Run(ctx *framework.TaskContext) error {
	createDBQuery := `create table test (
		id          INT,
		t_boolean   BOOLEAN,
		t_bigint    BIGINT,
		t_double    DOUBLE,
		t_decimal   DECIMAL(38, 19),
		t_date      DATE,
		t_datetime  DATETIME,
		t_timestamp TIMESTAMP NULL,
		t_time      TIME,
		t_char      CHAR,
		t_varchar   VARCHAR(10),
		t_blob      BLOB,
		t_text      TEXT,
		t_enum      ENUM ('enum1', 'enum2', 'enum3'),
		t_set       SET ('a', 'b', 'c'),
		t_json      JSON,
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
		"id":          0,
		"t_boolean":   true,
		"t_bigint":    math.MaxInt64,
		"t_double":    1.01234,
		"t_decimal":   "12345.6789",
		"t_date":      time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
		"t_datetime":  time.Now(),
		"t_timestamp": time.Now(),
		"t_time":      "23:59:59",
		"t_char":      "a",
		"t_varchar":   "测试varchar",
		"t_blob":      []byte{0x1, 0x2, 0x0, 0x3, 0x4},
		"t_text":      "测试text",
		"t_enum":      "enum2",
		"t_set":       "a,b",
		"t_json":      nil,
	}).Send().Wait().Check()

}

//nolint:unused
type canalSimpleCase struct {
	SimpleCase
}

// NewCanalSimpleCase construct simple case for canal
func NewCanalSimpleCase() *canalSimpleCase {
	return &canalSimpleCase{
		SimpleCase: NewSimpleCase(&canal.SingleTableTask{TableName: "test"}),
	}
}

//nolint:unused
type canalUnsignedCase struct {
	UnsignedCase
}

// NewCanalUnsignedCase construct unsigned case for canal
func NewCanalUnsignedCase() *canalUnsignedCase {
	return &canalUnsignedCase{
		UnsignedCase: NewUnsignedCase(&canal.SingleTableTask{TableName: "test"}),
	}
}
