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
	"github.com/pingcap/ticdc/integration/framework/avro"
	"math"
	"time"

	"github.com/pingcap/ticdc/integration/framework"
)

//nolint:unused
type manyTypesCase struct {
	avro.SingleTableTask
}

// NewManyTypesCase create a test case which has many types
func NewManyTypesCase() *manyTypesCase {
	manyTypesCase := new(manyTypesCase)
	manyTypesCase.SingleTableTask.TableName = "test"
	return manyTypesCase
}

func (s *manyTypesCase) Name() string {
	return "Many Types"
}

func (s *manyTypesCase) Run(ctx *framework.TaskContext) error {
	createDBQuery := `create table test (
		id          INT,
		t_boolean   BOOLEAN,
		t_bigint    BIGINT,
		t_double    DOUBLE,
		t_decimal   DECIMAL(38, 19),
		t_bit       BIT(64),
		t_date      DATE,
		t_datetime  DATETIME,
		t_timestamp TIMESTAMP NULL,
		t_time      TIME,
		t_year      YEAR,
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
		"t_bit":       0b1001001,
		"t_date":      time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
		"t_datetime":  time.Now(),
		"t_timestamp": time.Now(),
		"t_time":      "23:59:59",
		"t_year":      2019,
		"t_char":      "a",
		"t_varchar":   "测试varchar",
		"t_blob":      []byte{0x1, 0x2, 0x0, 0x3, 0x4},
		"t_text":      "测试text",
		"t_enum":      "enum2",
		"t_set":       "a,b",
		"t_json":      nil,
	}).Send().Wait().Check()

}
