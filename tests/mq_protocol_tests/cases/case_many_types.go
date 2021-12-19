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

package cases

import (
	"errors"
	"math"
	"time"

	"github.com/pingcap/tiflow/tests/mq_protocol_tests/framework"
	"github.com/pingcap/tiflow/tests/mq_protocol_tests/framework/avro"
	"github.com/pingcap/tiflow/tests/mq_protocol_tests/framework/canal"
	"github.com/pingcap/tiflow/tests/mq_protocol_tests/framework/mysql"
)

// ManyTypesCase is base impl of test case for different types data
type ManyTypesCase struct {
	framework.Task
}

// NewManyTypesCase create a test case which has many types
func NewManyTypesCase(task framework.Task) *ManyTypesCase {
	return &ManyTypesCase{
		Task: task,
	}
}

// Name impl framework.Task interface
func (s *ManyTypesCase) Name() string {
	return "Many Types"
}

// Run impl framework.Task interface
func (s *ManyTypesCase) Run(ctx *framework.TaskContext) error {
	var createDBQuery string
	switch s.Task.(type) {
	case *avro.SingleTableTask:
		createDBQuery = `create table test (
						id          INT,
						t_boolean   BOOLEAN,
						t_bigint    BIGINT,
						t_double    DOUBLE,
						t_float     FLOAT,
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
					)`
	case *canal.SingleTableTask:
		// t_year       YEAR,
		createDBQuery = `create table test (
						id           INT,
						t_boolean    BOOLEAN,
						t_tinyint    TINYINT,
						t_smallint   SMALLINT,
						t_mediumint  MEDIUMINT,
						t_int        INT,
						t_bigint     BIGINT,
						u_tinyint    TINYINT UNSIGNED,
						u_smallint   SMALLINT UNSIGNED,
						u_mediumint  MEDIUMINT UNSIGNED,
						u_int        INT UNSIGNED,
						u_bigint     BIGINT UNSIGNED,
						t_double     DOUBLE,
						t_float      FLOAT,
						t_decimal    DECIMAL(38, 19),
						t_date       DATE,
						t_datetime   DATETIME,
						t_timestamp  TIMESTAMP NULL,
						t_time       TIME,
						t_tinytext   TINYTEXT,
						t_mediumtext MEDIUMTEXT,
						t_text       TEXT,
						t_longtext   LONGTEXT,
						t_tinyblob   TINYBLOB,
						t_mediumblob MEDIUMBLOB,
						t_blob       BLOB,
						t_longblob   LONGBLOB,
						t_char       CHAR,
						t_varchar    VARCHAR(10),
						t_binary     BINARY(16),
						t_varbinary  VARBINARY(16),
						t_enum       ENUM ('enum1', 'enum2', 'enum3'),
						t_set        SET ('a', 'b', 'c'),
						t_json       JSON,
						PRIMARY KEY  (id)
					)`
	case *mysql.SingleTableTask:
		createDBQuery = `create table test (
						id          INT,
						t_boolean   BOOLEAN,
						t_bigint    BIGINT,
						t_double    DOUBLE,
						t_float     FLOAT,
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
					)`
	default:
		return errors.New("unknown test case type")
	}

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
	data := map[string]interface{}{
		"id":          0,
		"t_boolean":   true,
		"t_bigint":    math.MaxInt64,
		"t_double":    1.01234,
		"t_float":     2.45678,
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
	}
	_, ok := s.Task.(*avro.SingleTableTask)
	if ok {
		data["t_year"] = 2019
		data["t_bit"] = 0b1001001
	}

	_, ok = s.Task.(*canal.SingleTableTask)
	if ok {
		data["t_tinyint"] = 127
		data["t_smallint"] = 32767
		data["t_mediumint"] = 8388607
		data["t_int"] = 2147483647

		data["u_tinyint"] = 255
		data["u_smallint"] = 65535
		data["u_mediumint"] = 16777215
		data["u_int"] = 4294967295
		data["u_bigint"] = uint64(18446744073709551615)

		// todo(3AceShowHand): `year` cannot be synced by canal-adapter, investigate more later, comment it out now.
		// data["t_year"] = 2021
		data["t_binary"] = []byte{0x1, 0x2, 0x0, 0x3}
		data["t_varbinary"] = []byte{0x1, 0x2, 0x0, 0x3}

		data["t_tinytext"] = "测试tinytext"
		data["t_mediumtext"] = "测试mediumtext"
		data["t_longtext"] = "测试longtext"

		data["t_tinyblob"] = []byte{0x1, 0x2}
		data["t_mediumblob"] = []byte{0x1, 0x2}
		data["t_longblob"] = []byte{0x1, 0x2}
	}

	return table.Insert(data).Send().Wait().Check()
}
