// Copyright 2022 PingCAP, Inc.
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

package utils

import (
	"testing"

	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
)

// NewLargeEvent4Test creates large events for test
func NewLargeEvent4Test(t *testing.T) (*model.DDLEvent, *model.RowChangedEvent, *model.RowChangedEvent, *model.RowChangedEvent) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	sql := `create table test.t(
    	t tinyint primary key,
		tu1 tinyint unsigned,
		tu2 tinyint unsigned,
		tu3 tinyint unsigned,
		tu4 tinyint unsigned,
		s smallint,
		su1 smallint unsigned,
		su2 smallint unsigned,
		su3 smallint unsigned,
		su4 smallint unsigned,
		m mediumint,
		mu1 mediumint unsigned,
		mu2 mediumint unsigned,
		mu3 mediumint unsigned,
		mu4 mediumint unsigned,
		i int,
		iu1 int unsigned,
		iu2 int unsigned,
		iu3 int unsigned,
		iu4 int unsigned,
		bi bigint,
		biu1 bigint unsigned,
		biu2 bigint unsigned,
		biu3 bigint unsigned,
		biu4 bigint unsigned,
		floatT float,
		doubleT double,
	 	decimalT decimal,
	 	floatTu float unsigned,
		doubleTu double unsigned,
	 	decimalTu decimal unsigned,
	 	varcharT varchar(255),
	 	charT char(255),
	 	binaryT binary(255),
	 	varbinaryT varbinary(255),
	 	tinytextT tinytext,
	 	textT text,
	 	mediumtextT mediumtext,
	 	longtextT longtext,
	 	tinyblobT tinyblob,
	 	blobT blob,
	 	mediumblobT mediumblob,
	 	longblobT longblob,
	 	dateT date,
	 	datetimeT datetime,
	 	timestampT timestamp,
	 	timeT time,
	 	yearT year,
	 	enumT enum('a', 'b', 'c'),
	 	setT set('a', 'b', 'c'),
	 	bitT bit(10),
	 	jsonT json)`
	ddlEvent := helper.DDL2Event(sql)

	sql = `insert into test.t values(
		127,
		127,
		128,
		0,
		null,
		32767,
		32767,
		32768,
		0,
		null,
		8388607,
		8388607,
		8388608,
		0,
		null,
		2147483647,
		2147483647,
		2147483648,
		0,
		null,
		9223372036854775807,
		9223372036854775807,
		9223372036854775808,
		0,
		null,
		3.14,
		2.71,
		2333,
		3.14,
		2.71,
		2333,
		'测试Varchar',
		'测试String',
		'测试Binary',
		'测试varbinary',
		'测试Tinytext',
		'测试text',
		'测试mediumtext',
		'测试longtext',
		'测试tinyblob',
		'测试blob',
		'测试mediumblob',
		'测试longblob',
		'2020-02-20',
		'2020-02-20 02:20:20',
		'2020-02-20 10:20:20',
		'02:20:20',
		2020,
		'a',
		'b',
		65,
		'{"key1": "value1"}')`
	insert := helper.DML2Event(sql, "test", "t")

	update := *insert
	update.PreColumns = update.Columns

	deleteE := *insert
	deleteE.PreColumns = deleteE.Columns
	deleteE.Columns = nil

	return ddlEvent, insert, &update, &deleteE
}
