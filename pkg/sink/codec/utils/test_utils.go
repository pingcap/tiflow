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
	"database/sql/driver"
	"testing"

	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
)

// NewLargeEvent4Test creates large events for test
func NewLargeEvent4Test(t *testing.T, replicaConfig *config.ReplicaConfig) (*model.DDLEvent, *model.RowChangedEvent, *model.RowChangedEvent, *model.RowChangedEvent) {
	helper := entry.NewSchemaTestHelperWithReplicaConfig(t, replicaConfig)
	defer helper.Close()

	sql := `create table test.t(
    	t tinyint primary key,
		tu1 tinyint unsigned default 1,
		tu2 tinyint unsigned default 2,
		tu3 tinyint unsigned default 3,
		tu4 tinyint unsigned default 4,
		s smallint default 5,
		su1 smallint unsigned default 6,
		su2 smallint unsigned default 7,
		su3 smallint unsigned default 8,
		su4 smallint unsigned default 9,
		m mediumint default 10,
		mu1 mediumint unsigned default 11,
		mu2 mediumint unsigned default 12,
		mu3 mediumint unsigned default 13,
		mu4 mediumint unsigned default 14,
		i int default 15,
		iu1 int unsigned default 16,
		iu2 int unsigned default 17,
		iu3 int unsigned default 18,
		iu4 int unsigned default 19,
		bi bigint default 20,
		biu1 bigint unsigned default 21,
		biu2 bigint unsigned default 22,
		biu3 bigint unsigned default 23,
		biu4 bigint unsigned default 24,
		floatT float default 3.14,
		doubleT double default 2.7182818284,
	 	decimalT decimal(12, 6) default 179394.2333,
	 	floatTu float unsigned default 3.14,
		doubleTu double unsigned default 2.7182818284,
	 	decimalTu decimal(12, 6) unsigned default 179394.2333,
	 	decimalTu2 decimal(5, 4) unsigned default 3.1415,
	 	varcharT varchar(255) default '测试Varchar default',
	 	charT char(255) default '测试Char default',
	 	binaryT binary(255) default '测试Binary default',
	 	varbinaryT varbinary(255) default '测试varbinary default',
	 	tinytextT tinytext,
	 	textT text,
	 	mediumtextT mediumtext,
	 	longtextT longtext,
	 	tinyblobT tinyblob,
	 	blobT blob,
	 	mediumblobT mediumblob,
	 	longblobT longblob,
	 	dateT date default '2023-12-27',
	 	datetimeT datetime default '2023-12-27 12:27:23',
	 	timestampT timestamp default now(),
	 	timestampT2 timestamp(6) default '2024-03-11 08:51:01.461270',
	 	timeT time default '12:27:23',
	 	yearT year default 2023,
	 	enumT enum('a', 'b', 'c') default 'b',
	 	setT set('a', 'b', 'c') default 'c',
	 	bitT bit(10) default b'1010101010',
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
		2333.654321,
		3.14,
		2.71,
		2333.123456,
        1.7371,
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
	    '2024-03-11 08:51:01.461270',
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

// LargeColumnKeyValues returns the key values of large columns
func LargeColumnKeyValues() ([]string, []driver.Value) {
	names := make([]string, 0, len(LargeTableColumns))
	values := make([]driver.Value, 0, len(LargeTableColumns))
	for key, value := range LargeTableColumns {
		names = append(names, key)
		values = append(values, driver.Value(value))
	}
	return names, values
}

// LargeTableColumns is the columns of large table
var LargeTableColumns = map[string]interface{}{
	"t":           []uint8("127"),
	"tu1":         []uint8("127"),
	"tu2":         []uint8("128"),
	"tu3":         []uint8("0"),
	"tu4":         nil,
	"s":           []uint8("32767"),
	"su1":         []uint8("32767"),
	"su2":         []uint8("32768"),
	"su3":         []uint8("0"),
	"su4":         nil,
	"m":           []uint8("8388607"),
	"mu1":         []uint8("8388607"),
	"mu2":         []uint8("8388608"),
	"mu3":         []uint8("0"),
	"mu4":         nil,
	"i":           []uint8("2147483647"),
	"iu1":         []uint8("2147483647"),
	"iu2":         []uint8("2147483648"),
	"iu3":         []uint8("0"),
	"iu4":         nil,
	"bi":          []uint8("9223372036854775807"),
	"biu1":        []uint8("9223372036854775807"),
	"biu2":        []uint8("9223372036854775808"),
	"biu3":        []uint8("0"),
	"biu4":        nil,
	"floatT":      []uint8("3.14"),
	"doubleT":     []uint8("2.71"),
	"decimalT":    []uint8("2333.654321"),
	"floatTu":     []uint8("3.14"),
	"doubleTu":    []uint8("2.71"),
	"decimalTu":   []uint8("2333.123456"),
	"decimalTu2":  []uint8("1.7371"),
	"varcharT":    []uint8("测试Varchar"),
	"charT":       []uint8("测试String"),
	"binaryT":     []uint8("测试Binary"),
	"varbinaryT":  []uint8("测试varbinary"),
	"tinytextT":   []uint8("测试Tinytext"),
	"textT":       []uint8("测试text"),
	"mediumtextT": []uint8("测试mediumtext"),
	"longtextT":   []uint8("测试longtext"),
	"tinyblobT":   []uint8("测试tinyblob"),
	"blobT":       []uint8("测试blob"),
	"mediumblobT": []uint8("测试mediumblob"),
	"longblobT":   []uint8("测试longblob"),
	"dateT":       []uint8("2020-02-20"),
	"datetimeT":   []uint8("2020-02-20 02:20:20"),
	"timestampT":  []uint8("2020-02-20 10:20:20"),
	"timestampT2": []uint8("2024-03-11 08:51:01.461270"),
	"timeT":       []uint8("02:20:20"),
	"yearT":       []uint8("2020"),
	"enumT":       []uint8("a"),
	"setT":        []uint8("b"),
	"bitT":        []uint8{65},
	"jsonT":       []uint8("{\"key1\": \"value1\"}"),
}
