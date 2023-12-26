// Copyright 2023 PingCAP, Inc.
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

package simple

import (
	"testing"

	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/stretchr/testify/require"
)

func TestNewTableSchema(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	// case 1: test for primary key is not explicitly constraint
	sql := `create table test.t1(
		id int primary key,
		name varchar(64) not null,
		age int,
		email varchar(255) not null,
		unique index idx_name(name),
		index idx_age_email(age,email)
	);`
	tableInfo := helper.DDL2Event(sql).TableInfo
	want := &TableSchema{
		Database: tableInfo.TableName.Schema,
		Table:    tableInfo.TableName.Table,
		Version:  tableInfo.UpdateTS,
		Columns: []*columnSchema{
			{
				ID:   1,
				Name: "id",
				DataType: dataType{
					MySQLType: "int",
					Charset:   "binary",
					Collate:   "binary",
					Length:    11,
				},
				Nullable: false,
			},
			{
				ID:   2,
				Name: "name",
				DataType: dataType{
					MySQLType: "varchar",
					Charset:   "utf8mb4",
					Collate:   "utf8mb4_bin",
					Length:    64,
				},
				Nullable: false,
			},
			{
				ID:   3,
				Name: "age",
				DataType: dataType{
					MySQLType: "int",
					Charset:   "binary",
					Collate:   "binary",
					Length:    11,
				},
				Nullable: true,
			},
			{
				ID:   4,
				Name: "email",
				DataType: dataType{
					MySQLType: "varchar",
					Charset:   "utf8mb4",
					Collate:   "utf8mb4_bin",
					Length:    255,
				},
				Nullable: false,
			},
		},
		Indexes: []*IndexSchema{
			{
				Name:     "idx_name",
				Unique:   true,
				Primary:  false,
				Nullable: false,
				Columns:  []string{"name"},
			},
			{
				Name:     "idx_age_email",
				Unique:   false,
				Primary:  false,
				Nullable: true,
				Columns:  []string{"age", "email"},
			},
			{
				Name:     "primary",
				Unique:   true,
				Primary:  true,
				Nullable: false,
				Columns:  []string{"id"},
			},
		},
	}
	got := newTableSchema(tableInfo)
	require.Equal(t, want, got)

	// case 2: test for primary key is explicitly constraint
	sql = `create table test.t2(
		id int,
		name varchar(64) not null,
		age int,
		email varchar(255) not null,
		primary key(id),
		unique index idx_name(name),
		index idx_age_email(age,email)
	);`
	tableInfo = helper.DDL2Event(sql).TableInfo
	want = &TableSchema{
		Database: tableInfo.TableName.Schema,
		Table:    tableInfo.TableName.Table,
		Version:  tableInfo.UpdateTS,
		Columns: []*columnSchema{
			{
				ID:   1,
				Name: "id",
				DataType: dataType{
					MySQLType: "int",
					Charset:   "binary",
					Collate:   "binary",
					Length:    11,
				},
				Nullable: false,
			},
			{
				ID:   2,
				Name: "name",
				DataType: dataType{
					MySQLType: "varchar",
					Charset:   "utf8mb4",
					Collate:   "utf8mb4_bin",
					Length:    64,
				},
				Nullable: false,
			},
			{
				ID:   3,
				Name: "age",
				DataType: dataType{
					MySQLType: "int",
					Charset:   "binary",
					Collate:   "binary",
					Length:    11,
				},
				Nullable: true,
			},
			{
				ID:   4,
				Name: "email",
				DataType: dataType{
					MySQLType: "varchar",
					Charset:   "utf8mb4",
					Collate:   "utf8mb4_bin",
					Length:    255,
				},
				Nullable: false,
			},
		},
		Indexes: []*IndexSchema{
			{
				Name:     "idx_name",
				Unique:   true,
				Primary:  false,
				Nullable: false,
				Columns:  []string{"name"},
			},
			{
				Name:     "idx_age_email",
				Unique:   false,
				Primary:  false,
				Nullable: true,
				Columns:  []string{"age", "email"},
			},
			{
				Name:     "primary",
				Unique:   true,
				Primary:  true,
				Nullable: false,
				Columns:  []string{"id"},
			},
		},
	}
	got = newTableSchema(tableInfo)
	require.Equal(t, want, got)

	// case 3: test for all data types in TiDB
	sql = `create table test.t3(
		t tinyint primary key,
		tu1 tinyint unsigned,
		s smallint,
		su1 smallint unsigned,
		m mediumint,
		mu1 mediumint unsigned,
		i int default 100,
		iu1 int unsigned,
		bi bigint,
		biu1 bigint unsigned,
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
		 enumT enum('a', 'b', 'c') default 'b',
		 setT set('a', 'b', 'c'),
		 bitT bit(10),
		 jsonT json,
		 tgen tinyint AS (t+1))` // 38
	tableInfo = helper.DDL2Event(sql).TableInfo
	want = &TableSchema{
		Database: tableInfo.TableName.Schema,
		Table:    tableInfo.TableName.Table,
		Version:  tableInfo.UpdateTS,
		Columns: []*columnSchema{
			{
				ID:   1,
				Name: "t",
				DataType: dataType{
					MySQLType: "tinyint",
					Charset:   "binary",
					Collate:   "binary",
					Length:    4,
				},
				Nullable: false,
			},
			{
				ID:   2,
				Name: "tu1",
				DataType: dataType{
					MySQLType: "tinyint",
					Charset:   "binary",
					Collate:   "binary",
					Length:    3,
					Unsigned:  true,
				},
				Nullable: true,
			},
			{
				ID:   3,
				Name: "s",
				DataType: dataType{
					MySQLType: "smallint",
					Charset:   "binary",
					Collate:   "binary",
					Length:    6,
				},
				Nullable: true,
			},
			{
				ID:   4,
				Name: "su1",
				DataType: dataType{
					MySQLType: "smallint",
					Charset:   "binary",
					Collate:   "binary",
					Length:    5,
					Unsigned:  true,
				},
				Nullable: true,
			},
			{
				ID:   5,
				Name: "m",
				DataType: dataType{
					MySQLType: "mediumint",
					Charset:   "binary",
					Collate:   "binary",
					Length:    9,
				},
				Nullable: true,
			},
			{
				ID:   6,
				Name: "mu1",
				DataType: dataType{
					MySQLType: "mediumint",
					Charset:   "binary",
					Collate:   "binary",
					Length:    8,
					Unsigned:  true,
				},
				Nullable: true,
			},
			{
				ID:   7,
				Name: "i",
				DataType: dataType{
					MySQLType: "int",
					Charset:   "binary",
					Collate:   "binary",
					Length:    11,
				},
				Nullable: true,
				Default:  "100",
			},
			{
				ID:   8,
				Name: "iu1",
				DataType: dataType{
					MySQLType: "int",
					Charset:   "binary",
					Collate:   "binary",
					Length:    10,
					Unsigned:  true,
				},
				Nullable: true,
			},
			{
				ID:   9,
				Name: "bi",
				DataType: dataType{
					MySQLType: "bigint",
					Charset:   "binary",
					Collate:   "binary",
					Length:    20,
				},
				Nullable: true,
			},
			{
				ID:   10,
				Name: "biu1",
				DataType: dataType{
					MySQLType: "bigint",
					Charset:   "binary",
					Collate:   "binary",
					Length:    20,
					Unsigned:  true,
				},
				Nullable: true,
			},
			{
				ID:   11,
				Name: "floatT",
				DataType: dataType{
					MySQLType: "float",
					Charset:   "binary",
					Collate:   "binary",
					Length:    12,
					Decimal:   -1,
				},
				Nullable: true,
			},
			{
				ID:   12,
				Name: "doubleT",
				DataType: dataType{
					MySQLType: "double",
					Charset:   "binary",
					Collate:   "binary",
					Length:    22,
					Decimal:   -1,
				},
				Nullable: true,
			},
			{
				ID:   13,
				Name: "decimalT",
				DataType: dataType{
					MySQLType: "decimal",
					Charset:   "binary",
					Collate:   "binary",
					Length:    10,
				},
				Nullable: true,
			},
			{
				ID:   14,
				Name: "floatTu",
				DataType: dataType{
					MySQLType: "float",
					Charset:   "binary",
					Collate:   "binary",
					Length:    12,
					Decimal:   -1,
					Unsigned:  true,
				},
				Nullable: true,
			},
			{
				ID:   15,
				Name: "doubleTu",
				DataType: dataType{
					MySQLType: "double",
					Charset:   "binary",
					Collate:   "binary",
					Length:    22,
					Decimal:   -1,
					Unsigned:  true,
				},
				Nullable: true,
			},
			{
				ID:   16,
				Name: "decimalTu",
				DataType: dataType{
					MySQLType: "decimal",
					Charset:   "binary",
					Collate:   "binary",
					Length:    10,
					Unsigned:  true,
				},
				Nullable: true,
			},
			{
				ID:   17,
				Name: "varcharT",
				DataType: dataType{
					MySQLType: "varchar",
					Charset:   "utf8mb4",
					Collate:   "utf8mb4_bin",
					Length:    255,
				},
				Nullable: true,
			},
			{
				ID:   18,
				Name: "charT",
				DataType: dataType{
					MySQLType: "char",
					Charset:   "utf8mb4",
					Collate:   "utf8mb4_bin",
					Length:    255,
				},
				Nullable: true,
			},
			{
				ID:   19,
				Name: "binaryT",
				DataType: dataType{
					MySQLType: "binary",
					Charset:   "binary",
					Collate:   "binary",
					Length:    255,
				},
				Nullable: true,
			},
			{
				ID:   20,
				Name: "varbinaryT",
				DataType: dataType{
					MySQLType: "varbinary",
					Charset:   "binary",
					Collate:   "binary",
					Length:    255,
				},
				Nullable: true,
			},
			{
				ID:   21,
				Name: "tinytextT",
				DataType: dataType{
					MySQLType: "tinytext",
					Charset:   "utf8mb4",
					Collate:   "utf8mb4_bin",
					Length:    255,
				},
				Nullable: true,
			},
			{
				ID:   22,
				Name: "textT",
				DataType: dataType{
					MySQLType: "text",
					Charset:   "utf8mb4",
					Collate:   "utf8mb4_bin",
					Length:    65535,
				},
				Nullable: true,
			},
			{
				ID:   23,
				Name: "mediumtextT",
				DataType: dataType{
					MySQLType: "mediumtext",
					Charset:   "utf8mb4",
					Collate:   "utf8mb4_bin",
					Length:    16777215,
				},
				Nullable: true,
			},
			{
				ID:   24,
				Name: "longtextT",
				DataType: dataType{
					MySQLType: "longtext",
					Charset:   "utf8mb4",
					Collate:   "utf8mb4_bin",
					Length:    4294967295,
				},
				Nullable: true,
			},
			{
				ID:   25,
				Name: "tinyblobT",
				DataType: dataType{
					MySQLType: "tinyblob",
					Charset:   "binary",
					Collate:   "binary",
					Length:    255,
				},
				Nullable: true,
			},
			{
				ID:   26,
				Name: "blobT",
				DataType: dataType{
					MySQLType: "blob",
					Charset:   "binary",
					Collate:   "binary",
					Length:    65535,
				},
				Nullable: true,
			},
			{
				ID:   27,
				Name: "mediumblobT",
				DataType: dataType{
					MySQLType: "mediumblob",
					Charset:   "binary",
					Collate:   "binary",
					Length:    16777215,
				},
				Nullable: true,
			},
			{
				ID:   28,
				Name: "longblobT",
				DataType: dataType{
					MySQLType: "longblob",
					Charset:   "binary",
					Collate:   "binary",
					Length:    4294967295,
				},
				Nullable: true,
			},
			{
				ID:   29,
				Name: "dateT",
				DataType: dataType{
					MySQLType: "date",
					Charset:   "binary",
					Collate:   "binary",
					Length:    10,
				},
				Nullable: true,
			},
			{
				ID:   30,
				Name: "datetimeT",
				DataType: dataType{
					MySQLType: "datetime",
					Charset:   "binary",
					Collate:   "binary",
					Length:    19,
				},
				Nullable: true,
			},
			{
				ID:   31,
				Name: "timestampT",
				DataType: dataType{
					MySQLType: "timestamp",
					Charset:   "binary",
					Collate:   "binary",
					Length:    19,
				},
				Nullable: true,
			},
			{
				ID:   32,
				Name: "timeT",
				DataType: dataType{
					MySQLType: "time",
					Charset:   "binary",
					Collate:   "binary",
					Length:    10,
				},
				Nullable: true,
			},
			{
				ID:   33,
				Name: "yearT",
				DataType: dataType{
					MySQLType: "year",
					Charset:   "binary",
					Collate:   "binary",
					Length:    4,
					Unsigned:  true,
					Zerofill:  true,
				},
				Nullable: true,
			},
			{
				ID:   34,
				Name: "enumT",
				DataType: dataType{
					MySQLType: "enum",
					Charset:   "utf8mb4",
					Collate:   "utf8mb4_bin",
					Length:    1,
					Elements:  []string{"a", "b", "c"},
				},
				Nullable: true,
				Default:  "b",
			},
			{
				ID:   35,
				Name: "setT",
				DataType: dataType{
					MySQLType: "set",
					Charset:   "utf8mb4",
					Collate:   "utf8mb4_bin",
					Length:    5,
					Elements:  []string{"a", "b", "c"},
				},
				Nullable: true,
			},
			{
				ID:   36,
				Name: "bitT",
				DataType: dataType{
					MySQLType: "bit",
					Charset:   "binary",
					Collate:   "binary",
					Length:    10,
					Unsigned:  true,
				},
				Nullable: true,
			},
			{
				ID:   37,
				Name: "jsonT",
				DataType: dataType{
					MySQLType: "json",
					Charset:   "binary",
					Collate:   "binary",
					Length:    4294967295,
				},
				Nullable: true,
			},
			{
				ID:   38,
				Name: "tgen",
				DataType: dataType{
					MySQLType: "tinyint",
					Charset:   "binary",
					Collate:   "binary",
					Length:    4,
				},
				Nullable: true,
			},
		},
		Indexes: []*IndexSchema{
			{
				Name:     "primary",
				Unique:   true,
				Primary:  true,
				Nullable: false,
				Columns:  []string{"t"},
			},
		},
	}
	got = newTableSchema(tableInfo)
	require.Equal(t, want, got)
}
