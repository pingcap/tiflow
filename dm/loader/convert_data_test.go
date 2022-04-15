// Copyright 2019 PingCAP, Inc.
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

package loader

import (
	cm "github.com/pingcap/tidb-tools/pkg/column-mapping"

	regexprrouter "github.com/pingcap/tidb/util/regexpr-router"
	router "github.com/pingcap/tidb/util/table-router"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"

	. "github.com/pingcap/check"
)

var _ = Suite(&testConvertDataSuite{})

type testConvertDataSuite struct{}

func (t *testConvertDataSuite) TestReassemble(c *C) {
	table := &tableInfo{
		sourceSchema: "test2",
		sourceTable:  "t3",
		targetSchema: "test",
		targetTable:  "t",
		columnNameList: []string{
			"id",
			"t_boolean",
			"t_bigint",
			"t_double",
			"t_decimal",
			"t_bit",
			"t_date",
			"t_datetime",
			"t_timestamp",
			"t_time",
			"t_year",
			"t_char",
			"t_varchar",
			"t_blob",
			"t_text",
			"t_enum",
			"t_set",
		},
		insertHeadStmt: "INSERT INTO t VALUES",
	}

	// nolint:stylecheck
	sql := `INSERT INTO t1 VALUES
(10,1,9223372036854775807,123.123,123456789012.1234567890120000000,"\0\0\0\0\0\0\0A","1000-01-01","9999-12-31 23:59:59","1973-12-30 15:30:00","23:59:59",1970,"x","x\"x","blob","text","enum2","a,b"),
(9,1,9223372036854775807,123.123,123456789012.1234567890120000000,"\0\0\0\0\0\0\0A","1000-01-01","9999-12-31 23:59:59","1973-12-30 15:30:00","23:59:59",1970,"x","x\",\nx","blob","text","enum2","a,b");
(8,1,9223372036854775807,123.123,123456789012.1234567890120000000,"\0\0\0\0\0\0\0A","1000-01-01","9999-12-31 23:59:59","1973-12-30 15:30:00","23:59:59",1970,"x","x\",\nx","blob","text\n","enum2",  "	a,b ");
`

	expected := []string{
		// nolint:stylecheck
		`INSERT INTO t VALUES(585520728116297738,1,9223372036854775807,123.123,123456789012.1234567890120000000,"\0\0\0\0\0\0\0A","1000-01-01","9999-12-31 23:59:59","1973-12-30 15:30:00","23:59:59",1970,"x","x\"x","blob","text","enum2","a,b"),(585520728116297737,1,9223372036854775807,123.123,123456789012.1234567890120000000,"\0\0\0\0\0\0\0A","1000-01-01","9999-12-31 23:59:59","1973-12-30 15:30:00","23:59:59",1970,"x","x\",\nx","blob","text","enum2","a,b"),(585520728116297736,1,9223372036854775807,123.123,123456789012.1234567890120000000,"\0\0\0\0\0\0\0A","1000-01-01","9999-12-31 23:59:59","1973-12-30 15:30:00","23:59:59",1970,"x","x\",\nx","blob","text\n","enum2","	a,b ");`,
		// nolint:stylecheck
		`INSERT INTO t VALUES(10,1,9223372036854775807,123.123,123456789012.1234567890120000000,"\0\0\0\0\0\0\0A","1000-01-01","9999-12-31 23:59:59","1973-12-30 15:30:00","23:59:59",1970,"test:x","x\"x","blob","text","enum2","a,b"),(9,1,9223372036854775807,123.123,123456789012.1234567890120000000,"\0\0\0\0\0\0\0A","1000-01-01","9999-12-31 23:59:59","1973-12-30 15:30:00","23:59:59",1970,"test:x","x\",\nx","blob","text","enum2","a,b"),(8,1,9223372036854775807,123.123,123456789012.1234567890120000000,"\0\0\0\0\0\0\0A","1000-01-01","9999-12-31 23:59:59","1973-12-30 15:30:00","23:59:59",1970,"test:x","x\",\nx","blob","text\n","enum2","	a,b ");`,
	}

	rules := []*cm.Rule{
		{
			PatternSchema: "test*",
			PatternTable:  "t*",
			TargetColumn:  "id",
			Expression:    cm.PartitionID,
			Arguments:     []string{"1", "test", "t"},
		}, {
			PatternSchema: "test*",
			PatternTable:  "t*",
			TargetColumn:  "t_char",
			Expression:    cm.AddPrefix,
			Arguments:     []string{"test:"},
		},
	}

	for i, r := range rules {
		columnMapping, err := cm.NewMapping(false, []*cm.Rule{r})
		c.Assert(err, IsNil)

		query, err := reassemble([]byte(sql), table, columnMapping)
		c.Assert(err, IsNil)
		c.Assert(expected[i], Equals, query)
	}
}

func (t *testConvertDataSuite) TestReassembleWithGeneratedColumn(c *C) {
	table := &tableInfo{
		sourceSchema: "test2",
		sourceTable:  "t3",
		targetSchema: "test",
		targetTable:  "t",
		columnNameList: []string{
			"id",
			"t_json",
		},
		insertHeadStmt: "INSERT INTO t (`id`,`t_json`) VALUES",
	}
	sql := `INSERT INTO t1 (id,t_json) VALUES
(10,'{}'),
(9,NULL);
(8,'{"a":123}');
`
	expected := "INSERT INTO t (`id`,`t_json`) VALUES(585520728116297738,'{}'),(585520728116297737,NULL),(585520728116297736,'{\"a\":123}');"
	rules := []*cm.Rule{
		{
			PatternSchema: "test*",
			PatternTable:  "t*",
			TargetColumn:  "id",
			Expression:    cm.PartitionID,
			Arguments:     []string{"1", "test", "t"},
		},
	}

	columnMapping, err := cm.NewMapping(false, rules)
	c.Assert(err, IsNil)
	query, err := reassemble([]byte(sql), table, columnMapping)
	c.Assert(err, IsNil)
	c.Assert(query, Equals, expected)
}

func (t *testConvertDataSuite) TestParseTable(c *C) {
	rules := []*router.TableRule{{
		SchemaPattern: "test*",
		TablePattern:  "t*",
		TargetSchema:  "test",
		TargetTable:   "t",
	}}

	expectedTableInfo := &tableInfo{
		sourceSchema: "test1",
		sourceTable:  "t2",
		targetSchema: "test",
		targetTable:  "t",
		columnNameList: []string{
			"id",
			"t_boolean",
			"t_bigint",
			"t_double",
			"t_decimal",
			"t_bit",
			"t_date",
			"t_datetime",
			"t_timestamp",
			"t_time",
			"t_year",
			"t_char",
			"t_varchar",
			"t_blob",
			"t_text",
			"t_enum",
			"t_set",
			"t_json",
		},
		insertHeadStmt: "INSERT INTO `t` VALUES",
	}

	r, err := regexprrouter.NewRegExprRouter(false, rules)
	c.Assert(err, IsNil)

	tableInfo, err := parseTable(tcontext.Background(), r, "test1", "t2", "./dumpfile/test1.t2-schema.sql", "ANSI_QUOTES", "source-mysql-01")
	c.Assert(err, IsNil)
	c.Assert(tableInfo, DeepEquals, expectedTableInfo)
}

func (t *testConvertDataSuite) TestParseTableWithGeneratedColumn(c *C) {
	rules := []*router.TableRule{{
		SchemaPattern: "test*",
		TablePattern:  "t*",
		TargetSchema:  "test",
		TargetTable:   "t",
	}}

	expectedTableInfo := &tableInfo{
		sourceSchema: "test1",
		sourceTable:  "t3",
		targetSchema: "test",
		targetTable:  "t",
		columnNameList: []string{
			"id",
			"t_json",
		},
		insertHeadStmt: "INSERT INTO `t` (`id`,`t_json`) VALUES",
	}

	r, err := regexprrouter.NewRegExprRouter(false, rules)
	c.Assert(err, IsNil)

	tableInfo, err := parseTable(tcontext.Background(), r, "test1", "t3", "./dumpfile/test1.t3-schema.sql", "", "source-mysql-01")
	c.Assert(err, IsNil)
	c.Assert(tableInfo, DeepEquals, expectedTableInfo)
}

func (t *testConvertDataSuite) TestParseRowValues(c *C) {
	var (
		data = []byte("585520728116297738")
		ti   = &tableInfo{
			sourceSchema:   "test_parse_rows_values",
			sourceTable:    "tbl_1",
			targetSchema:   "test_parse_rows_values",
			targetTable:    "tbl_1",
			columnNameList: []string{"c1"},
		}
		rules = []*cm.Rule{
			{
				PatternSchema: "test_parse_rows_values",
				PatternTable:  "tbl_1",
				TargetColumn:  "c1",
				Expression:    cm.PartitionID,
				Arguments:     []string{"1", "", ""},
			},
		}
	)

	columnMapping, err := cm.NewMapping(false, rules)
	c.Assert(err, IsNil)

	values, err := parseRowValues(data, ti, columnMapping)
	c.Assert(err, ErrorMatches, ".*mapping row data \\[585520728116297738\\] for table.*")
	c.Assert(values, IsNil)
}

func (t *testConvertDataSuite) TestReassembleExtractor(c *C) {
	table := &tableInfo{
		sourceSchema: "test2",
		sourceTable:  "t3",
		targetSchema: "test",
		targetTable:  "t",
		columnNameList: []string{
			"id",
			"t_boolean",
			"t_bigint",
			"t_double",
			"t_decimal",
			"t_bit",
			"t_date",
			"t_datetime",
			"t_timestamp",
			"t_time",
			"t_year",
			"t_char",
			"t_varchar",
			"t_blob",
			"t_text",
			"t_enum",
			"t_set",
			"table_name",
			"schema_name",
			"source_name",
		},
		insertHeadStmt: "INSERT INTO t VALUES",
		extendCol:      []string{"table_name", "schema_name", "source_name"},
		extendVal:      []string{"table1", "schema1", "source1"},
	}

	// nolint:stylecheck
	sql := `INSERT INTO t1 VALUES
(10,1,9223372036854775807,123.123,123456789012.1234567890120000000,"\0\0\0\0\0\0\0A","1000-01-01","9999-12-31 23:59:59","1973-12-30 15:30:00","23:59:59",1970,"x","x\"x","blob","text","enum2","a,b"),
(9,1,9223372036854775807,123.123,123456789012.1234567890120000000,"\0\0\0\0\0\0\0A","1000-01-01","9999-12-31 23:59:59","1973-12-30 15:30:00","23:59:59",1970,"x","x\",\nx","blob","text","enum2","a,b");
(8,1,9223372036854775807,123.123,123456789012.1234567890120000000,"\0\0\0\0\0\0\0A","1000-01-01","9999-12-31 23:59:59","1973-12-30 15:30:00","23:59:59",1970,"x","x\",\nx","blob","text\n","enum2",  "	a,b ");
`

	expected := []string{
		// nolint:stylecheck
		`INSERT INTO t VALUES(585520728116297738,1,9223372036854775807,123.123,123456789012.1234567890120000000,"\0\0\0\0\0\0\0A","1000-01-01","9999-12-31 23:59:59","1973-12-30 15:30:00","23:59:59",1970,"x","x\"x","blob","text","enum2","a,b",'table1','schema1','source1'),(585520728116297737,1,9223372036854775807,123.123,123456789012.1234567890120000000,"\0\0\0\0\0\0\0A","1000-01-01","9999-12-31 23:59:59","1973-12-30 15:30:00","23:59:59",1970,"x","x\",\nx","blob","text","enum2","a,b",'table1','schema1','source1'),(585520728116297736,1,9223372036854775807,123.123,123456789012.1234567890120000000,"\0\0\0\0\0\0\0A","1000-01-01","9999-12-31 23:59:59","1973-12-30 15:30:00","23:59:59",1970,"x","x\",\nx","blob","text\n","enum2","	a,b ",'table1','schema1','source1');`,
		// nolint:stylecheck
		`INSERT INTO t VALUES(10,1,9223372036854775807,123.123,123456789012.1234567890120000000,"\0\0\0\0\0\0\0A","1000-01-01","9999-12-31 23:59:59","1973-12-30 15:30:00","23:59:59",1970,"test:x","x\"x","blob","text","enum2","a,b",'table1','schema1','source1'),(9,1,9223372036854775807,123.123,123456789012.1234567890120000000,"\0\0\0\0\0\0\0A","1000-01-01","9999-12-31 23:59:59","1973-12-30 15:30:00","23:59:59",1970,"test:x","x\",\nx","blob","text","enum2","a,b",'table1','schema1','source1'),(8,1,9223372036854775807,123.123,123456789012.1234567890120000000,"\0\0\0\0\0\0\0A","1000-01-01","9999-12-31 23:59:59","1973-12-30 15:30:00","23:59:59",1970,"test:x","x\",\nx","blob","text\n","enum2","	a,b ",'table1','schema1','source1');`,
	}

	rules := []*cm.Rule{
		{
			PatternSchema: "test*",
			PatternTable:  "t*",
			TargetColumn:  "id",
			Expression:    cm.PartitionID,
			Arguments:     []string{"1", "test", "t"},
		}, {
			PatternSchema: "test*",
			PatternTable:  "t*",
			TargetColumn:  "t_char",
			Expression:    cm.AddPrefix,
			Arguments:     []string{"test:"},
		},
	}

	for i, r := range rules {
		columnMapping, err := cm.NewMapping(false, []*cm.Rule{r})
		c.Assert(err, IsNil)
		// extract column with column mapping
		query, err := reassemble([]byte(sql), table, columnMapping)
		c.Assert(err, IsNil)
		c.Assert(expected[i], Equals, query)
	}

	expected = []string{
		// nolint:stylecheck
		`INSERT INTO t VALUES(10,1,9223372036854775807,123.123,123456789012.1234567890120000000,"\0\0\0\0\0\0\0A","1000-01-01","9999-12-31 23:59:59","1973-12-30 15:30:00","23:59:59",1970,"x","x\"x","blob","text","enum2","a,b",'table1','schema1','source1'),(9,1,9223372036854775807,123.123,123456789012.1234567890120000000,"\0\0\0\0\0\0\0A","1000-01-01","9999-12-31 23:59:59","1973-12-30 15:30:00","23:59:59",1970,"x","x\",\nx","blob","text","enum2","a,b",'table1','schema1','source1'),(8,1,9223372036854775807,123.123,123456789012.1234567890120000000,"\0\0\0\0\0\0\0A","1000-01-01","9999-12-31 23:59:59","1973-12-30 15:30:00","23:59:59",1970,"x","x\",\nx","blob","text\n","enum2","	a,b ",'table1','schema1','source1');`,
	}
	// only extract column
	query, err := reassemble([]byte(sql), table, nil)
	c.Assert(err, IsNil)
	c.Assert(expected[0], Equals, query)
}

func (t *testConvertDataSuite) TestReassembleWithGeneratedColumnExtractor(c *C) {
	table := &tableInfo{
		sourceSchema: "test2",
		sourceTable:  "t3",
		targetSchema: "test",
		targetTable:  "t",
		columnNameList: []string{
			"id",
			"t_json",
			"table_name",
			"schema_name",
			"source_name",
		},
		insertHeadStmt: "INSERT INTO t (`id`,`t_json`,`table_name`,`schema_name`,`source_name`) VALUES",
		extendCol:      []string{"table_name", "schema_name", "source_name"},
		extendVal:      []string{"table1", "schema1", "source1"},
	}
	sql := `INSERT INTO t1 (id,t_json) VALUES
(10,'{}'),
(9,NULL);
(8,'{"a":123}');
`
	expected := "INSERT INTO t (`id`,`t_json`,`table_name`,`schema_name`,`source_name`) VALUES(585520728116297738,'{}','table1','schema1','source1'),(585520728116297737,NULL,'table1','schema1','source1'),(585520728116297736,'{\"a\":123}','table1','schema1','source1');"
	rules := []*cm.Rule{
		{
			PatternSchema: "test*",
			PatternTable:  "t*",
			TargetColumn:  "id",
			Expression:    cm.PartitionID,
			Arguments:     []string{"1", "test", "t"},
		},
	}

	columnMapping, err := cm.NewMapping(false, rules)
	c.Assert(err, IsNil)
	query, err := reassemble([]byte(sql), table, columnMapping)
	c.Assert(err, IsNil)
	c.Assert(query, Equals, expected)

	// only extract
	expected2 := "INSERT INTO t (`id`,`t_json`,`table_name`,`schema_name`,`source_name`) VALUES(10,'{}','table1','schema1','source1'),(9,NULL,'table1','schema1','source1'),(8,'{\"a\":123}','table1','schema1','source1');"
	query2, err := reassemble([]byte(sql), table, nil)
	c.Assert(err, IsNil)
	c.Assert(query2, Equals, expected2)
}

func (t *testConvertDataSuite) TestParseTableWithExtendColumn(c *C) {
	rules := []*router.TableRule{{
		SchemaPattern: "test*",
		TablePattern:  "t*",
		TargetSchema:  "test",
		TargetTable:   "t",
		TableExtractor: &router.TableExtractor{
			TargetColumn: "table_name",
			TableRegexp:  "(.*)",
		},
		SchemaExtractor: &router.SchemaExtractor{
			TargetColumn: "schema_name",
			SchemaRegexp: "(.*)",
		},
		SourceExtractor: &router.SourceExtractor{
			TargetColumn: "source_name",
			SourceRegexp: "(.*)",
		},
	}}

	expectedTableInfo := &tableInfo{
		sourceSchema: "test1",
		sourceTable:  "t2",
		targetSchema: "test",
		targetTable:  "t",
		columnNameList: []string{
			"id",
			"t_boolean",
			"t_bigint",
			"t_double",
			"t_decimal",
			"t_bit",
			"t_date",
			"t_datetime",
			"t_timestamp",
			"t_time",
			"t_year",
			"t_char",
			"t_varchar",
			"t_blob",
			"t_text",
			"t_enum",
			"t_set",
			"t_json",
			"table_name",
			"schema_name",
			"source_name",
		},
		insertHeadStmt: "INSERT INTO `t` VALUES",
		extendCol:      []string{"table_name", "schema_name", "source_name"},
		extendVal:      []string{"t2", "test1", "source1"},
	}

	r, err := regexprrouter.NewRegExprRouter(false, rules)
	c.Assert(err, IsNil)

	tableInfo, err := parseTable(tcontext.Background(), r, "test1", "t2", "./dumpfile/test1.t2-schema.sql", "ANSI_QUOTES", "source1")
	c.Assert(err, IsNil)
	c.Assert(tableInfo, DeepEquals, expectedTableInfo)
}

func (t *testConvertDataSuite) TestParseTableWithGeneratedColumnExtendColumn(c *C) {
	rules := []*router.TableRule{{
		SchemaPattern: "test*",
		TablePattern:  "t*",
		TargetSchema:  "test",
		TargetTable:   "t",
		TableExtractor: &router.TableExtractor{
			TargetColumn: "table_name",
			TableRegexp:  "(.*)",
		},
		SchemaExtractor: &router.SchemaExtractor{
			TargetColumn: "schema_name",
			SchemaRegexp: "(.*)",
		},
		SourceExtractor: &router.SourceExtractor{
			TargetColumn: "source_name",
			SourceRegexp: "(.*)",
		},
	}}

	expectedTableInfo := &tableInfo{
		sourceSchema: "test1",
		sourceTable:  "t3",
		targetSchema: "test",
		targetTable:  "t",
		columnNameList: []string{
			"id",
			"t_json",
			"table_name",
			"schema_name",
			"source_name",
		},
		insertHeadStmt: "INSERT INTO `t` (`id`,`t_json`,`table_name`,`schema_name`,`source_name`) VALUES",
		extendCol:      []string{"table_name", "schema_name", "source_name"},
		extendVal:      []string{"t3", "test1", "source1"},
	}

	r, err := regexprrouter.NewRegExprRouter(false, rules)
	c.Assert(err, IsNil)

	tableInfo, err := parseTable(tcontext.Background(), r, "test1", "t3", "./dumpfile/test1.t3-schema.sql", "", "source1")
	c.Assert(err, IsNil)
	c.Assert(tableInfo, DeepEquals, expectedTableInfo)
}
