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

package router

import (
	"fmt"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tiflow/dm/pkg/terror"

	oldrouter "github.com/pingcap/tidb-tools/pkg/table-router"
)

var _ = Suite(&testRouteSuite{})

type testRouteSuite struct{}

func TestRoute(t *testing.T) {
	TestingT(t)
}

func (s *testRouteSuite) TestCreateRouter(c *C) {
	_, err := NewRouter(true, []*TableRule{})
	c.Assert(err, Equals, nil)
	_, err = NewRouter(false, []*TableRule{})
	c.Assert(err, Equals, nil)
}

func (s *testRouteSuite) TestAddRule(c *C) {
	r, err := NewRouter(true, []*TableRule{})
	c.Assert(err, Equals, nil)
	rules := []*TableRule{
		{
			SchemaPattern: "test1",
			TargetSchema:  "dtest1",
		},
		{
			SchemaPattern: "test2",
			TablePattern:  "table2",
			TargetSchema:  "dtest2",
			TargetTable:   "dtable2",
		},
	}
	for _, rule := range rules {
		err = r.AddRule(rule)
		c.Assert(err, Equals, nil) // successfully insert
	}
	r, err = NewRouter(false, []*TableRule{})
	c.Assert(err, Equals, nil)
	for _, rule := range rules {
		err := r.AddRule(rule)
		c.Assert(err, Equals, nil) // successfully insert
	}
}

func (s *testRouteSuite) TestSchemaRoute(c *C) {
	rules := []*TableRule{
		{
			SchemaPattern: "test1",
			TargetSchema:  "dtest1",
		},
		{
			SchemaPattern: "gtest*",
			TargetSchema:  "dtest",
		},
	}
	oldRouter, err := oldrouter.NewTableRouter(true, rules)
	c.Assert(err, Equals, nil)
	newRouter, err := NewRouter(true, rules)
	c.Assert(err, Equals, nil)
	inputTables := []Table{
		{
			Schema: "test1", // match rule 1
			Name:   "table1",
		},
		{
			Schema: "gtesttest", // match rule 2
			Name:   "atable",
		},
		{
			Schema: "ptest", // match neither
			Name:   "atableg",
		},
	}
	expectedResult := []Table{
		{
			Schema: "dtest1",
			Name:   "table1",
		},
		{
			Schema: "dtest",
			Name:   "atable",
		},
		{
			Schema: "ptest",
			Name:   "atableg",
		},
	}
	for idx := range inputTables {
		schema, table := inputTables[idx].Schema, inputTables[idx].Name
		expSchema, expTable := expectedResult[idx].Schema, expectedResult[idx].Name
		oldSchema, oldTable, err := oldRouter.Route(schema, table)
		c.Assert(err, Equals, nil)
		newSchema, newTable, err := newRouter.Route(schema, table)
		c.Assert(err, Equals, nil)
		c.Assert(oldSchema, Equals, expSchema)
		c.Assert(oldTable, Equals, expTable)
		c.Assert(newSchema, Equals, expSchema)
		c.Assert(newTable, Equals, expTable)
	}
}

func (s *testRouteSuite) TestTableRoute(c *C) {
	rules := []*TableRule{
		{
			SchemaPattern: "test1",
			TablePattern:  "table1",
			TargetSchema:  "dtest1",
			TargetTable:   "dtable1",
		},
		{
			SchemaPattern: "test*",
			TablePattern:  "table2",
			TargetSchema:  "dtest2",
			TargetTable:   "dtable2",
		},
		{
			SchemaPattern: "test3",
			TablePattern:  "table*",
			TargetSchema:  "dtest3",
			TargetTable:   "dtable3",
		},
	}
	inputTables := []*Table{}
	expTables := []*Table{}
	for i := 1; i <= 3; i++ {
		inputTables = append(inputTables, &Table{
			Schema: fmt.Sprintf("test%d", i),
			Name:   fmt.Sprintf("table%d", i),
		})
		expTables = append(expTables, &Table{
			Schema: fmt.Sprintf("dtest%d", i),
			Name:   fmt.Sprintf("dtable%d", i),
		})
	}
	oldRouter, err := oldrouter.NewTableRouter(true, rules)
	c.Assert(err, Equals, nil)
	newRouter, err := NewRouter(true, rules)
	c.Assert(err, Equals, nil)
	for i := range inputTables {
		schema, table := inputTables[i].Schema, inputTables[i].Name
		expSchema, expTable := expTables[i].Schema, expTables[i].Name
		oldSch, oldTbl, _ := oldRouter.Route(schema, table)
		newSch, newTbl, _ := newRouter.Route(schema, table)
		c.Assert(newSch, Equals, expSchema)
		c.Assert(newTbl, Equals, expTable)
		c.Assert(oldSch, Equals, expSchema)
		c.Assert(oldTbl, Equals, expTable)
	}
}

func (s *testRouteSuite) TestRegExprRoute(c *C) {
	rules := []*TableRule{
		{
			SchemaPattern: "~test.[0-9]+",
			TargetSchema:  "dtest1",
		},
		{
			SchemaPattern: "~test2?[animal|human]",
			TablePattern:  "~tbl.*[cat|dog]+",
			TargetSchema:  "dtest2",
			TargetTable:   "dtable2",
		},
		{
			SchemaPattern: "~test3_(schema)?.*",
			TablePattern:  "test3_*",
			TargetSchema:  "dtest3",
			TargetTable:   "dtable3",
		},
		{
			SchemaPattern: "test4s_*",
			TablePattern:  "~testtable_[donot_delete]?",
			TargetSchema:  "dtest4",
			TargetTable:   "dtable4",
		},
	}
	inputTable := []Table{
		{
			Schema: "tests100",
			Name:   "table1", // match rule 1
		},
		{
			Schema: "test2animal",
			Name:   "tbl_animal_dogcat", // match rule 2
		},
		{
			Schema: "test3_schema_meta",
			Name:   "test3_tail", // match rule 3
		},
		{
			Schema: "test4s_2022",
			Name:   "testtable_donot_delete", // match rule 4
		},
		{
			Schema: "mytst5566",
			Name:   "gtable", // match nothing
		},
	}
	expectedOutput := []Table{
		{
			Schema: "dtest1",
			Name:   "table1",
		},
		{
			Schema: "dtest2",
			Name:   "dtable2",
		},
		{
			Schema: "dtest3",
			Name:   "dtable3",
		},
		{
			Schema: "dtest4",
			Name:   "dtable4",
		},
		{
			Schema: "mytst5566",
			Name:   "gtable",
		},
	}
	newRouter, err := NewRouter(true, rules)
	c.Assert(err, Equals, nil)
	for idx := range inputTable {
		s, n := inputTable[idx].Schema, inputTable[idx].Name
		expSchm, expName := expectedOutput[idx].Schema, expectedOutput[idx].Name
		newSchm, newName, err := newRouter.Route(s, n)
		c.Assert(err, Equals, nil)
		c.Assert(newSchm, Equals, expSchm)
		c.Assert(newName, Equals, expName)
	}
}

func (s *testRouteSuite) TestFetchExtendColumn(c *C) {
	rules := []*TableRule{
		{
			SchemaPattern: "schema*",
			TablePattern:  "t*",
			TargetSchema:  "test",
			TargetTable:   "t",
			TableExtractor: &TableExtractor{
				TargetColumn: "table_name",
				TableRegexp:  "table_(.*)",
			},
			SchemaExtractor: &SchemaExtractor{
				TargetColumn: "schema_name",
				SchemaRegexp: "schema_(.*)",
			},
			SourceExtractor: &SourceExtractor{
				TargetColumn: "source_name",
				SourceRegexp: "source_(.*)_(.*)",
			},
		},
		{
			SchemaPattern: "~s?chema.*",
			TargetSchema:  "test",
			TargetTable:   "t2",
			SchemaExtractor: &SchemaExtractor{
				TargetColumn: "schema_name",
				SchemaRegexp: "(.*)",
			},
			SourceExtractor: &SourceExtractor{
				TargetColumn: "source_name",
				SourceRegexp: "(.*)",
			},
		},
	}
	r, err := NewRouter(false, rules)
	c.Assert(err, IsNil)
	expected := [][]string{
		{"table_name", "schema_name", "source_name"},
		{"t1", "s1", "s1s1"},

		{"schema_name", "source_name"},
		{"schema_s2", "source_s2"},
	}

	// table level rules have highest priority
	extendCol, extendVal := r.FetchExtendColumn("schema_s1", "table_t1", "source_s1_s1")
	c.Assert(expected[0], DeepEquals, extendCol)
	c.Assert(expected[1], DeepEquals, extendVal)

	// only schema rules
	extendCol2, extendVal2 := r.FetchExtendColumn("schema_s2", "a_table_t2", "source_s2")
	c.Assert(expected[2], DeepEquals, extendCol2)
	c.Assert(expected[3], DeepEquals, extendVal2)
}

func (s *testRouteSuite) TestAllRule(c *C) {
	rules := []*TableRule{
		{
			SchemaPattern: "~test.[0-9]+",
			TargetSchema:  "dtest1",
		},
		{
			SchemaPattern: "~test2?[animal|human]",
			TablePattern:  "~tbl.*[cat|dog]+",
			TargetSchema:  "dtest2",
			TargetTable:   "dtable2",
		},
		{
			SchemaPattern: "~test3_(schema)?.*",
			TablePattern:  "test3_*",
			TargetSchema:  "dtest3",
			TargetTable:   "dtable3",
		},
		{
			SchemaPattern: "test4s_*",
			TablePattern:  "~testtable_[donot_delete]?",
			TargetSchema:  "dtest4",
			TargetTable:   "dtable4",
		},
	}
	r, err := NewRouter(true, rules)
	c.Assert(err, Equals, nil)
	schemaRules, tableRules := r.AllRules()
	c.Assert(len(schemaRules), Equals, 1)
	c.Assert(len(tableRules), Equals, 3)
	c.Assert(schemaRules[0].SchemaPattern, Equals, rules[0].SchemaPattern)
	for i := 0; i < 3; i++ {
		c.Assert(tableRules[i].SchemaPattern, Equals, rules[i+1].SchemaPattern)
		c.Assert(tableRules[i].TablePattern, Equals, rules[i+1].TablePattern)
	}
}

func (s *testRouteSuite) TestDupMatch(c *C) {
	rules := []*TableRule{
		{
			SchemaPattern: "~test[0-9]+.*",
			TablePattern:  "~.*",
			TargetSchema:  "dtest1",
		},
		{
			SchemaPattern: "~test2?[a|b]",
			TablePattern:  "~tbl2",
			TargetSchema:  "dtest2",
			TargetTable:   "dtable2",
		},
		{
			SchemaPattern: "mytest*",
			TargetSchema:  "mytest",
		},
		{
			SchemaPattern: "~mytest(_meta)?_schema",
			TargetSchema:  "test",
		},
	}
	inputTables := []Table{
		{
			Schema: "test2a", // match rule1 and rule2
			Name:   "tbl2",
		},
		{
			Schema: "mytest_meta_schema", // match rule3 and rule4
			Name:   "",
		},
	}
	r, err := NewRouter(true, rules)
	c.Assert(err, Equals, nil)
	for i := range inputTables {
		targetSchm, targetTbl, err := r.Route(inputTables[i].Schema, inputTables[i].Name)
		c.Assert(targetSchm, Equals, "")
		c.Assert(targetTbl, Equals, "")
		c.Assert(terror.ErrWorkerRouteTableDupMatch.Equal(err), Equals, true)
	}
}
