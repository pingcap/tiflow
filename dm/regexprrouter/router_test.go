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

package regexprrouter

import (
	"fmt"
	"testing"

	router "github.com/pingcap/tidb-tools/pkg/table-router"
	"github.com/stretchr/testify/assert"
)

func TestCreateRouter(t *testing.T) {
	_, err := NewRegExprRouter(true, []*TableRule{})
	assert.Equal(t, nil, err)
	_, err = NewRegExprRouter(false, []*TableRule{})
	assert.Equal(t, nil, err)
}

func TestAddRule(t *testing.T) {
	r, err := NewRegExprRouter(true, []*TableRule{})
	assert.Equal(t, nil, err)
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
		assert.Equal(t, nil, err) // successfully insert
	}
	r, err = NewRegExprRouter(false, []*TableRule{})
	assert.Equal(t, nil, err)
	for _, rule := range rules {
		err := r.AddRule(rule)
		assert.Equal(t, nil, err) // successfully insert
	}
}

func TestSchemaRoute(t *testing.T) {
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
	oldRouter, err := router.NewTableRouter(true, rules)
	assert.Equal(t, nil, err)
	newRouter, err := NewRegExprRouter(true, rules)
	assert.Equal(t, nil, err)
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
		assert.Equal(t, nil, err)
		newSchema, newTable, err := newRouter.Route(schema, table)
		assert.Equal(t, nil, err)
		assert.Equal(t, expSchema, oldSchema)
		assert.Equal(t, expTable, oldTable)
		assert.Equal(t, expSchema, newSchema)
		assert.Equal(t, expTable, newTable)
	}
}

func TestTableRoute(t *testing.T) {
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
	oldRouter, err := router.NewTableRouter(true, rules)
	assert.Equal(t, nil, err)
	newRouter, err := NewRegExprRouter(true, rules)
	assert.Equal(t, nil, err)
	for i := range inputTables {
		schema, table := inputTables[i].Schema, inputTables[i].Name
		expSchema, expTable := expTables[i].Schema, expTables[i].Name
		oldSch, oldTbl, _ := oldRouter.Route(schema, table)
		newSch, newTbl, _ := newRouter.Route(schema, table)
		assert.Equal(t, expSchema, newSch)
		assert.Equal(t, expTable, newTbl)
		assert.Equal(t, expSchema, oldSch)
		assert.Equal(t, expTable, oldTbl)
	}
}

func TestRegExprRoute(t *testing.T) {
	rules := []*TableRule{
		{
			SchemaPattern: "~test.[0-9]+",
			TargetSchema:  "dtest1",
		},
		{
			SchemaPattern: "~test2?[ab|cd]",
			TablePattern:  "~g*[cat|dog]+[0-9]*",
			TargetSchema:  "dtest2",
			TargetTable:   "dtable2",
		},
		{
			SchemaPattern: "~test3(gd|ct)?8917*",
			TablePattern:  "test3_*",
			TargetSchema:  "dtest3",
			TargetTable:   "dtable3",
		},
		{
			SchemaPattern: "test4s_*",
			TablePattern:  "~testtable4?(ddd|lbc)+",
			TargetSchema:  "dtest4",
			TargetTable:   "dtable4",
		},
	}
	inputTable := []Table{
		{
			Schema: "tests99",
			Name:   "table1", // match rule 1
		},
		{
			Schema: "testcd",
			Name:   "ggggdog9897", // match rule 2
		},
		{
			Schema: "test3gd8917777",
			Name:   "test3_99aa99", // match rule 3
		},
		{
			Schema: "test4s_2022",
			Name:   "testtablelbclbc", // match rule 4
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
	newRouter, err := NewRegExprRouter(true, rules)
	assert.Equal(t, nil, err)
	for idx := range inputTable {
		s, n := inputTable[idx].Schema, inputTable[idx].Name
		expSchm, expName := expectedOutput[idx].Schema, expectedOutput[idx].Name
		newSchm, newName, err := newRouter.Route(s, n)
		assert.Equal(t, nil, err)
		assert.Equal(t, expSchm, newSchm)
		assert.Equal(t, expName, newName)
	}
}
