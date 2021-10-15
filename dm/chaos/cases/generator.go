// Copyright 2021 PingCAP, Inc.
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
	"context"
	"fmt"
	"math/rand"

	config2 "github.com/pingcap/dm/dm/config"
)

const (
	source1 = iota
	source2
	source3
)

// SQL represents sql statement for a source.
type SQL struct {
	statement string
	source    int
}

// SQLs represents sql statements in a group.
type SQLs []SQL

// Case represents a sqls test case.
type Case []SQLs

var (
	// add some flags column, so we can make sure the order of new column in optimsitic.
	preSQLs1 = SQLs{
		{"ALTER TABLE %s.%s ADD COLUMN case3_flag1 INT, ADD COLUMN case3_flag2 INT, ADD COLUMN case3_flag3 INT", source1},
		{"ALTER TABLE %s.%s ADD COLUMN case3_flag1 INT, ADD COLUMN case3_flag2 INT, ADD COLUMN case3_flag3 INT", source2},
		{"ALTER TABLE %s.%s ADD COLUMN case3_flag1 INT, ADD COLUMN case3_flag2 INT, ADD COLUMN case3_flag3 INT", source3},
		{"ALTER TABLE %s.%s ADD COLUMN case4_flag1 INT, ADD COLUMN case4_flag2 INT, ADD COLUMN case4_flag3 INT", source1},
		{"ALTER TABLE %s.%s ADD COLUMN case4_flag1 INT, ADD COLUMN case4_flag2 INT, ADD COLUMN case4_flag3 INT", source2},
		{"ALTER TABLE %s.%s ADD COLUMN case4_flag1 INT, ADD COLUMN case4_flag2 INT, ADD COLUMN case4_flag3 INT", source3},
		{"ALTER TABLE %s.%s ADD COLUMN case5_flag1 INT, ADD COLUMN case5_flag2 INT, ADD COLUMN case5_flag3 INT;", source1},
		{"ALTER TABLE %s.%s ADD COLUMN case5_flag1 INT, ADD COLUMN case5_flag2 INT, ADD COLUMN case5_flag3 INT;", source2},
		{"ALTER TABLE %s.%s ADD COLUMN case5_flag1 INT, ADD COLUMN case5_flag2 INT, ADD COLUMN case5_flag3 INT;", source3},
		{"ALTER TABLE %s.%s ADD COLUMN case9_flag INT;", source1},
		{"ALTER TABLE %s.%s ADD COLUMN case9_flag INT;", source2},
		{"ALTER TABLE %s.%s ADD COLUMN case9_flag INT;", source3},
	}

	// ALL ADD COLUMN, ALL DROP COLUMN.
	case1 = Case{
		{{"ALTER TABLE %s.%s ADD COLUMN case1 INT;", source1}},
		{{"ALTER TABLE %s.%s ADD COLUMN case1 INT;", source2}},
		{{"ALTER TABLE %s.%s ADD COLUMN case1 INT;", source3}},
		{{"ALTER TABLE %s.%s DROP COLUMN case1;", source1}},
		{{"ALTER TABLE %s.%s DROP COLUMN case1;", source2}},
		{{"ALTER TABLE %s.%s DROP COLUMN case1;", source3}},
	}
	// ADD COLUMN, DROP COLUMN for one source.
	case2 = Case{
		{{"ALTER TABLE %s.%s ADD COLUMN case2 INT;", source1}},
		{{"ALTER TABLE %s.%s DROP COLUMN case2;", source1}},
	}
	// ADD columns out of order.
	case3 = Case{
		{
			{"ALTER TABLE %s.%s DROP COLUMN case3_1;", source1},
			{"ALTER TABLE %s.%s DROP COLUMN case3_2;", source1},
			{"ALTER TABLE %s.%s DROP COLUMN case3_3;", source1},
			{"ALTER TABLE %s.%s DROP COLUMN case3_1;", source2},
			{"ALTER TABLE %s.%s DROP COLUMN case3_2;", source2},
			{"ALTER TABLE %s.%s DROP COLUMN case3_3;", source2},
			{"ALTER TABLE %s.%s DROP COLUMN case3_1;", source3},
			{"ALTER TABLE %s.%s DROP COLUMN case3_2;", source3},
			{"ALTER TABLE %s.%s DROP COLUMN case3_3;", source3},
		},
		{{"ALTER TABLE %s.%s ADD COLUMN case3_1 INT AFTER case3_flag1;", source1}},
		{{"ALTER TABLE %s.%s ADD COLUMN case3_2 INT AFTER case3_flag2;", source2}},
		{{"ALTER TABLE %s.%s ADD COLUMN case3_3 INT AFTER case3_flag3;", source3}},
		{{"ALTER TABLE %s.%s ADD COLUMN case3_2 INT AFTER case3_flag2;", source1}},
		{{"ALTER TABLE %s.%s ADD COLUMN case3_3 INT AFTER case3_flag3;", source2}},
		{{"ALTER TABLE %s.%s ADD COLUMN case3_1 INT AFTER case3_flag1;", source3}},
		{{"ALTER TABLE %s.%s ADD COLUMN case3_3 INT AFTER case3_flag3;", source1}},
		{{"ALTER TABLE %s.%s ADD COLUMN case3_1 INT AFTER case3_flag1;", source2}},
		{{"ALTER TABLE %s.%s ADD COLUMN case3_2 INT AFTER case3_flag2;", source3}},
	}
	// MULTIPLE ADD COLUMN out of order.
	case4 = Case{
		{
			{"ALTER TABLE %s.%s DROP COLUMN case4_1, DROP COLUMN case4_2, DROP COLUMN case4_3;", source1},
			{"ALTER TABLE %s.%s DROP COLUMN case4_1, DROP COLUMN case4_2, DROP COLUMN case4_3;", source2},
			{"ALTER TABLE %s.%s DROP COLUMN case4_1, DROP COLUMN case4_2, DROP COLUMN case4_3;", source3},
		},
		{{"ALTER TABLE %s.%s ADD COLUMN case4_1 INT AFTER case4_flag1, ADD COLUMN case4_2 INT AFTER case4_flag2, ADD COLUMN case4_3 INT AFTER case4_flag3;", source1}},
		{{"ALTER TABLE %s.%s ADD COLUMN case4_2 INT AFTER case4_flag2, ADD COLUMN case4_3 INT AFTER case4_flag3, ADD COLUMN case4_1 INT AFTER case4_flag1;", source2}},
		{{"ALTER TABLE %s.%s ADD COLUMN case4_3 INT AFTER case4_flag3, ADD COLUMN case4_1 INT AFTER case4_flag1, ADD COLUMN case4_2 INT AFTER case4_flag2;", source3}},
	}
	// MULTIPLE ADD COLUMN vs ADD columns.
	case5 = Case{
		{
			{"ALTER TABLE %s.%s DROP COLUMN case5_1;", source1},
			{"ALTER TABLE %s.%s DROP COLUMN case5_2;", source1},
			{"ALTER TABLE %s.%s DROP COLUMN case5_3;", source1},
			{"ALTER TABLE %s.%s DROP COLUMN case5_1;", source2},
			{"ALTER TABLE %s.%s DROP COLUMN case5_2;", source2},
			{"ALTER TABLE %s.%s DROP COLUMN case5_3;", source2},
			{"ALTER TABLE %s.%s DROP COLUMN case5_1;", source3},
			{"ALTER TABLE %s.%s DROP COLUMN case5_2;", source3},
			{"ALTER TABLE %s.%s DROP COLUMN case5_3;", source3},
		},
		{{"ALTER TABLE %s.%s ADD COLUMN case5_1 INT AFTER case5_flag1;", source1}},
		{{"ALTER TABLE %s.%s ADD COLUMN case5_2 INT AFTER case5_flag2, ADD COLUMN case5_1 INT AFTER case5_flag1;", source2}},
		{{"ALTER TABLE %s.%s ADD COLUMN case5_3 INT AFTER case5_flag3, ADD COLUMN case5_1 INT AFTER case5_flag1, ADD COLUMN case5_2 INT AFTER case5_flag2;", source3}},
		{{"ALTER TABLE %s.%s ADD COLUMN case5_3 INT AFTER case5_flag3;", source2}},
		{{"ALTER TABLE %s.%s ADD COLUMN case5_2 INT AFTER case5_flag2, ADD COLUMN case5_3 INT AFTER case5_flag3;", source1}},
	}
	// ALL ADD INDEX, ALL DROP INDEX.
	case6 = Case{
		{{"ALTER TABLE %s.%s ADD INDEX case6_idx(case3_flag1);", source1}},
		{{"ALTER TABLE %s.%s ADD INDEX case6_idx(case3_flag1);", source2}},
		{{"ALTER TABLE %s.%s ADD INDEX case6_idx(case3_flag1);", source3}},
		{{"ALTER TABLE %s.%s DROP INDEX case6_idx;", source1}},
		{{"ALTER TABLE %s.%s DROP INDEX case6_idx;", source2}},
		{{"ALTER TABLE %s.%s DROP INDEX case6_idx;", source3}},
	}
	// ADD INDEX, DROP INDEX for one source.
	case7 = Case{
		{{"ALTER TABLE %s.%s ADD INDEX case7_idx(uuid);", source1}},
		{{"ALTER TABLE %s.%s DROP INDEX case7_idx;", source1}},
	}
	// ADD MULTI-COLUMN INDEX.
	case8 = Case{
		{
			{"ALTER TABLE %s.%s DROP INDEX case8_idx;", source1},
			{"ALTER TABLE %s.%s DROP INDEX case8_idx;", source2},
			{"ALTER TABLE %s.%s DROP INDEX case8_idx;", source3},
		},
		{{"ALTER TABLE %s.%s ADD INDEX case8_idx(case4_flag1, case4_flag2, case4_flag3);", source1}},
		{{"ALTER TABLE %s.%s ADD INDEX case8_idx(case4_flag1, case4_flag2, case4_flag3);", source2}},
		{{"ALTER TABLE %s.%s ADD INDEX case8_idx(case4_flag1, case4_flag2, case4_flag3);", source3}},
	}
	// ADD COLUMN AND INDEX.
	case9 = Case{
		{
			{"ALTER TABLE %s.%s DROP INDEX case9_idx;", source1},
			{"ALTER TABLE %s.%s DROP INDEX case9_idx;", source2},
			{"ALTER TABLE %s.%s DROP INDEX case9_idx;", source3},
			{"ALTER TABLE %s.%s DROP COLUMN case9;", source1},
			{"ALTER TABLE %s.%s DROP COLUMN case9;", source2},
			{"ALTER TABLE %s.%s DROP COLUMN case9;", source3},
		},
		{{"ALTER TABLE %s.%s ADD COLUMN case9 INT AFTER case9_flag;", source1}},
		{{"ALTER TABLE %s.%s ADD INDEX case9_idx(case9);", source1}},
		{{"ALTER TABLE %s.%s ADD COLUMN case9 INT AFTER case9_flag;", source2}},
		{{"ALTER TABLE %s.%s ADD INDEX case9_idx(case9);", source2}},
		{{"ALTER TABLE %s.%s ADD COLUMN case9 INT AFTER case9_flag;", source3}},
		{{"ALTER TABLE %s.%s ADD INDEX case9_idx(case9);", source3}},
	}
	cases = map[string][]Case{
		config2.ShardOptimistic: {
			case1,
			case2,
			case3,
			case4,
			case5,
			case6,
			case7,
			case8,
			case9,
		},
		config2.ShardPessimistic: {},
		"":                       {},
	}
	preSQLs = map[string]SQLs{
		config2.ShardOptimistic:  preSQLs1,
		config2.ShardPessimistic: nil,
		"":                       nil,
	}
)

// CaseGenerator generator test cases.
type CaseGenerator struct {
	testCases   []Case
	testPreSQLs SQLs
	sqlsChan    chan SQLs
	schema      string
	tables      []string
}

// NewCaseGenerator creates a new CaseGenerator instance.
func NewCaseGenerator(shardMode string) *CaseGenerator {
	g := &CaseGenerator{
		testCases:   cases[shardMode],
		sqlsChan:    make(chan SQLs),
		testPreSQLs: preSQLs[shardMode],
	}
	return g
}

// Start starts to generate sqls case.
func (g *CaseGenerator) Start(ctx context.Context, schema string, tables []string) {
	g.schema = schema
	g.tables = tables
	go g.genSQLs(ctx)
}

func (g *CaseGenerator) genSQLs(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			for _, table := range g.tables {
				rand.Shuffle(len(g.testCases), func(i, j int) { g.testCases[i], g.testCases[j] = g.testCases[j], g.testCases[i] })
				casesNum := rand.Intn(len(g.testCases) + 1)
				for i := 0; i < casesNum; i++ {
					for _, sqls := range g.testCases[i] {
						fullSqls := make(SQLs, len(sqls))
						copy(fullSqls, sqls)
						for idx, sql := range fullSqls {
							fullSqls[idx].statement = fmt.Sprintf(sql.statement, g.schema, table)
						}
						g.sqlsChan <- fullSqls
					}
				}
			}
			g.sqlsChan <- nil
		}
	}
}

// GetSQLs gets sql from CaseGenerator.
func (g *CaseGenerator) GetSQLs() SQLs {
	return <-g.sqlsChan
}

// GetPreSQLs gets preSQLs from CaseGenerator.
func (g *CaseGenerator) GetPreSQLs() SQLs {
	testPreSQLs := make(SQLs, 0, len(g.testPreSQLs)*len(g.tables))
	for _, table := range g.tables {
		for _, sql := range g.testPreSQLs {
			testPreSQLs = append(testPreSQLs, SQL{fmt.Sprintf(sql.statement, g.schema, table), sql.source})
		}
	}
	return testPreSQLs
}
