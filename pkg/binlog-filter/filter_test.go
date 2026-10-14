// Copyright 2018 PingCAP, Inc.
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

package filter

import (
	"testing"

	"github.com/pingcap/errors"
	selector "github.com/pingcap/tidb/pkg/util/table-rule-selector"
	"github.com/stretchr/testify/require"
)

func TestFilter(t *testing.T) {
	rules := []*BinlogEventRule{
		{"Test_1_*", "abc*", []EventType{DeleteEvent, InsertEvent, CreateIndex, DropIndex, DropView}, []string{"^DROP\\s+PROCEDURE", "^CREATE\\s+PROCEDURE"}, nil, Ignore},
		{"xxx_*", "abc_*", []EventType{AllDML, NoneDDL}, nil, nil, Ignore},
		{"yyy_*", "abc_*", []EventType{EventType("ALL DML")}, nil, nil, Do},
		{"cdc", "t1", []EventType{RebaseAutoID}, nil, nil, Ignore},
	}

	cases := []struct {
		schema, table string
		event         EventType
		sql           string
		action        ActionType
	}{
		{"test_1_a", "abc1", DeleteEvent, "", Ignore},
		{"test_1_a", "abc1", InsertEvent, "", Ignore},
		{"test_1_a", "abc1", UpdateEvent, "", Do},
		{"test_1_a", "abc1", CreateIndex, "", Ignore},
		{"test_1_a", "abc1", RenameTable, "", Do},
		{"test_1_a", "abc1", NullEvent, "drop procedure abc", Ignore},
		{"test_1_a", "abc1", NullEvent, "create procedure abc", Ignore},
		{"test_1_a", "abc1", NullEvent, "create function abc", Do},
		{"xxx_1", "abc_1", NullEvent, "create function abc", Do},
		{"xxx_1", "abc_1", InsertEvent, "", Ignore},
		{"xxx_1", "abc_1", CreateIndex, "", Do},
		{"yyy_1", "abc_1", InsertEvent, "", Do},
		{"yyy_1", "abc_1", CreateIndex, "", Ignore},
		{"test_1_a", "abc1", DropView, "", Ignore},
		{"cdc", "t1", RebaseAutoID, "", Ignore},
	}

	// initial binlog event filter
	filter, err := NewBinlogEvent(false, rules)
	require.NoError(t, err)

	// insert duplicate rules: AddRule itself (unlike NewBinlogEvent) never
	// composes same-key Ignore rules, so any key collision is an error.
	for _, rule := range rules {
		err = filter.AddRule(rule)
		require.Error(t, err)
	}
	for _, cs := range cases {
		action, err := filter.Filter(cs.schema, cs.table, cs.event, cs.sql)
		require.NoError(t, err)
		require.Equal(t, cs.action, action)
	}

	// update rules
	rules[0].Events = []EventType{}
	rules[1].Action = Do
	rules[2].Events = []EventType{"ALL DDL"}
	rules = rules[:3]
	for _, rule := range rules {
		err = filter.UpdateRule(rule)
		require.NoError(t, err)
	}

	cases[0].action = Do      // delete
	cases[1].action = Do      // insert
	cases[3].action = Do      // create index
	cases[9].action = Do      // match all event and insert
	cases[10].action = Ignore // match none event and create index
	cases[11].action = Ignore // no match
	cases[12].action = Do     // match all ddl
	cases[13].action = Do     // match all ddl
	for _, cs := range cases {
		action, err := filter.Filter(cs.schema, cs.table, cs.event, cs.sql)
		require.NoError(t, err)
		require.Equal(t, cs.action, action)
	}

	// test multiple rules
	rule := &BinlogEventRule{"test_*", "ab*", []EventType{InsertEvent, AllDDL}, []string{"^DROP\\s+PROCEDURE"}, nil, Do}
	err = filter.AddRule(rule)
	require.NoError(t, err)
	cases[0].action = Ignore // delete
	cases[2].action = Ignore // update
	cases[4].action = Do     // rename table
	cases[7].action = Ignore // create function
	for _, cs := range cases {
		action, err := filter.Filter(cs.schema, cs.table, cs.event, cs.sql)
		require.NoError(t, err)
		require.Equal(t, cs.action, action)
	}

	// remove rule
	err = filter.RemoveRule(rules[0])
	require.NoError(t, err)
	// remove not existing rule
	err = filter.RemoveRule(rules[0])
	require.Error(t, err)
	cases[3].action = Do // create index
	cases[5].action = Do // drop procedure
	for _, cs := range cases {
		action, err := filter.Filter(cs.schema, cs.table, cs.event, cs.sql)
		require.NoError(t, err)
		require.Equal(t, cs.action, action)
	}

	// mismatched
	action, err := filter.Filter("xxx_a", "", InsertEvent, "")
	require.NoError(t, err)
	require.Equal(t, Do, action)

	// invalid rule
	err = filter.Selector.Insert("test_1_*", "abc*", "error", selector.Insert)
	require.NoError(t, err)
	_, err = filter.Filter("test_1_a", "abc", InsertEvent, "")
	require.Error(t, err)
}

func ignoreAddColumn(schemaPattern, tablePattern, column string) *BinlogEventRule {
	return &BinlogEventRule{
		SchemaPattern: schemaPattern,
		TablePattern:  tablePattern,
		SQLPattern:    []string{"ADD COLUMN " + column},
		Action:        Ignore,
	}
}

func addColumnAction(t *testing.T, filter *BinlogEvent, column string) ActionType {
	t.Helper()
	action, err := filter.Filter("db", "t", NullEvent, "alter table t add column "+column+" int")
	require.NoError(t, err)
	return action
}

func TestSameKeyIgnoreComposition(t *testing.T) {
	ruleA := ignoreAddColumn("*", "", "a_col")
	ruleB := ignoreAddColumn("*", "", "b_col")
	ruleC := ignoreAddColumn("*", "", "c_col")

	check := func(filter *BinlogEvent) {
		require.Equal(t, Ignore, addColumnAction(t, filter, "a_col"))
		require.Equal(t, Ignore, addColumnAction(t, filter, "b_col"))
		require.Equal(t, Ignore, addColumnAction(t, filter, "c_col"))
		// Distinguishes composing each rule's own condition from treating
		// the whole key as Ignore.
		require.Equal(t, Do, addColumnAction(t, filter, "d_col"))
	}

	filterABC, err := NewBinlogEvent(false, []*BinlogEventRule{ruleA, ruleB, ruleC})
	require.NoError(t, err)
	check(filterABC)

	filterCBA, err := NewBinlogEvent(false, []*BinlogEventRule{ruleC, ruleB, ruleA})
	require.NoError(t, err)
	check(filterCBA)
}

func TestValidateRulesMixedAction(t *testing.T) {
	ignoreRule := ignoreAddColumn("*", "", "a_col")
	doRule := &BinlogEventRule{SchemaPattern: "*", SQLPattern: []string{"ADD COLUMN b_col"}, Action: Do}
	errorRule := &BinlogEventRule{SchemaPattern: "*", SQLPattern: []string{"ADD COLUMN c_col"}, Action: Error}

	require.ErrorContains(t, ValidateRules(false, []*BinlogEventRule{ignoreRule, doRule}),
		"only supported when all of them are Ignore rules")
	require.ErrorContains(t, ValidateRules(false, []*BinlogEventRule{ignoreRule, errorRule}),
		"only supported when all of them are Ignore rules")
	require.NoError(t, ValidateRules(false, []*BinlogEventRule{ignoreRule, ignoreAddColumn("*", "", "d_col")}))
}

func TestValidateRulesUnusable(t *testing.T) {
	usable := ignoreAddColumn("*", "", "a_col")
	unusable := &BinlogEventRule{SchemaPattern: "db9", Events: []EventType{"truncate"}, Action: Ignore}

	require.NoError(t, ValidateRules(false, []*BinlogEventRule{usable, unusable}))
}

func TestValidateRulesAliasedKey(t *testing.T) {
	// "[ab]" and "[ba]" name one trie key but read back as only the first
	// spelling, so the rules at that key cannot be listed, whatever their
	// actions. The error has to say that rather than blame the actions.
	ignoreRule := ignoreAddColumn("[ab]", "", "a_col")
	alsoIgnore := ignoreAddColumn("[ba]", "", "b_col")
	doRule := &BinlogEventRule{SchemaPattern: "[ba]", SQLPattern: []string{"ADD COLUMN b_col"}, Action: Do}

	const sameWay = "write the pattern the same way in both"
	require.ErrorContains(t, ValidateRules(false, []*BinlogEventRule{ignoreRule, alsoIgnore}), sameWay)
	require.ErrorContains(t, ValidateRules(false, []*BinlogEventRule{ignoreRule, doRule}), sameWay)
	require.ErrorContains(t, ValidateRules(false, []*BinlogEventRule{doRule, ignoreRule}), sameWay)
}

func TestLoadKeepsFirstRule(t *testing.T) {
	// Such a configuration may already be persisted, so loading it must keep
	// working; rejecting it belongs to ValidateRules.
	ignoreRule := ignoreAddColumn("*", "", "a_col")
	doRule := &BinlogEventRule{SchemaPattern: "*", SQLPattern: []string{"ADD COLUMN b_col"}, Action: Do}

	filter, err := NewBinlogEvent(false, []*BinlogEventRule{ignoreRule, doRule})
	require.NoError(t, err)
	require.Equal(t, Ignore, addColumnAction(t, filter, "a_col"))
	require.Equal(t, Do, addColumnAction(t, filter, "b_col"))
}

func TestNilRuleSkipped(t *testing.T) {
	filter, err := NewBinlogEvent(false, []*BinlogEventRule{ignoreAddColumn("*", "", "a_col"), nil})
	require.NoError(t, err)
	require.Equal(t, Ignore, addColumnAction(t, filter, "a_col"))
}

func TestUnusableRuleSkipped(t *testing.T) {
	unusable := &BinlogEventRule{SchemaPattern: "*", Events: []EventType{"wrong event"}, Action: Ignore}
	filter, err := NewBinlogEvent(false, []*BinlogEventRule{unusable, ignoreAddColumn("*", "", "a_col")})
	require.NoError(t, err)
	require.Equal(t, Ignore, addColumnAction(t, filter, "a_col"))
}

func TestCaseSensitiveComposition(t *testing.T) {
	ruleUpper := ignoreAddColumn("DB", "", "a_col")
	ruleLower := ignoreAddColumn("db", "", "b_col")

	filter, err := NewBinlogEvent(false, []*BinlogEventRule{ruleUpper, ruleLower})
	require.NoError(t, err)
	require.Equal(t, Ignore, addColumnAction(t, filter, "a_col"))
	require.Equal(t, Ignore, addColumnAction(t, filter, "b_col"))
}

func TestAddRuleNoCompose(t *testing.T) {
	filter, err := NewBinlogEvent(false, []*BinlogEventRule{ignoreAddColumn("*", "", "a_col")})
	require.NoError(t, err)

	require.Error(t, filter.AddRule(ignoreAddColumn("*", "", "s_col")))
	require.Equal(t, Do, addColumnAction(t, filter, "s_col"))

	sourceDoRule := &BinlogEventRule{SchemaPattern: "*", SQLPattern: []string{"ADD COLUMN s2_col"}, Action: Do}
	require.Error(t, filter.AddRule(sourceDoRule))
}

func TestMergeLowerPriorityRules(t *testing.T) {
	t.Run("composes within the merged set", func(t *testing.T) {
		filter, err := NewBinlogEvent(false, nil)
		require.NoError(t, err)

		lowerA := ignoreAddColumn("*", "", "a_col")
		lowerB := ignoreAddColumn("*", "", "b_col")
		added, overwritten, err := filter.MergeLowerPriorityRules([]*BinlogEventRule{lowerA, lowerB})
		require.NoError(t, err)
		require.Equal(t, []*BinlogEventRule{lowerA, lowerB}, added)
		require.Empty(t, overwritten)
		require.Equal(t, Ignore, addColumnAction(t, filter, "a_col"))
		require.Equal(t, Ignore, addColumnAction(t, filter, "b_col"))
		require.Equal(t, Do, addColumnAction(t, filter, "c_col"))
	})

	t.Run("existing key wins over the whole group", func(t *testing.T) {
		filter, err := NewBinlogEvent(false, []*BinlogEventRule{ignoreAddColumn("*", "", "a_col")})
		require.NoError(t, err)

		lowerA := ignoreAddColumn("*", "", "s1_col")
		lowerB := ignoreAddColumn("*", "", "s2_col")
		added, overwritten, err := filter.MergeLowerPriorityRules([]*BinlogEventRule{lowerA, lowerB})
		require.NoError(t, err)
		require.Empty(t, added)
		require.Equal(t, []*BinlogEventRule{lowerA, lowerB}, overwritten)
		require.Equal(t, Do, addColumnAction(t, filter, "s1_col"))
		require.Equal(t, Do, addColumnAction(t, filter, "s2_col"))
	})

	t.Run("existing key wins when spelled differently", func(t *testing.T) {
		filter, err := NewBinlogEvent(false, []*BinlogEventRule{ignoreAddColumn("[ab]", "", "a_col")})
		require.NoError(t, err)

		lower := ignoreAddColumn("[ba]", "", "s_col")
		added, overwritten, err := filter.MergeLowerPriorityRules([]*BinlogEventRule{lower})
		require.NoError(t, err)
		require.Empty(t, added)
		require.Equal(t, []*BinlogEventRule{lower}, overwritten)
	})

	t.Run("keys are exact", func(t *testing.T) {
		filter, err := NewBinlogEvent(false, []*BinlogEventRule{ignoreAddColumn("*", "", "a_col")})
		require.NoError(t, err)

		lower := ignoreAddColumn("*", "*", "s_col")
		added, _, err := filter.MergeLowerPriorityRules([]*BinlogEventRule{lower})
		require.NoError(t, err)
		require.Equal(t, []*BinlogEventRule{lower}, added)
		require.Equal(t, Ignore, addColumnAction(t, filter, "s_col"))
	})

	t.Run("added keeps the input order", func(t *testing.T) {
		lower := []*BinlogEventRule{
			ignoreAddColumn("db3", "", "c_col"),
			ignoreAddColumn("db1", "", "a_col"),
			ignoreAddColumn("db2", "t", "b_col"),
			ignoreAddColumn("db1", "", "d_col"),
		}
		for range 20 {
			filter, err := NewBinlogEvent(false, nil)
			require.NoError(t, err)
			added, _, err := filter.MergeLowerPriorityRules(lower)
			require.NoError(t, err)
			require.Equal(t, lower, added)
		}
	})

	t.Run("a group it cannot compose keeps the first rule", func(t *testing.T) {
		filter, err := NewBinlogEvent(false, nil)
		require.NoError(t, err)

		lower := ignoreAddColumn("*", "", "a_col")
		doRule := &BinlogEventRule{SchemaPattern: "*", SQLPattern: []string{"ADD COLUMN b_col"}, Action: Do}
		added, _, err := filter.MergeLowerPriorityRules([]*BinlogEventRule{lower, doRule})
		require.NoError(t, err)
		require.Equal(t, []*BinlogEventRule{lower}, added)
		require.Equal(t, Ignore, addColumnAction(t, filter, "a_col"))
		require.Equal(t, Do, addColumnAction(t, filter, "b_col"))
	})

	t.Run("a rule it cannot use is left out", func(t *testing.T) {
		filter, err := NewBinlogEvent(false, nil)
		require.NoError(t, err)

		usable := ignoreAddColumn("db9", "", "a_col")
		unusable := &BinlogEventRule{SchemaPattern: "*", Events: []EventType{"wrong event"}, Action: Ignore}
		added, overwritten, err := filter.MergeLowerPriorityRules([]*BinlogEventRule{usable, unusable})
		require.NoError(t, err)
		require.Equal(t, []*BinlogEventRule{usable}, added)
		require.Empty(t, overwritten)
	})
}

func TestCaseSensitive(t *testing.T) {
	// we test case insensitive in TestFilter
	rules := []*BinlogEventRule{
		{"Test_1_*", "abc*", []EventType{DeleteEvent, InsertEvent, CreateIndex, DropIndex}, []string{"^DROP\\s+PROCEDURE", "^CREATE\\s+PROCEDURE"}, nil, Ignore},
		{"xxx_*", "abc_*", []EventType{AllDML, NoneDDL}, nil, nil, Ignore},
	}

	cases := []struct {
		schema, table string
		event         EventType
		sql           string
		action        ActionType
	}{
		{"test_1_a", "abc1", DeleteEvent, "", Do},
		{"test_1_a", "abc1", InsertEvent, "", Do},
		{"test_1_a", "abc1", UpdateEvent, "", Do},
		{"test_1_a", "abc1", CreateIndex, "", Do},
		{"test_1_a", "abc1", RenameTable, "", Do},
		{"test_1_a", "abc1", NullEvent, "drop procedure abc", Do},
		{"test_1_a", "abc1", NullEvent, "create procedure abc", Do},
		{"test_1_a", "abc1", NullEvent, "create function abc", Do},
		{"xxx_1", "abc_1", NullEvent, "create function abc", Do},
		{"xxx_1", "abc_1", InsertEvent, "", Ignore},
		{"xxx_1", "abc_1", CreateIndex, "", Do},
	}

	// initial binlog event filter
	filter, err := NewBinlogEvent(true, rules)
	require.NoError(t, err)

	// insert duplicate rules
	for _, rule := range rules {
		err = filter.AddRule(rule)
		require.Error(t, err)
	}
	for _, cs := range cases {
		action, err := filter.Filter(cs.schema, cs.table, cs.event, cs.sql)
		require.NoError(t, err)
		require.Equal(t, cs.action, action)
	}
}

func TestGlobalFilter(t *testing.T) {
	schemaRule := &BinlogEventRule{
		SchemaPattern: "*",
		SQLPattern:    []string{"^FLUSH"},
		Action:        Ignore,
	}
	tableRule := &BinlogEventRule{
		SchemaPattern: "*",
		TablePattern:  "*",
		SQLPattern:    []string{"^FLUSH"},
		Action:        Ignore,
	}

	cases := []struct {
		schema string
		table  string
		sql    string
		action ActionType
	}{
		{
			schema: "db",
			table:  "tbl",
			sql:    "FLUSH ENGINE LOGS",
			action: Ignore,
		},
		{
			schema: "db",
			table:  "",
			sql:    "FLUSH ENGINE LOGS",
			action: Ignore,
		},
		{
			schema: "",
			table:  "tbl",
			sql:    "FLUSH ENGINE LOGS",
			action: Ignore,
		},
		{
			schema: "",
			table:  "",
			sql:    "FLUSH ENGINE LOGS",
			action: Ignore,
		},
	}

	// initial binlog event filter with schema rule
	filter, err := NewBinlogEvent(false, []*BinlogEventRule{schemaRule})
	require.NoError(t, err)

	for _, cs := range cases {
		action, err := filter.Filter(cs.schema, cs.table, NullEvent, cs.sql)
		require.NoError(t, err)
		require.Equal(t, cs.action, action)
	}

	// remove schema rule
	err = filter.RemoveRule(schemaRule)
	require.NoError(t, err)

	// add table rule
	err = filter.AddRule(tableRule)
	require.NoError(t, err)

	for _, cs := range cases {
		action, err := filter.Filter(cs.schema, cs.table, NullEvent, cs.sql)
		require.NoError(t, err)
		require.Equal(t, cs.action, action)
	}
}

func TestToEventType(t *testing.T) {
	cases := []struct {
		eventStr string
		event    EventType
		err      error
	}{
		{"", NullEvent, nil},
		{"insert", InsertEvent, nil},
		{"Insert", InsertEvent, nil},
		{"update", UpdateEvent, nil},
		{"UPDATE", UpdateEvent, nil},
		{"delete", DeleteEvent, nil},
		{"create", NullEvent, errors.NotValidf("event type %s", "create")},
		{"create schema", CreateDatabase, nil},
		{"create SCHEMA", CreateDatabase, nil},
		{"create database", CreateDatabase, nil},
		{"drop schema", DropDatabase, nil},
		{"drop Schema", DropDatabase, nil},
		{"drop database", DropDatabase, nil},
		{"alter database", AlterDatabase, nil},
		{"alter schema", AlterDatabase, nil},
		{"create index", CreateIndex, nil},
		{"add table partition", AddTablePartition, nil},
		{"drop taBle partition", DropTablePartition, nil},
		{"truncate tablE parTition", TruncateTablePartition, nil},
		{"rebase auto id", RebaseAutoID, nil},
		{"xxx", NullEvent, errors.NotValidf("event type %s", "xxx")},
		{"I don't know", NullEvent, errors.NotValidf("event type %s", "I don't know")},
	}

	for _, cs := range cases {
		event, err := toEventType(cs.eventStr)
		require.Equal(t, cs.event, event)
		if err != nil {
			require.ErrorContains(t, err, cs.err.Error())
		} else {
			require.NoError(t, err)
		}
	}
}

func TestClassifyEvent(t *testing.T) {
	cases := []struct {
		event    EventType
		evenType EventType
		err      error
	}{
		{NullEvent, NullEvent, nil},
		// dml
		{InsertEvent, dml, nil},
		{UpdateEvent, dml, nil},
		{DeleteEvent, dml, nil},
		// ddl
		{CreateDatabase, ddl, nil},
		{CreateSchema, ddl, nil},
		{DropDatabase, incompatibleDDL, nil},
		{DropSchema, incompatibleDDL, nil},
		{AlterSchema, ddl, nil},
		{CreateTable, ddl, nil},
		{DropTable, incompatibleDDL, nil},
		{TruncateTable, incompatibleDDL, nil},
		{RenameTable, incompatibleDDL, nil},
		{CreateIndex, ddl, nil},
		{DropIndex, incompatibleDDL, nil},
		{CreateView, ddl, nil},
		{DropView, ddl, nil},
		{AlterTable, ddl, nil},
		{AddTablePartition, ddl, nil},
		{DropTablePartition, incompatibleDDL, nil},
		{RebaseAutoID, incompatibleDDL, nil},
		{TruncateTablePartition, incompatibleDDL, nil},
		{"create", NullEvent, errors.NotValidf("event type %s", "create")},
		{EventType("xxx"), NullEvent, errors.NotValidf("event type %s", "xxx")},
		{EventType("I don't know"), NullEvent, errors.NotValidf("event type %s", "I don't know")},
	}

	for _, cs := range cases {
		et, err := ClassifyEvent(cs.event)
		require.Equal(t, cs.evenType, et)
		if err != nil {
			require.ErrorContains(t, err, cs.err.Error())
		} else {
			require.NoError(t, err)
		}
	}
}
