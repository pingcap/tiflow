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
		{"Test_1_*", "abc*", []EventType{"wrong event"}, []string{"^DROP\\s+PROCEDURE", "^CREATE\\s+PROCEDURE"}, nil, Ignore},
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
