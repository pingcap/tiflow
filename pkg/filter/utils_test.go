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

package filter

import (
	"strings"
	"testing"

	bf "github.com/pingcap/tidb-tools/pkg/binlog-filter"
	timodel "github.com/pingcap/tidb/parser/model"
	tifilter "github.com/pingcap/tidb/util/filter"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestIsSchema(t *testing.T) {
	t.Parallel()
	cases := []struct {
		schema string
		result bool
	}{
		{"", false},
		{"test", false},
		{"SYS", true},
		{"MYSQL", true},
		{tifilter.InformationSchemaName, true},
		{tifilter.InspectionSchemaName, true},
		{tifilter.PerformanceSchemaName, true},
		{tifilter.MetricSchemaName, true},
	}
	for _, c := range cases {
		require.Equal(t, c.result, isSysSchema(c.schema))
	}
}

func TestSupportedEventTypeString(t *testing.T) {
	expected := `Invalid input, 'ignore-event' parameters can only accept 
[all dml, all ddl, insert, update, delete, create schema, create database, 
drop schema, drop database, create table, drop table, add column, drop column, 
add index, create index, drop index, truncate table, modify column, 
rename table, set default value, modify table comment, rename index, 
add table partition, drop table partition, create view, 
modify table charset and collate, truncate table partition, drop view, 
recover table, modify schema charset and collate, add primary key, drop primary key]`
	expected = strings.ReplaceAll(expected, "\n", "")
	require.Equal(t, expected, SupportedEventWarnMessage())
}

func TestVerifyTableRules(t *testing.T) {
	t.Parallel()
	cases := []struct {
		cfg      *config.FilterConfig
		hasError bool
	}{
		{&config.FilterConfig{Rules: []string{""}}, false},
		{&config.FilterConfig{Rules: []string{"*.*"}}, false},
		{&config.FilterConfig{Rules: []string{"test.*ms"}}, false},
		{&config.FilterConfig{Rules: []string{"*.889"}}, false},
		{&config.FilterConfig{Rules: []string{"test-a.*", "*.*.*"}}, true},
		{&config.FilterConfig{Rules: []string{"*.*", "*.*.*", "*.*.*.*"}}, true},
	}
	for _, c := range cases {
		_, err := VerifyTableRules(c.cfg)
		require.Equal(t, c.hasError, err != nil, "case: %s", c.cfg.Rules)
	}
}

func TestJobTypeToEventType(t *testing.T) {
	t.Parallel()
	cases := []struct {
		jobType   timodel.ActionType
		eventType bf.EventType
	}{
		{timodel.ActionCreateSchema, bf.CreateDatabase},
		{timodel.ActionDropSchema, bf.DropDatabase},
		{timodel.ActionCreateTable, bf.CreateTable},
		{timodel.ActionDropTable, bf.DropTable},
		{timodel.ActionTruncateTable, bf.TruncateTable},
		{timodel.ActionRenameTable, bf.RenameTable},
		{timodel.ActionRenameTables, bf.RenameTable},
		{timodel.ActionAddIndex, bf.CreateIndex},
		{timodel.ActionDropIndex, bf.DropIndex},
		{timodel.ActionCreateView, bf.CreateView},
		{timodel.ActionDropView, bf.DropView},
		{timodel.ActionAddColumn, bf.AddColumn},
		{timodel.ActionAddColumns, bf.AddColumn},
		{timodel.ActionDropColumn, bf.DropColumn},
		{timodel.ActionDropColumns, bf.DropColumn},
		{timodel.ActionModifyColumn, bf.ModifyColumn},
		{timodel.ActionSetDefaultValue, bf.SetDefaultValue},
		{timodel.ActionModifyTableComment, bf.ModifyTableComment},
		{timodel.ActionRenameIndex, bf.RenameIndex},
		{timodel.ActionAddTablePartition, bf.AddTablePartition},
		{timodel.ActionDropTablePartition, bf.DropTablePartition},
		{timodel.ActionTruncateTablePartition, bf.TruncateTablePartition},
		{timodel.ActionModifyTableCharsetAndCollate, bf.ModifyTableCharsetAndCollate},
		{timodel.ActionModifySchemaCharsetAndCollate, bf.ModifySchemaCharsetAndCollate},
		{timodel.ActionRecoverTable, bf.RecoverTable},
		{timodel.ActionAddPrimaryKey, bf.AddPrimaryKey},
		{timodel.ActionDropPrimaryKey, bf.DropPrimaryKey},
		{timodel.ActionAddForeignKey, bf.NullEvent},
		{timodel.ActionDropForeignKey, bf.NullEvent},
		{timodel.ActionAlterCheckConstraint, bf.NullEvent},
	}

	for _, c := range cases {
		require.Equal(t, c.eventType, jobTypeToEventType(c.jobType))
	}
}

func TestShouldDiscardByBuiltInDDLAllowlist(t *testing.T) {
	t.Parallel()
	cases := []struct {
		jobType timodel.ActionType
		discard bool
	}{
		{timodel.ActionCreateSchema, false},
		{timodel.ActionDropSchema, false},
		{timodel.ActionCreateTable, false},
		{timodel.ActionDropTable, false},
		{timodel.ActionAddColumn, false},
		{timodel.ActionDropColumn, false},
		{timodel.ActionAddIndex, false},
		{timodel.ActionDropIndex, false},
		{timodel.ActionTruncateTable, false},
		{timodel.ActionModifyColumn, false},
		{timodel.ActionRenameTable, false},
		{timodel.ActionRenameTables, false},
		{timodel.ActionSetDefaultValue, false},
		{timodel.ActionModifyTableComment, false},
		{timodel.ActionRenameIndex, false},
		{timodel.ActionAddTablePartition, false},
		{timodel.ActionDropTablePartition, false},
		{timodel.ActionCreateView, false},
		{timodel.ActionModifyTableCharsetAndCollate, false},
		{timodel.ActionTruncateTablePartition, false},
		{timodel.ActionDropView, false},
		{timodel.ActionRecoverTable, false},
		{timodel.ActionModifySchemaCharsetAndCollate, false},
		{timodel.ActionAddPrimaryKey, false},
		{timodel.ActionDropPrimaryKey, false},
		{timodel.ActionAddColumns, false},
		{timodel.ActionDropColumns, false},
		{timodel.ActionAddForeignKey, true},  // not in built-in DDL allowlist
		{timodel.ActionDropForeignKey, true}, // not in built-in DDL allowlist
	}

	for _, c := range cases {
		require.Equal(t, c.discard, shouldDiscardByBuiltInDDLAllowlist(c.jobType))
	}
}
