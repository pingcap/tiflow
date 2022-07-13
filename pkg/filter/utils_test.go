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
	"github.com/pingcap/tidb/parser"
	timodel "github.com/pingcap/tidb/parser/model"
	tifilter "github.com/pingcap/tidb/util/filter"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
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
drop schema, drop database, create table, drop table, add index, create index, 
drop index, truncate table, rename table, create view, drop view, alter table]`
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

func TestDDLToEventType(t *testing.T) {
	t.Parallel()
	cases := []struct {
		ddl       string
		eventType bf.EventType
		err       error
	}{
		{"CREATE DATABASE test", bf.CreateDatabase, nil},
		{"DROP DATABASE test", bf.DropDatabase, nil},
		{"CREATE TABLE test.t1(id int primary key)", bf.CreateTable, nil},
		{"DROP TABLE test.t1", bf.DropTable, nil},
		{"TRUNCATE TABLE test.t1", bf.TruncateTable, nil},
		{"rename table s1.t1 to s2.t2", bf.RenameTable, nil},
		{"rename table s1.t1 to s2.t2, test.t1 to test.t2", bf.RenameTable, nil},
		{"create index i1 on test.t1 (age)", bf.CreateIndex, nil},
		{"drop index i1 on test.t1", bf.DropIndex, nil},
		{"CREATE VIEW test.v AS SELECT * FROM t", bf.CreateView, nil},
		{"DROP view if exists test.v", bf.DropView, nil},

		{"alter table test.t1 add column name varchar(50)", bf.AlterTable, nil},
		{"alter table test.t1 drop column name", bf.AlterTable, nil},
		{"alter table test.t1 modify column name varchar(100)", bf.AlterTable, nil},
		{"ALTER TABLE test.t1 CONVERT TO CHARACTER SET gbk", bf.AlterTable, nil},
		{"alter table test add primary key(b)", bf.AlterTable, nil},
		{"ALTER DATABASE dbname CHARACTER SET utf8 COLLATE utf8_general_ci;", bf.AlterDatabase, nil},
		{"Alter table test.t1 drop partition t11", bf.AlterTable, nil},
		{"alter table add i", bf.NullEvent, cerror.ErrConvertDDLToEventTypeFailed},
	}
	p := parser.New()
	for _, c := range cases {
		et, err := ddlToEventType(p, c.ddl)
		if c.err != nil {
			errRFC, ok := cerror.RFCCode(err)
			require.True(t, ok)
			caseErrRFC, ok := cerror.RFCCode(c.err)
			require.True(t, ok)
			require.Equal(t, caseErrRFC, errRFC)
		} else {
			require.NoError(t, err)
		}
		require.Equal(t, c.eventType, et, "case%v", c.ddl)
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
