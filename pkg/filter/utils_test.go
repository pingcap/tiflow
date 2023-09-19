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
	t.Parallel()
	require.Equal(t, supportedEventTypes, SupportedEventTypes())
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
		jobType   timodel.ActionType
		eventType bf.EventType
		err       error
	}{
		{"CREATE DATABASE test", timodel.ActionCreateSchema, bf.CreateDatabase, nil},
		{"DROP DATABASE test", timodel.ActionDropSchema, bf.DropDatabase, nil},
		{"CREATE TABLE test.t1(id int primary key)", timodel.ActionCreateTable, bf.CreateTable, nil},
		{"DROP TABLE test.t1", timodel.ActionDropTable, bf.DropTable, nil},
		{"TRUNCATE TABLE test.t1", timodel.ActionTruncateTable, bf.TruncateTable, nil},
		{"rename table s1.t1 to s2.t2", timodel.ActionRenameTable, bf.RenameTable, nil},
		{"rename table s1.t1 to s2.t2, test.t1 to test.t2", timodel.ActionRenameTables, bf.RenameTable, nil},
		{"create index i1 on test.t1 (age)", timodel.ActionAddIndex, bf.AlterTable, nil},
		{"drop index i1 on test.t1", timodel.ActionDropIndex, bf.AlterTable, nil},
		{"CREATE VIEW test.v AS SELECT * FROM t", timodel.ActionCreateView, bf.CreateView, nil},
		{"DROP view if exists test.v", timodel.ActionDropView, bf.DropView, nil},

		{"alter table test.t1 add column name varchar(50)", timodel.ActionAddColumn, bf.AlterTable, nil},
		{"alter table test.t1 drop column name", timodel.ActionDropColumn, bf.AlterTable, nil},
		{"alter table test.t1 modify column name varchar(100)", timodel.ActionModifyColumn, bf.AlterTable, nil},
		{"ALTER TABLE test.t1 CONVERT TO CHARACTER SET gbk", timodel.ActionModifyTableCharsetAndCollate, bf.AlterTable, nil},
		{"alter table test add primary key(b)", timodel.ActionAddIndex, bf.AlterTable, nil},
		{"ALTER DATABASE dbname CHARACTER SET utf8 COLLATE utf8_general_ci;", timodel.ActionModifySchemaCharsetAndCollate, bf.AlterDatabase, nil},
		{"Alter table test.t1 drop partition t11", timodel.ActionDropTablePartition, bf.DropTablePartition, nil},
		{"Alter table test.t1 add partition (partition p3 values less than (2002))", timodel.ActionDropTablePartition, bf.DropTablePartition, nil},
		{"Alter table test.t1 truncate partition t11", timodel.ActionDropTablePartition, bf.DropTablePartition, nil},
		{"Alter table test.t1 reorganize partition p11 into (partition p1 values less than (10), partition p2 values less than (20))", timodel.ActionReorganizePartition, bf.AlterTable, nil},
		{"alter table add i", timodel.ActionAddIndex, bf.NullEvent, cerror.ErrConvertDDLToEventTypeFailed},
	}
	p := parser.New()
	for _, c := range cases {
		et, err := ddlToEventType(p, c.ddl, c.jobType)
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

func TestDDLToTypeSpecialDDL(t *testing.T) {
	type c struct {
		ddl      string
		jobType  timodel.ActionType
		evenType bf.EventType
		err      error
	}

	ddlWithTab := `CREATE TABLE if not exists sbtest25 
	(
		id bigint NOT NULL,
		k bigint NOT NULL DEFAULT '0',
		c char(30) NOT NULL DEFAULT '',
		pad char(20) NOT NULL DEFAULT '',
		PRIMARY KEY (id),
	    KEY k_1 (k)
	) 	ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin`
	ddlWithTwoTab := `		CREATE TABLE if not exists sbtest25 
	(
		id bigint NOT NULL,
		k bigint NOT NULL DEFAULT '0',
		c char(30) NOT NULL DEFAULT '',
		pad char(20) NOT NULL DEFAULT '',
		PRIMARY KEY (id),
		KEY k_1 (k)
		)
		ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin`
	ddlWithNewLine := `CREATE TABLE finish_mark 
	(
		
		id INT AUTO_INCREMENT PRIMARY KEY,
		val INT DEFAULT 0,                     
		col0 INT NOT NULL)`

	cases := []c{
		{"CREATE DATABASE test", timodel.ActionCreateSchema, bf.CreateDatabase, nil},
		{ddlWithTwoTab, timodel.ActionCreateTable, bf.CreateTable, nil},
		{ddlWithTab, timodel.ActionCreateTable, bf.CreateTable, nil},
		{ddlWithNewLine, timodel.ActionCreateTable, bf.CreateTable, nil},
	}
	p := parser.New()
	for _, c := range cases {
		et, err := ddlToEventType(p, c.ddl, c.jobType)
		if c.err != nil {
			errRFC, ok := cerror.RFCCode(err)
			require.True(t, ok)
			caseErrRFC, ok := cerror.RFCCode(c.err)
			require.True(t, ok)
			require.Equal(t, caseErrRFC, errRFC)
		} else {
			require.NoError(t, err)
		}
		require.Equal(t, c.evenType, et, "case%v", c.ddl)
	}
}
