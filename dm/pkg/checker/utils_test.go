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

package checker

import (
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/stretchr/testify/require"
)

func TestVersionComparison(t *testing.T) {
	// test normal cases
	cases := []struct {
		rawVersion     string
		ge, gt, lt, le bool
	}{
		{"0.0.0", false, false, true, true},
		{"5.5.0", false, false, true, true},
		{"5.6.0", true, false, true, true},
		{"5.7.0", true, true, true, true},
		{"5.8.0", true, true, true, true}, // although it does not exist
		{"8.0.1", true, true, false, false},
		{"255.255.255", true, true, false, false}, // max version
	}

	var (
		version MySQLVersion
		err     error
	)
	for _, cs := range cases {
		version, err = toMySQLVersion(cs.rawVersion)
		require.NoError(t, err)
		require.Equal(t, cs.ge, version.Ge(SupportedVersion["mysql"].Min))
		require.Equal(t, cs.gt, version.Gt(SupportedVersion["mysql"].Min))
		require.Equal(t, cs.lt, version.Lt(SupportedVersion["mysql"].Max))
		require.Equal(t, cs.le, version.Le(SupportedVersion["mysql"].Max))
	}
}

func TestToVersion(t *testing.T) {
	// test normal cases
	cases := []struct {
		rawVersion      string
		expectedVersion MySQLVersion
		hasError        bool
	}{
		{"", MinVersion, true},
		{"1.2.3.4", MinVersion, true},
		{"1.x.3", MySQLVersion{1, 0, 0}, true},
		{"5.7.18-log", MySQLVersion{5, 7, 18}, false},
		{"5.5.50-MariaDB-1~wheezy", MySQLVersion{5, 5, 50}, false},
		{"5.7.19-17-log", MySQLVersion{5, 7, 19}, false},
		{"5.7.18-log", MySQLVersion{5, 7, 18}, false},
		{"5.7.16-log", MySQLVersion{5, 7, 16}, false},
	}

	for _, cs := range cases {
		version, err := toMySQLVersion(cs.rawVersion)
		require.Equal(t, cs.expectedVersion, version)
		if cs.hasError {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
		}
	}
}

func TestGetColumnsAndIgnorable(t *testing.T) {
	cases := []struct {
		sql      string
		expected map[string]bool
	}{
		{
			"CREATE TABLE `t` (\n" +
				"  `c` int(11) NOT NULL,\n" +
				"  `c2` int(11) NOT NULL,\n" +
				"  `c3` int(11) DEFAULT '1',\n" +
				"  `c4` int(11) NOT NULL DEFAULT '2',\n" +
				"  PRIMARY KEY (`c`) /*T![clustered_index] CLUSTERED */\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
			map[string]bool{"c": false, "c2": false, "c3": true, "c4": true},
		},
		{
			" CREATE TABLE `t2` (\n" +
				"  `c` int(11) NOT NULL AUTO_INCREMENT,\n" +
				"  `c2` int(11) DEFAULT NULL,\n" +
				"  `c3` int(11) GENERATED ALWAYS AS (`c2` + 1) VIRTUAL,\n" +
				"  PRIMARY KEY (`c`) /*T![clustered_index] CLUSTERED */\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
			map[string]bool{"c": true, "c2": true, "c3": true},
		},
		// TODO: in this case, c2 should NOT be ignored when inserting
		{
			"CREATE TABLE `t3` (\n" +
				"  `c` bigint(20) NOT NULL /*T![auto_rand] AUTO_RANDOM(5) */,\n" +
				"  `c2` int(11) DEFAULT NULL,\n" +
				"  `c3` int(11) GENERATED ALWAYS AS (`c2` + 1) VIRTUAL NOT NULL,\n" +
				"  `c4` int(11) DEFAULT NULL,\n" +
				"  PRIMARY KEY (`c`) /*T![clustered_index] CLUSTERED */\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![auto_rand_base] AUTO_RANDOM_BASE=30001 */",
			map[string]bool{"c": true, "c2": true, "c3": true, "c4": true},
		},
	}

	p := parser.New()
	for _, c := range cases {
		stmt, err := p.ParseOneStmt(c.sql, "", "")
		require.NoError(t, err)
		columns := getColumnsAndIgnorable(stmt.(*ast.CreateTableStmt))
		require.Equal(t, c.expected, columns)
	}
}

func TestMarkCheckError(t *testing.T) {
	res := &Result{}
	markCheckError(res, terror.ErrNoMasterStatus.Generate())
	require.Equal(t, terror.ErrNoMasterStatus.Workaround(), res.Instruction)
	res = &Result{}
	markCheckError(res, errors.New("other err"))
	require.Zero(t, len(res.Instruction))
}
