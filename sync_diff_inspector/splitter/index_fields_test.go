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

package splitter

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/util/dbutil/dbutiltest"
	"github.com/stretchr/testify/require"
)

func TestIndexFieldsSimple(t *testing.T) {
	t.Parallel()

	createTableSQL1 := "CREATE TABLE `sbtest1` " +
		"(`id` int(11) NOT NULL AUTO_INCREMENT, " +
		" `k` int(11) NOT NULL DEFAULT '0', " +
		"`c` char(120) NOT NULL DEFAULT '', " +
		"PRIMARY KEY (`id`), KEY `k_1` (`k`))"

	tableInfo, err := dbutiltest.GetTableInfoBySQL(createTableSQL1, parser.New())
	require.NoError(t, err)

	fields, err := indexFieldsFromConfigString("k", tableInfo)
	require.NoError(t, err)
	require.False(t, fields.IsEmpty())
	require.Len(t, fields.Cols(), 1)

	for _, index := range tableInfo.Indices {
		switch index.Name.String() {
		case "PRIMARY":
			require.False(t, fields.MatchesIndex(index))
		case "k_1":
			require.True(t, fields.MatchesIndex(index))
		default:
			require.FailNow(t, "unreachable")
		}
	}
}

func TestIndexFieldsComposite(t *testing.T) {
	t.Parallel()

	createTableSQL1 := "CREATE TABLE `sbtest1` " +
		"(`id` int(11) NOT NULL AUTO_INCREMENT, " +
		" `k` int(11) NOT NULL DEFAULT '0', " +
		"`c` char(120) NOT NULL DEFAULT '', " +
		"PRIMARY KEY (`id`, `k`)," +
		"KEY `k_1` (`k`)," +
		"UNIQUE INDEX `c_1` (`c`))"

	tableInfo, err := dbutiltest.GetTableInfoBySQL(createTableSQL1, parser.New())
	require.NoError(t, err)

	fields, err := indexFieldsFromConfigString("id, k", tableInfo)
	require.NoError(t, err)
	require.False(t, fields.IsEmpty())
	require.Len(t, fields.Cols(), 2)

	for _, index := range tableInfo.Indices {
		switch index.Name.String() {
		case "PRIMARY":
			require.True(t, fields.MatchesIndex(index))
		case "k_1":
			require.False(t, fields.MatchesIndex(index))
		case "c_1":
			require.False(t, fields.MatchesIndex(index))
		default:
			require.FailNow(t, "unreachable")
		}
	}
}

func TestIndexFieldsEmpty(t *testing.T) {
	t.Parallel()

	createTableSQL1 := "CREATE TABLE `sbtest1` " +
		"(`id` int(11) NOT NULL AUTO_INCREMENT, " +
		" `k` int(11) NOT NULL DEFAULT '0', " +
		"`c` char(120) NOT NULL DEFAULT '', " +
		"PRIMARY KEY (`id`), KEY `k_1` (`k`))"

	tableInfo, err := dbutiltest.GetTableInfoBySQL(createTableSQL1, parser.New())
	require.NoError(t, err)

	fields, err := indexFieldsFromConfigString("", tableInfo)
	require.NoError(t, err)
	require.True(t, fields.IsEmpty())

	for _, index := range tableInfo.Indices {
		// Expected to match all.
		require.True(t, fields.MatchesIndex(index))
	}
}
