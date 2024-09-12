// Copyright 2024 PingCAP, Inc.
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

package mysql

import (
	"bytes"
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/stretchr/testify/require"
)

func TestFormatQuery(t *testing.T) {
	sql := "CREATE TABLE `test` (`id` INT PRIMARY KEY,`data` VECTOR(5))"
	expectSQL := "CREATE TABLE `test` (`id` INT PRIMARY KEY,`data` LONGTEXT)"
	p := parser.New()
	stmt, err := p.ParseOneStmt(sql, "", "")
	require.NoError(t, err)
	stmt.Accept(&visiter{})

	buf := new(bytes.Buffer)
	restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, buf)
	err = stmt.Restore(restoreCtx)
	require.NoError(t, err)
	require.Equal(t, buf.String(), expectSQL)
}

func BenchmarkFormatQuery(b *testing.B) {
	sql := "CREATE TABLE `test` (`id` INT PRIMARY KEY,`data` LONGTEXT)"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		formatQuery(sql)
	}
}
