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

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestFormatQuery(t *testing.T) {
	sql := "CREATE TABLE `test` (`id` INT PRIMARY KEY,`data` VECTOR(5));"
	expectSql := "CREATE TABLE `test` (`id` INT PRIMARY KEY,`data` LONGTEXT)"
	p := parser.New()
	stmt, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		log.Error("format query parse one stmt failed", zap.Error(err))
	}
	stmt.Accept(&visiter{})

	buf := new(bytes.Buffer)
	restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, buf)
	if err = stmt.Restore(restoreCtx); err != nil {
		log.Error("format query restore failed", zap.Error(err))
	}
	require.Equal(t, buf.String(), expectSql)
}
