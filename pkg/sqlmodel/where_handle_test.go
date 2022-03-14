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

package sqlmodel

import (
	"testing"

	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/stretchr/testify/require"
)

func TestGenWhereHandle(t *testing.T) {
	t.Parallel()

	// 1. target is same as source

	createSQL := `
CREATE TABLE t (
	c INT, c2 INT NOT NULL, c3 VARCHAR(20) NOT NULL,
	UNIQUE INDEX idx3 (c2, c3)
)`
	p := parser.New()
	node, err := p.ParseOneStmt(createSQL, "", "")
	require.NoError(t, err)
	ti, err := ddl.BuildTableInfoFromAST(node.(*ast.CreateTableStmt))
	require.NoError(t, err)
	require.Len(t, ti.Indices, 1)
	idx := ti.Indices[0]
	rewritten := rewriteColsOffset(idx, ti)
	require.Equal(t, idx, rewritten)

	// check GetWhereHandle when target is same as source
	handle := GetWhereHandle(ti, ti)
	require.Len(t, handle.UniqueNotNullIdx.Columns, 2)
	require.Equal(t, handle.UniqueNotNullIdx.Columns[0].Offset, 1)
	require.Equal(t, handle.UniqueNotNullIdx.Columns[1].Offset, 2)
	require.Len(t, handle.UniqueIdxs, 1)

	// 2. target has more columns, some index doesn't use it

	targetCreateSQL := `
CREATE TABLE t (
	pk INT PRIMARY KEY, c INT, c2 INT NOT NULL, c3 VARCHAR(20) NOT NULL, extra INT,
	UNIQUE INDEX idx2 (c2, c3),
	UNIQUE INDEX idx3 (extra)
)`
	node, err = p.ParseOneStmt(targetCreateSQL, "", "")
	require.NoError(t, err)
	targetTI, err := ddl.BuildTableInfoFromAST(node.(*ast.CreateTableStmt))
	require.NoError(t, err)
	require.Len(t, targetTI.Indices, 2)
	targetIdx := targetTI.Indices[0]
	require.Len(t, targetIdx.Columns, 2)
	require.Equal(t, targetIdx.Columns[0].Offset, 2)
	require.Equal(t, targetIdx.Columns[1].Offset, 3)

	rewritten = rewriteColsOffset(targetIdx, ti)
	require.Len(t, rewritten.Columns, 2)
	require.Equal(t, rewritten.Columns[0].Offset, 1)
	require.Equal(t, rewritten.Columns[1].Offset, 2)

	// target has more columns, some index uses it
	targetIdx = targetTI.Indices[1]
	require.Len(t, targetIdx.Columns, 1)
	require.Equal(t, targetIdx.Columns[0].Offset, 4)

	rewritten = rewriteColsOffset(targetIdx, ti)
	require.Nil(t, rewritten)

	// check GetWhereHandle when target has more columns
	handle = GetWhereHandle(ti, targetTI)
	require.Len(t, handle.UniqueNotNullIdx.Columns, 2)
	require.Equal(t, handle.UniqueNotNullIdx.Columns[0].Offset, 1)
	require.Equal(t, handle.UniqueNotNullIdx.Columns[1].Offset, 2)
	// PRIMARY and idx3 is not usable
	require.Len(t, handle.UniqueIdxs, 1)

	// 3. PKIsHandle case

	targetCreateSQL = `
CREATE TABLE t (
	extra INT, c INT PRIMARY KEY
)`
	node, err = p.ParseOneStmt(targetCreateSQL, "", "")
	require.NoError(t, err)
	targetTI, err = ddl.BuildTableInfoFromAST(node.(*ast.CreateTableStmt))
	require.NoError(t, err)
	// PKIsHandle has no entry in Indices
	require.Len(t, targetTI.Indices, 0)

	handle = GetWhereHandle(ti, targetTI)
	require.Len(t, handle.UniqueNotNullIdx.Columns, 1)
	require.Equal(t, handle.UniqueNotNullIdx.Columns[0].Offset, 0)
	require.Len(t, handle.UniqueIdxs, 1)

	// 4. target has no available index

	targetCreateSQL = `
CREATE TABLE t (
	extra INT PRIMARY KEY
)`
	node, err = p.ParseOneStmt(targetCreateSQL, "", "")
	require.NoError(t, err)
	targetTI, err = ddl.BuildTableInfoFromAST(node.(*ast.CreateTableStmt))
	require.NoError(t, err)
	// PKIsHandle has no entry in Indices
	require.Len(t, targetTI.Indices, 0)

	handle = GetWhereHandle(ti, targetTI)
	require.Nil(t, handle.UniqueNotNullIdx)
	require.Len(t, handle.UniqueIdxs, 0)

	// 5. composite PK, and PK has higher priority

	targetCreateSQL = `
CREATE TABLE t (
	extra INT, c INT NOT NULL, c2 INT NOT NULL, c3 VARCHAR(20) NOT NULL,
	UNIQUE INDEX idx (c, c3),
	PRIMARY KEY (c, c2),
	UNIQUE INDEX idx3 (c2, c3)
)`
	node, err = p.ParseOneStmt(targetCreateSQL, "", "")
	require.NoError(t, err)
	targetTI, err = ddl.BuildTableInfoFromAST(node.(*ast.CreateTableStmt))
	require.NoError(t, err)

	handle = GetWhereHandle(ti, targetTI)
	require.Len(t, handle.UniqueNotNullIdx.Columns, 2)
	require.Equal(t, handle.UniqueNotNullIdx.Columns[0].Offset, 0)
	require.Equal(t, handle.UniqueNotNullIdx.Columns[1].Offset, 1)
	require.Len(t, handle.UniqueIdxs, 3)
}

func TestAllColsNotNull(t *testing.T) {
	t.Parallel()

	createSQL := `
CREATE TABLE t (
	pk VARCHAR(20) PRIMARY KEY,
	c1 INT,
	c2 INT,
	c3 INT NOT NULL,
	c4 INT NOT NULL,
	INDEX idx1 (c1, c2),
	INDEX idx2 (c2, c3),
	INDEX idx3 (c3, c4)
)`
	p := parser.New()
	node, err := p.ParseOneStmt(createSQL, "", "")
	require.NoError(t, err)
	ti, err := ddl.BuildTableInfoFromAST(node.(*ast.CreateTableStmt))
	require.NoError(t, err)
	require.Len(t, ti.Indices, 4)

	pk := ti.Indices[3]
	require.Equal(t, "PRIMARY", pk.Name.O)
	require.True(t, allColsNotNull(pk, ti.Columns))

	idx1 := ti.Indices[0]
	require.Equal(t, "idx1", idx1.Name.O)
	require.False(t, allColsNotNull(idx1, ti.Columns))

	idx2 := ti.Indices[1]
	require.Equal(t, "idx2", idx2.Name.O)
	require.False(t, allColsNotNull(idx2, ti.Columns))

	idx3 := ti.Indices[2]
	require.Equal(t, "idx3", idx3.Name.O)
	require.True(t, allColsNotNull(idx3, ti.Columns))
}

func TestGetWhereIdxByData(t *testing.T) {
	t.Parallel()

	createSQL := `
CREATE TABLE t (
	c1 INT,
	c2 INT,
	c3 INT,
	c4 INT,
	UNIQUE INDEX idx1 (c1, c2),
	UNIQUE INDEX idx2 (c3, c4)
)`
	p := parser.New()
	node, err := p.ParseOneStmt(createSQL, "", "")
	require.NoError(t, err)
	ti, err := ddl.BuildTableInfoFromAST(node.(*ast.CreateTableStmt))
	require.NoError(t, err)

	handle := GetWhereHandle(ti, ti)
	idx := handle.getWhereIdxByData([]interface{}{nil, 2, 3, 4})
	require.Equal(t, "idx2", idx.Name.L)
	require.Equal(t, idx, handle.UniqueIdxs[0])

	// last used index is moved to front
	idx = handle.getWhereIdxByData([]interface{}{1, 2, 3, nil})
	require.Equal(t, "idx1", idx.Name.L)
	require.Equal(t, idx, handle.UniqueIdxs[0])

	// no index available
	idx = handle.getWhereIdxByData([]interface{}{1, nil, 3, nil})
	require.Nil(t, idx)
}
