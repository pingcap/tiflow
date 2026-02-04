// Copyright 2025 PingCAP, Inc.
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

package mariadb2tidb

import (
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tiflow/dm/pkg/mariadb2tidb/config"
	"github.com/stretchr/testify/require"
)

func TestTransformSQLDefaultRules(t *testing.T) {
	converter := NewConverter(nil)
	input := `CREATE TABLE t (
  id INT DEFAULT uuid(),
  txt TEXT DEFAULT 'x',
  v VARCHAR(800),
  j JSON,
  g JSON GENERATED ALWAYS AS (JSON_EXTRACT(j, '$.a')) VIRTUAL,
  CHECK (json_valid(j)),
  KEY idx_txt (txt),
  KEY idx_v (v)
) DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;`

	output, err := converter.TransformSQL(input)
	require.NoError(t, err)

	stmt := parseCreateTableStmt(t, output)
	require.Equal(t, "utf8mb4", findTableOptionValue(stmt.Options, ast.TableOptionCharset))
	require.Equal(t, "utf8mb4_0900_ai_ci", findTableOptionValue(stmt.Options, ast.TableOptionCollate))

	idCol := findColumn(stmt, "id")
	require.NotNil(t, idCol)
	require.False(t, hasColumnOption(idCol, ast.ColumnOptionDefaultValue))

	txtCol := findColumn(stmt, "txt")
	require.NotNil(t, txtCol)
	require.False(t, hasColumnOption(txtCol, ast.ColumnOptionDefaultValue))

	vCol := findColumn(stmt, "v")
	require.NotNil(t, vCol)
	require.Equal(t, 768, vCol.Tp.GetFlen())

	gCol := findColumn(stmt, "g")
	require.NotNil(t, gCol)
	require.False(t, hasColumnOption(gCol, ast.ColumnOptionGenerated))

	require.False(t, hasJSONValidCheck(stmt))

	idxTxt := findIndex(stmt, "idx_txt")
	require.NotNil(t, idxTxt)
	require.Len(t, idxTxt.Keys, 1)
	require.Equal(t, 255, idxTxt.Keys[0].Length)

	idxV := findIndex(stmt, "idx_v")
	require.NotNil(t, idxV)
	require.Len(t, idxV.Keys, 1)
	require.Equal(t, 768, idxV.Keys[0].Length)
}

func TestTransformSQLNonStrictReturnsOriginal(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.StrictMode = false
	converter := NewConverter(cfg)

	input := "this is not valid sql"
	output, err := converter.TransformSQL(input)
	require.NoError(t, err)
	require.Equal(t, input, output)
}

func TestTransformSQLTrailingCommaAndVersionMacros(t *testing.T) {
	converter := NewConverter(nil)
	input := "/*!40101 SET NAMES utf8 */;\nCREATE TABLE t (id INT,);"

	output, err := converter.TransformSQL(input)
	require.NoError(t, err)

	stmts := parseStatements(t, output)
	require.Len(t, stmts, 2)
	_, ok := stmts[0].(*ast.SetStmt)
	require.True(t, ok)
	create, ok := stmts[1].(*ast.CreateTableStmt)
	require.True(t, ok)
	require.Len(t, create.Cols, 1)
}

func TestTransformSQLPlannedRules(t *testing.T) {
	converter := NewConverter(nil)
	input := `CREATE TABLE db1.t1 (
  id INT(11) NOT NULL AUTO_INCREMENT,
  ts TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  zero_ts TIMESTAMP NOT NULL DEFAULT '0000-00-00 00:00:00',
  uuid CHAR(36) NOT NULL,
  CONSTRAINT fk_ref FOREIGN KEY (id) REFERENCES db1.t2(id),
  KEY idx_hash (id) USING HASH,
  UNIQUE KEY uuid (uuid)
) AUTO_INCREMENT=0 ROW_FORMAT=COMPACT KEY_BLOCK_SIZE=8;`

	output, err := converter.TransformSQL(input)
	require.NoError(t, err)

	stmt := parseCreateTableStmt(t, output)
	require.Equal(t, "", stmt.Table.Schema.O)

	idCol := findColumn(stmt, "id")
	require.NotNil(t, idCol)
	require.Equal(t, types.UnspecifiedLength, idCol.Tp.GetFlen())

	tsCol := findColumn(stmt, "ts")
	require.NotNil(t, tsCol)
	require.True(t, hasColumnOption(tsCol, ast.ColumnOptionDefaultValue))
	require.True(t, hasColumnOption(tsCol, ast.ColumnOptionOnUpdate))

	zeroCol := findColumn(stmt, "zero_ts")
	require.NotNil(t, zeroCol)
	require.False(t, hasColumnOption(zeroCol, ast.ColumnOptionDefaultValue))

	uuidCol := findColumn(stmt, "uuid")
	require.NotNil(t, uuidCol)
	require.True(t, hasColumnOption(uuidCol, ast.ColumnOptionDefaultValue))

	require.True(t, hasForeignKeyConstraint(stmt))

	idxHash := findIndex(stmt, "idx_hash")
	require.NotNil(t, idxHash)
	if idxHash.Option != nil {
		require.Equal(t, ast.IndexTypeInvalid, idxHash.Option.Tp)
	}

	require.NotNil(t, findIndex(stmt, "uuid_key"))
	require.Nil(t, findIndex(stmt, "uuid"))

	autoInc, ok := findTableOptionUint(stmt.Options, ast.TableOptionAutoIncrement)
	require.True(t, ok)
	require.Equal(t, uint64(1), autoInc)
	require.False(t, hasTableOption(stmt.Options, ast.TableOptionRowFormat))
	require.False(t, hasTableOption(stmt.Options, ast.TableOptionKeyBlockSize))
}

func TestTransformSQLConstraintCleanup(t *testing.T) {
	converter := NewConverter(nil)
	input := `CREATE TABLE t (
  id INT,
  ref_id INT,
  CONSTRAINT fk_ref FOREIGN KEY (ref_id) REFERENCES ref_t(id) MATCH FULL ON DELETE SET DEFAULT ON UPDATE CASCADE
);`

	output, err := converter.TransformSQL(input)
	require.NoError(t, err)

	stmt := parseCreateTableStmt(t, output)
	fk := findConstraintByName(stmt, "fk_ref")
	require.NotNil(t, fk)
	require.Equal(t, ast.ConstraintForeignKey, fk.Tp)
	require.NotNil(t, fk.Refer)
	require.Equal(t, ast.MatchNone, fk.Refer.Match)
	require.NotNil(t, fk.Refer.OnDelete)
	require.Equal(t, ast.ReferOptionNoOption, fk.Refer.OnDelete.ReferOpt)
	require.NotNil(t, fk.Refer.OnUpdate)
	require.Equal(t, ast.ReferOptionCascade, fk.Refer.OnUpdate.ReferOpt)
}

func TestTransformSQLSystemVersioningCleanup(t *testing.T) {
	converter := NewConverter(nil)
	input := `CREATE TABLE t (
  id INT,
  sys_start TIMESTAMP(6) GENERATED ALWAYS AS ROW START,
  sys_end TIMESTAMP(6) GENERATED ALWAYS AS ROW END,
  PERIOD FOR SYSTEM_TIME (sys_start, sys_end)
) WITH SYSTEM VERSIONING;`

	output, err := converter.TransformSQL(input)
	require.NoError(t, err)
	require.NotContains(t, strings.ToLower(output), "system versioning")
	require.NotContains(t, strings.ToLower(output), "period for system_time")

	stmt := parseCreateTableStmt(t, output)
	startCol := findColumn(stmt, "sys_start")
	require.NotNil(t, startCol)
	require.False(t, hasColumnOption(startCol, ast.ColumnOptionGenerated))
}

func TestTransformSQLSequenceAndCreateOrReplaceCleanup(t *testing.T) {
	converter := NewConverter(nil)
	input := "CREATE OR REPLACE TABLE t (id INT);\nCREATE SEQUENCE s AS BIGINT;"

	output, err := converter.TransformSQL(input)
	require.NoError(t, err)
	lower := strings.ToLower(output)
	require.NotContains(t, lower, "or replace")
	require.NotContains(t, lower, "as bigint")

	stmts := parseStatements(t, output)
	require.Len(t, stmts, 3)
	_, ok := stmts[0].(*ast.DropTableStmt)
	require.True(t, ok)
	_, ok = stmts[1].(*ast.CreateTableStmt)
	require.True(t, ok)
	_, ok = stmts[2].(*ast.CreateSequenceStmt)
	require.True(t, ok)
}

func TestTransformSQLColumnAttributesAndIndexOptions(t *testing.T) {
	converter := NewConverter(nil)
	input := `CREATE TABLE t (
  a INT INVISIBLE,
  b INT COMPRESSED,
  c INT PERSISTENT,
  KEY idx_ignored (a) IGNORED,
  KEY idx_overlap (a) WITHOUT OVERLAPS,
  VECTOR INDEX idx_vector (a)
);`

	output, err := converter.TransformSQL(input)
	require.NoError(t, err)
	lower := strings.ToLower(output)
	require.NotContains(t, lower, "invisible")
	require.NotContains(t, lower, "compressed")
	require.NotContains(t, lower, "persistent")
	require.NotContains(t, lower, " ignored")
	require.NotContains(t, lower, "not ignored")
	require.NotContains(t, lower, "without overlaps")
	require.NotContains(t, lower, "vector index")

	stmt := parseCreateTableStmt(t, output)
	idxVector := findIndex(stmt, "idx_vector")
	require.NotNil(t, idxVector)
	require.NotEqual(t, ast.ConstraintVector, idxVector.Tp)
}

func TestTransformSQLFulltextSpatialEngineCollationIgnored(t *testing.T) {
	converter := NewConverter(nil)
	collation, ok := pickUnsupportedCollation()
	if !ok {
		t.Skip("no unsupported collation available in parser catalog")
	}

	input := `CREATE TABLE t (
  a TEXT COLUMN_FORMAT DYNAMIC,
  b TEXT STORAGE DISK,
  c TEXT,
  FULLTEXT KEY ft_idx (a, b) WITH PARSER ngram
) ENGINE=MyISAM COLLATE=` + collation + `;

CREATE SPATIAL INDEX sp_idx ON t (c);`

	output, err := converter.TransformSQL(input)
	require.NoError(t, err)

	stmts := parseStatements(t, output)
	require.Len(t, stmts, 2)

	create, ok := stmts[0].(*ast.CreateTableStmt)
	require.True(t, ok)
	require.False(t, hasTableOption(create.Options, ast.TableOptionEngine))
	require.False(t, hasTableOption(create.Options, ast.TableOptionCollate))

	colA := findColumn(create, "a")
	require.NotNil(t, colA)
	require.False(t, hasColumnOption(colA, ast.ColumnOptionColumnFormat))
	colB := findColumn(create, "b")
	require.NotNil(t, colB)
	require.False(t, hasColumnOption(colB, ast.ColumnOptionStorage))

	ft := findConstraintByName(create, "ft_idx")
	require.NotNil(t, ft)
	require.Equal(t, ast.ConstraintFulltext, ft.Tp)
	require.Len(t, ft.Keys, 1)
	if ft.Option != nil {
		require.Equal(t, "", ft.Option.ParserName.O)
	}

	spatial, ok := stmts[1].(*ast.CreateIndexStmt)
	require.True(t, ok)
	require.Equal(t, ast.IndexKeyTypeNone, spatial.KeyType)
}

func parseCreateTableStmt(t *testing.T, sql string) *ast.CreateTableStmt {
	t.Helper()
	p := parser.New()
	stmt, err := p.ParseOneStmt(sql, "", "")
	require.NoError(t, err)
	create, ok := stmt.(*ast.CreateTableStmt)
	require.True(t, ok)
	return create
}

func parseStatements(t *testing.T, sql string) []ast.StmtNode {
	t.Helper()
	p := parser.New()
	stmts, _, err := p.Parse(sql, "", "")
	require.NoError(t, err)
	return stmts
}

func findTableOptionValue(options []*ast.TableOption, optionType ast.TableOptionType) string {
	for _, opt := range options {
		if opt.Tp == optionType {
			return strings.ToLower(opt.StrValue)
		}
	}
	return ""
}

func findTableOptionUint(options []*ast.TableOption, optionType ast.TableOptionType) (uint64, bool) {
	for _, opt := range options {
		if opt.Tp == optionType {
			return opt.UintValue, true
		}
	}
	return 0, false
}

func hasTableOption(options []*ast.TableOption, optionType ast.TableOptionType) bool {
	for _, opt := range options {
		if opt.Tp == optionType {
			return true
		}
	}
	return false
}

func findColumn(stmt *ast.CreateTableStmt, name string) *ast.ColumnDef {
	lower := strings.ToLower(name)
	for _, col := range stmt.Cols {
		if col.Name.Name.L == lower {
			return col
		}
	}
	return nil
}

func hasColumnOption(col *ast.ColumnDef, optionType ast.ColumnOptionType) bool {
	for _, opt := range col.Options {
		if opt.Tp == optionType {
			return true
		}
	}
	return false
}

func hasForeignKeyConstraint(stmt *ast.CreateTableStmt) bool {
	for _, constraint := range stmt.Constraints {
		if constraint.Tp == ast.ConstraintForeignKey {
			return true
		}
	}
	return false
}

func hasJSONValidCheck(stmt *ast.CreateTableStmt) bool {
	for _, constraint := range stmt.Constraints {
		if constraint.Tp == ast.ConstraintCheck && isJSONValidExpr(constraint.Expr) {
			return true
		}
	}
	return false
}

func isJSONValidExpr(expr ast.ExprNode) bool {
	fc, ok := expr.(*ast.FuncCallExpr)
	if !ok {
		return false
	}
	return strings.EqualFold(fc.FnName.O, "json_valid")
}

func findIndex(stmt *ast.CreateTableStmt, name string) *ast.Constraint {
	lower := strings.ToLower(name)
	for _, constraint := range stmt.Constraints {
		switch constraint.Tp {
		case ast.ConstraintPrimaryKey, ast.ConstraintKey, ast.ConstraintIndex, ast.ConstraintUniq:
			if strings.EqualFold(constraint.Name, lower) {
				return constraint
			}
		}
	}
	return nil
}

func findConstraintByName(stmt *ast.CreateTableStmt, name string) *ast.Constraint {
	lower := strings.ToLower(name)
	for _, constraint := range stmt.Constraints {
		if strings.EqualFold(constraint.Name, lower) {
			return constraint
		}
	}
	return nil
}

func pickUnsupportedCollation() (string, bool) {
	supported := make(map[string]struct{})
	for _, coll := range collate.GetSupportedCollations() {
		supported[strings.ToLower(coll.Name)] = struct{}{}
	}
	for _, coll := range charset.GetSupportedCollations() {
		name := strings.ToLower(coll.Name)
		if _, ok := supported[name]; !ok {
			return coll.Name, true
		}
	}
	return "", false
}
