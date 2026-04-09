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

package rules

import (
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
	_ "github.com/pingcap/tidb/pkg/types/parser_driver" // required: register TiDB SQL driver for parser
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tiflow/dm/pkg/mariadbcompat/config"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func parseCreateTable(t *testing.T, sql string) *ast.CreateTableStmt {
	t.Helper()
	p := parser.New()
	stmt, err := p.ParseOneStmt(sql, "", "")
	require.NoError(t, err)
	ct, ok := stmt.(*ast.CreateTableStmt)
	require.True(t, ok)
	return ct
}

func findCol(stmt *ast.CreateTableStmt, name string) *ast.ColumnDef {
	for _, col := range stmt.Cols {
		if strings.EqualFold(col.Name.Name.O, name) {
			return col
		}
	}
	return nil
}

func findConstraint(stmt *ast.CreateTableStmt, name string) *ast.Constraint {
	for _, c := range stmt.Constraints {
		if strings.EqualFold(c.Name, name) {
			return c
		}
	}
	return nil
}

func hasColOpt(col *ast.ColumnDef, tp ast.ColumnOptionType) bool {
	for _, opt := range col.Options {
		if opt.Tp == tp {
			return true
		}
	}
	return false
}

func hasTblOpt(opts []*ast.TableOption, tp ast.TableOptionType) bool {
	for _, opt := range opts {
		if opt.Tp == tp {
			return true
		}
	}
	return false
}

func tblOptStr(opts []*ast.TableOption, tp ast.TableOptionType) string {
	for _, opt := range opts {
		if opt.Tp == tp {
			return strings.ToLower(opt.StrValue)
		}
	}
	return ""
}

// ---------------------------------------------------------------------------
// CollationRule
// ---------------------------------------------------------------------------

func TestCollationRule_CreateTable(t *testing.T) {
	cfg := config.DefaultConfig()
	rule := NewCollationRule(cfg)

	stmt := parseCreateTable(t, `CREATE TABLE t (id INT) DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;`)
	require.True(t, rule.ShouldApply(stmt))

	out, err := rule.Apply(stmt)
	require.NoError(t, err)
	ct := out.(*ast.CreateTableStmt)

	require.Equal(t, "utf8mb4", tblOptStr(ct.Options, ast.TableOptionCharset))
	require.Equal(t, "utf8mb4_0900_ai_ci", tblOptStr(ct.Options, ast.TableOptionCollate))
}

func TestCollationRule_NoMatchReturnsUnchanged(t *testing.T) {
	cfg := config.DefaultConfig()
	rule := NewCollationRule(cfg)

	stmt := parseCreateTable(t, `CREATE TABLE t (id INT) DEFAULT CHARSET=utf8mb4;`)
	require.False(t, rule.ShouldApply(stmt))
}

func TestCollationRule_WildcardCollation(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.CollationMappings = map[string]string{
		"utf8mb4_unicode_*": "utf8mb4_0900_ai_ci",
	}
	rule := NewCollationRule(cfg)

	stmt := parseCreateTable(t, `CREATE TABLE t (id INT) COLLATE=utf8mb4_unicode_ci;`)
	require.True(t, rule.ShouldApply(stmt))

	out, err := rule.Apply(stmt)
	require.NoError(t, err)
	ct := out.(*ast.CreateTableStmt)
	require.Equal(t, "utf8mb4_0900_ai_ci", tblOptStr(ct.Options, ast.TableOptionCollate))
}

// ---------------------------------------------------------------------------
// ConstraintsRule
// ---------------------------------------------------------------------------

func TestConstraintsRule_MatchFullRemoved(t *testing.T) {
	rule := &ConstraintsRule{}
	stmt := parseCreateTable(t, `CREATE TABLE t (
		id INT,
		ref_id INT,
		CONSTRAINT fk FOREIGN KEY (ref_id) REFERENCES r(id) MATCH FULL ON DELETE CASCADE ON UPDATE CASCADE
	);`)

	require.True(t, rule.ShouldApply(stmt))
	out, err := rule.Apply(stmt)
	require.NoError(t, err)
	ct := out.(*ast.CreateTableStmt)
	fk := findConstraint(ct, "fk")
	require.NotNil(t, fk)
	require.Equal(t, ast.MatchNone, fk.Refer.Match)
	require.Equal(t, ast.ReferOptionCascade, fk.Refer.OnDelete.ReferOpt)
	require.Equal(t, ast.ReferOptionCascade, fk.Refer.OnUpdate.ReferOpt)
}

func TestConstraintsRule_SetDefaultReplaced(t *testing.T) {
	rule := &ConstraintsRule{}
	stmt := parseCreateTable(t, `CREATE TABLE t (
		id INT,
		ref_id INT,
		CONSTRAINT fk FOREIGN KEY (ref_id) REFERENCES r(id) ON DELETE SET DEFAULT ON UPDATE SET DEFAULT
	);`)

	require.True(t, rule.ShouldApply(stmt))
	out, err := rule.Apply(stmt)
	require.NoError(t, err)
	ct := out.(*ast.CreateTableStmt)
	fk := findConstraint(ct, "fk")
	require.NotNil(t, fk)
	require.Equal(t, ast.ReferOptionNoOption, fk.Refer.OnDelete.ReferOpt)
	require.Equal(t, ast.ReferOptionNoOption, fk.Refer.OnUpdate.ReferOpt)
}

func TestConstraintsRule_CascadePreserved(t *testing.T) {
	rule := &ConstraintsRule{}
	stmt := parseCreateTable(t, `CREATE TABLE t (
		id INT,
		ref_id INT,
		CONSTRAINT fk FOREIGN KEY (ref_id) REFERENCES r(id) ON DELETE CASCADE ON UPDATE CASCADE
	);`)

	require.False(t, rule.ShouldApply(stmt))
}

// ---------------------------------------------------------------------------
// IndexPrefixRule
// ---------------------------------------------------------------------------

func TestIndexPrefixRule_TextGets255(t *testing.T) {
	rule := &IndexPrefixRule{}
	stmt := parseCreateTable(t, `CREATE TABLE t (
		a TEXT,
		KEY idx_a (a)
	);`)

	require.True(t, rule.ShouldApply(stmt))
	out, err := rule.Apply(stmt)
	require.NoError(t, err)
	ct := out.(*ast.CreateTableStmt)
	idx := findConstraint(ct, "idx_a")
	require.NotNil(t, idx)
	require.Equal(t, 255, idx.Keys[0].Length)
}

func TestIndexPrefixRule_VarcharUsesColumnLength(t *testing.T) {
	rule := &IndexPrefixRule{}
	stmt := parseCreateTable(t, `CREATE TABLE t (
		a VARCHAR(500),
		KEY idx_a (a)
	);`)

	require.True(t, rule.ShouldApply(stmt))
	out, err := rule.Apply(stmt)
	require.NoError(t, err)
	ct := out.(*ast.CreateTableStmt)
	idx := findConstraint(ct, "idx_a")
	require.NotNil(t, idx)
	require.Equal(t, 500, idx.Keys[0].Length)
}

func TestIndexPrefixRule_ExistingPrefixPreserved(t *testing.T) {
	rule := &IndexPrefixRule{}
	stmt := parseCreateTable(t, `CREATE TABLE t (
		a TEXT,
		KEY idx_a (a(100))
	);`)

	require.False(t, rule.ShouldApply(stmt))
}

// ---------------------------------------------------------------------------
// CollationFallbackRule
// ---------------------------------------------------------------------------

func TestCollationFallbackRule_UnsupportedRemoved(t *testing.T) {
	rule := &CollationFallbackRule{}
	name, ok := pickUnsupportedCollation()
	if !ok {
		t.Skip("no unsupported collation available")
	}

	stmt := parseCreateTable(t, `CREATE TABLE t (id INT) COLLATE=`+name+`;`)
	require.True(t, rule.ShouldApply(stmt))

	out, err := rule.Apply(stmt)
	require.NoError(t, err)
	ct := out.(*ast.CreateTableStmt)
	require.False(t, hasTblOpt(ct.Options, ast.TableOptionCollate))
}

func TestCollationFallbackRule_SupportedKept(t *testing.T) {
	rule := &CollationFallbackRule{}
	stmt := parseCreateTable(t, `CREATE TABLE t (id INT) COLLATE=utf8mb4_general_ci;`)
	require.False(t, rule.ShouldApply(stmt))
}

func pickUnsupportedCollation() (string, bool) {
	supported := make(map[string]struct{})
	for _, coll := range collate.GetSupportedCollations() {
		supported[strings.ToLower(coll.Name)] = struct{}{}
	}
	// Look in the parser's charset catalog for a collation not supported at runtime
	for _, cs := range []string{"utf8mb4_ja_0900_as_cs_ks", "utf8mb4_uca_test_ci", "utf32_general_ci"} {
		if _, ok := supported[cs]; !ok {
			return cs, true
		}
	}
	return "", false
}

// ---------------------------------------------------------------------------
// ZeroTimestampRule
// ---------------------------------------------------------------------------

func TestZeroTimestampRule_Removed(t *testing.T) {
	rule := &ZeroTimestampRule{}
	stmt := parseCreateTable(t, `CREATE TABLE t (ts TIMESTAMP NOT NULL DEFAULT '0000-00-00 00:00:00');`)
	col := findCol(stmt, "ts")
	require.NotNil(t, col)
	require.True(t, rule.ShouldApply(col))

	out, err := rule.Apply(col)
	require.NoError(t, err)
	cd := out.(*ast.ColumnDef)
	require.False(t, hasColOpt(cd, ast.ColumnOptionDefaultValue))
}

func TestZeroTimestampRule_NonZeroKept(t *testing.T) {
	rule := &ZeroTimestampRule{}
	stmt := parseCreateTable(t, `CREATE TABLE t (ts TIMESTAMP NOT NULL DEFAULT '2024-01-01 00:00:00');`)
	col := findCol(stmt, "ts")
	require.NotNil(t, col)
	require.False(t, rule.ShouldApply(col))
}

// ---------------------------------------------------------------------------
// IntegerWidthRule
// ---------------------------------------------------------------------------

func TestIntegerWidthRule_StripsWidth(t *testing.T) {
	rule := &IntegerWidthRule{}
	stmt := parseCreateTable(t, `CREATE TABLE t (id INT(11));`)
	col := findCol(stmt, "id")
	require.NotNil(t, col)
	require.True(t, rule.ShouldApply(col))

	out, err := rule.Apply(col)
	require.NoError(t, err)
	cd := out.(*ast.ColumnDef)
	require.Equal(t, types.UnspecifiedLength, cd.Tp.GetFlen())
}

func TestIntegerWidthRule_NoWidthNoOp(t *testing.T) {
	rule := &IntegerWidthRule{}
	stmt := parseCreateTable(t, `CREATE TABLE t (id INT);`)
	col := findCol(stmt, "id")
	require.NotNil(t, col)
	require.False(t, rule.ShouldApply(col))
}

func TestIntegerWidthRule_NonIntegerIgnored(t *testing.T) {
	rule := &IntegerWidthRule{}
	stmt := parseCreateTable(t, `CREATE TABLE t (v VARCHAR(100));`)
	col := findCol(stmt, "v")
	require.NotNil(t, col)
	require.False(t, rule.ShouldApply(col))
}

// ---------------------------------------------------------------------------
// TextBlobDefaultRule
// ---------------------------------------------------------------------------

func TestTextBlobDefaultRule_DefaultRemoved(t *testing.T) {
	rule := &TextBlobDefaultRule{}
	stmt := parseCreateTable(t, `CREATE TABLE t (a TEXT DEFAULT 'x');`)
	col := findCol(stmt, "a")
	require.NotNil(t, col)
	require.True(t, rule.ShouldApply(col))

	out, err := rule.Apply(col)
	require.NoError(t, err)
	cd := out.(*ast.ColumnDef)
	require.False(t, hasColOpt(cd, ast.ColumnOptionDefaultValue))
}

func TestTextBlobDefaultRule_NoDefaultNoOp(t *testing.T) {
	rule := &TextBlobDefaultRule{}
	stmt := parseCreateTable(t, `CREATE TABLE t (a TEXT);`)
	col := findCol(stmt, "a")
	require.NotNil(t, col)
	require.False(t, rule.ShouldApply(col))
}

func TestTextBlobDefaultRule_VarcharIgnored(t *testing.T) {
	rule := &TextBlobDefaultRule{}
	stmt := parseCreateTable(t, `CREATE TABLE t (a VARCHAR(100) DEFAULT 'x');`)
	col := findCol(stmt, "a")
	require.NotNil(t, col)
	require.False(t, rule.ShouldApply(col))
}

// ---------------------------------------------------------------------------
// AutoIncrementValuesRule
// ---------------------------------------------------------------------------

func TestAutoIncrementValuesRule_ZeroBecomes1(t *testing.T) {
	rule := &AutoIncrementValuesRule{}
	stmt := parseCreateTable(t, `CREATE TABLE t (id INT AUTO_INCREMENT PRIMARY KEY) AUTO_INCREMENT=0;`)
	require.True(t, rule.ShouldApply(stmt))

	out, err := rule.Apply(stmt)
	require.NoError(t, err)
	ct := out.(*ast.CreateTableStmt)
	for _, opt := range ct.Options {
		if opt.Tp == ast.TableOptionAutoIncrement {
			require.Equal(t, uint64(1), opt.UintValue)
		}
	}
}

func TestAutoIncrementValuesRule_PositiveNoOp(t *testing.T) {
	rule := &AutoIncrementValuesRule{}
	stmt := parseCreateTable(t, `CREATE TABLE t (id INT AUTO_INCREMENT PRIMARY KEY) AUTO_INCREMENT=100;`)
	require.False(t, rule.ShouldApply(stmt))
}

// ---------------------------------------------------------------------------
// MariaDBSpecificRule
// ---------------------------------------------------------------------------

func TestMariaDBSpecificRule_StripsOptions(t *testing.T) {
	rule := &MariaDBSpecificRule{}
	stmt := parseCreateTable(t, `CREATE TABLE t (id INT) ROW_FORMAT=COMPACT KEY_BLOCK_SIZE=8;`)
	require.True(t, rule.ShouldApply(stmt))

	out, err := rule.Apply(stmt)
	require.NoError(t, err)
	ct := out.(*ast.CreateTableStmt)
	require.False(t, hasTblOpt(ct.Options, ast.TableOptionRowFormat))
	require.False(t, hasTblOpt(ct.Options, ast.TableOptionKeyBlockSize))
}

func TestMariaDBSpecificRule_InnoDB_NotStripped(t *testing.T) {
	// ENGINE option is handled by EngineOptionsRule, not this rule
	rule := &MariaDBSpecificRule{}
	stmt := parseCreateTable(t, `CREATE TABLE t (id INT) ENGINE=InnoDB;`)
	require.False(t, rule.ShouldApply(stmt))
}

// ---------------------------------------------------------------------------
// EngineOptionsRule
// ---------------------------------------------------------------------------

func TestEngineOptionsRule_StripsEngine(t *testing.T) {
	rule := &EngineOptionsRule{}
	stmt := parseCreateTable(t, `CREATE TABLE t (id INT) ENGINE=MyISAM;`)
	require.True(t, rule.ShouldApply(stmt))

	out, err := rule.Apply(stmt)
	require.NoError(t, err)
	ct := out.(*ast.CreateTableStmt)
	require.False(t, hasTblOpt(ct.Options, ast.TableOptionEngine))
}

// ---------------------------------------------------------------------------
// KeyLengthRule
// ---------------------------------------------------------------------------

func TestKeyLengthRule_VarcharTruncatedTo768(t *testing.T) {
	rule := &KeyLengthRule{}
	stmt := parseCreateTable(t, `CREATE TABLE t (a VARCHAR(1000), KEY idx_a (a));`)
	col := findCol(stmt, "a")
	require.NotNil(t, col)
	require.True(t, rule.ShouldApply(col))

	out, err := rule.Apply(col)
	require.NoError(t, err)
	cd := out.(*ast.ColumnDef)
	require.Equal(t, 768, cd.Tp.GetFlen())
}

func TestKeyLengthRule_ShortVarcharNoOp(t *testing.T) {
	rule := &KeyLengthRule{}
	stmt := parseCreateTable(t, `CREATE TABLE t (a VARCHAR(100), KEY idx_a (a));`)
	col := findCol(stmt, "a")
	require.NotNil(t, col)
	require.False(t, rule.ShouldApply(col))
}

// ---------------------------------------------------------------------------
// Preparse helpers
// ---------------------------------------------------------------------------

func TestStripTrailingCommas_Basic(t *testing.T) {
	input := "CREATE TABLE t (id INT,)"
	out, changed := stripTrailingCommas(input)
	require.True(t, changed)
	require.Equal(t, "CREATE TABLE t (id INT)", out)
}

func TestStripTrailingCommas_NoTrailing(t *testing.T) {
	input := "CREATE TABLE t (id INT, name VARCHAR(100))"
	_, changed := stripTrailingCommas(input)
	require.False(t, changed)
}

func TestStripTrailingCommas_InsideString(t *testing.T) {
	input := "SELECT '(,)' FROM t"
	_, changed := stripTrailingCommas(input)
	require.False(t, changed)
}

func TestStripTrailingCommas_WithComment(t *testing.T) {
	input := "CREATE TABLE t (id INT, -- comment\n)"
	out, changed := stripTrailingCommas(input)
	require.True(t, changed)
	require.Contains(t, out, "id INT")
	require.NotContains(t, out, "id INT,")
}

func TestStripVersionMacros(t *testing.T) {
	input := "/*!40101 SET NAMES utf8 */"
	out := stripVersionMacros(input)
	require.Equal(t, "SET NAMES utf8 ", out)
}

func TestStripSystemVersioning(t *testing.T) {
	input := "CREATE TABLE t (id INT) WITH SYSTEM VERSIONING"
	out := stripSystemVersioning(input)
	require.NotContains(t, strings.ToLower(out), "system versioning")
}

func TestStripSystemVersioning_RowStart(t *testing.T) {
	input := "ts TIMESTAMP(6) GENERATED ALWAYS AS ROW START"
	out := stripSystemVersioning(input)
	require.NotContains(t, strings.ToLower(out), "row start")
}

func TestStripColumnAttributes(t *testing.T) {
	input := "a INT INVISIBLE, b TEXT COMPRESSED"
	out := stripColumnAttributes(input)
	require.NotContains(t, strings.ToLower(out), "invisible")
	require.NotContains(t, strings.ToLower(out), "compressed")
}

func TestRewriteCreateOrReplace_Table(t *testing.T) {
	input := "CREATE OR REPLACE TABLE t (id INT)"
	out := rewriteCreateOrReplace(input)
	require.Contains(t, out, "DROP TABLE IF EXISTS t")
	require.Contains(t, out, "CREATE TABLE t")
}

func TestRewriteCreateOrReplace_Index(t *testing.T) {
	input := "CREATE OR REPLACE INDEX idx ON t(a)"
	out := rewriteCreateOrReplace(input)
	require.Contains(t, out, "DROP INDEX IF EXISTS idx ON t")
	require.Contains(t, out, "CREATE INDEX idx ON t")
}

func TestRewriteCreateOrReplace_Sequence(t *testing.T) {
	input := "CREATE OR REPLACE SEQUENCE s"
	out := rewriteCreateOrReplace(input)
	require.Contains(t, out, "DROP SEQUENCE IF EXISTS s")
	require.Contains(t, out, "CREATE SEQUENCE s")
}

func TestStripSequenceType(t *testing.T) {
	input := "CREATE SEQUENCE s AS BIGINT"
	out := stripSequenceType(input)
	require.NotContains(t, strings.ToLower(out), "as bigint")
}

func TestApplyPreparseRules_EmptyList(t *testing.T) {
	out, err := ApplyPreparseRules("SELECT 1", nil)
	require.NoError(t, err)
	require.Equal(t, "SELECT 1", out)
}

// ---------------------------------------------------------------------------
// isZeroTimeString
// ---------------------------------------------------------------------------

func TestIsZeroTimeString(t *testing.T) {
	cases := []struct {
		input string
		want  bool
	}{
		{"0000-00-00", true},
		{"0000-00-00 00:00:00", true},
		{"0000-00-00 00:00:00.000000", true},
		{"0000-00-00 00:00:00.0", true},
		{"2024-01-01", false},
		{"0000-00-00 00:00:01", false},
		{"0000-00-01", false},
		{"", false},
	}
	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			require.Equal(t, tc.want, isZeroTimeString(tc.input))
		})
	}
}

// ---------------------------------------------------------------------------
// Helper function tests
// ---------------------------------------------------------------------------

func TestBuildColumnMap(t *testing.T) {
	stmt := parseCreateTable(t, `CREATE TABLE t (a INT, b TEXT, c VARCHAR(100));`)
	m := buildColumnMap(stmt.Cols)
	require.Len(t, m, 3)
	require.Contains(t, m, "a")
	require.Contains(t, m, "b")
	require.Contains(t, m, "c")
}

func TestNeedsPrefix(t *testing.T) {
	textFt := types.NewFieldType(mysql.TypeBlob)
	require.True(t, needsPrefix(textFt))

	varcharFt := types.NewFieldType(mysql.TypeVarchar)
	require.True(t, needsPrefix(varcharFt))

	intFt := types.NewFieldType(mysql.TypeLong)
	require.False(t, needsPrefix(intFt))
}
