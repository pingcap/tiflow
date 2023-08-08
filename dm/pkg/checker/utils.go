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
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-tools/pkg/utils"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"
)

// MySQLVersion represents MySQL version number.
type MySQLVersion [3]uint

// MinVersion define a mininum version.
var MinVersion = MySQLVersion{0, 0, 0}

// MaxVersion define a maximum version.
var MaxVersion = MySQLVersion{math.MaxUint8, math.MaxUint8, math.MaxUint8}

// version format:
// mysql        5.7.18-log
// mariadb      5.5.50-MariaDB-1~wheezy
// percona      5.7.19-17-log
// aliyun rds   5.7.18-log
// aws rds      5.7.16-log
// ref: https://dev.mysql.com/doc/refman/5.7/en/which-version.html

// v is mysql version in string format.
func toMySQLVersion(v string) (MySQLVersion, error) {
	version := MySQLVersion{0, 0, 0}
	tmp := strings.Split(v, "-")
	if len(tmp) == 0 {
		return version, errors.NotValidf("MySQL version %s", v)
	}

	tmp = strings.Split(tmp[0], ".")
	if len(tmp) != 3 {
		return version, errors.NotValidf("MySQL version %s", v)
	}

	for i := range tmp {
		val, err := strconv.ParseUint(tmp[i], 10, 64)
		if err != nil {
			return version, errors.NotValidf("MySQL version %s", v)
		}
		version[i] = uint(val)
	}
	return version, nil
}

// Ge means v >= min.
func (v MySQLVersion) Ge(min MySQLVersion) bool {
	for i := range v {
		if v[i] > min[i] {
			return true
		} else if v[i] < min[i] {
			return false
		}
	}
	return true
}

// Gt means v > min.
func (v MySQLVersion) Gt(min MySQLVersion) bool {
	for i := range v {
		if v[i] > min[i] {
			return true
		} else if v[i] < min[i] {
			return false
		}
	}
	return false
}

// Lt means v < min.
func (v MySQLVersion) Lt(max MySQLVersion) bool {
	for i := range v {
		if v[i] < max[i] {
			return true
		} else if v[i] > max[i] {
			return false
		}
	}
	return false
}

// Le means v <= min.
func (v MySQLVersion) Le(max MySQLVersion) bool {
	for i := range v {
		if v[i] < max[i] {
			return true
		} else if v[i] > max[i] {
			return false
		}
	}
	return true
}

// String implements the Stringer interface.
func (v MySQLVersion) String() string {
	return fmt.Sprintf("%d.%d.%d", v[0], v[1], v[2])
}

// IsTiDBFromVersion tells whether the version is tidb.
func IsTiDBFromVersion(version string) bool {
	return strings.Contains(strings.ToUpper(version), "TIDB")
}

func markCheckError(result *Result, err error) {
	if err != nil {
		state := StateFailure
		if utils.OriginError(err) == context.Canceled {
			state = StateWarning
		}
		// `StateWarning` can't cover `StateFailure`.
		if result.State != StateFailure {
			result.State = state
		}
		result.Errors = append(result.Errors, &Error{Severity: state, ShortErr: err.Error()})
	}
}

func markCheckErrorFromParser(result *Result, err error) {
	if err != nil {
		state := StateWarning
		// `StateWarning` can't cover `StateFailure`.
		if result.State != StateFailure {
			result.State = state
		}
		result.Errors = append(result.Errors, &Error{Severity: state, ShortErr: err.Error()})
	}
}

//nolint:unparam
func isMySQLError(err error, code uint16) bool {
	err = errors.Cause(err)
	e, ok := err.(*mysql.MySQLError)
	return ok && e.Number == code
}

func getCreateTableStmt(p *parser.Parser, statement string) (*ast.CreateTableStmt, error) {
	stmt, err := p.ParseOneStmt(statement, "", "")
	if err != nil {
		return nil, errors.Annotatef(err, "statement %s", statement)
	}

	ctStmt, ok := stmt.(*ast.CreateTableStmt)
	if !ok {
		return nil, errors.Errorf("Expect CreateTableStmt but got %T", stmt)
	}
	return ctStmt, nil
}

func getCharset(stmt *ast.CreateTableStmt) string {
	if stmt.Options != nil {
		for _, option := range stmt.Options {
			if option.Tp == ast.TableOptionCharset {
				return option.StrValue
			}
		}
	}
	return ""
}

func getCollation(stmt *ast.CreateTableStmt) string {
	if stmt.Options != nil {
		for _, option := range stmt.Options {
			if option.Tp == ast.TableOptionCollate {
				return option.StrValue
			}
		}
	}
	return ""
}

// getPKAndUK returns a map of INDEX_NAME -> set of COLUMN_NAMEs.
func getPKAndUK(stmt *ast.CreateTableStmt) map[string]map[string]struct{} {
	ret := make(map[string]map[string]struct{})
	var sb strings.Builder
	restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)

	for _, constraint := range stmt.Constraints {
		switch constraint.Tp {
		case ast.ConstraintPrimaryKey:
			ret["PRIMARY"] = make(map[string]struct{})
			for _, key := range constraint.Keys {
				ret["PRIMARY"][key.Column.Name.L] = struct{}{}
			}
		case ast.ConstraintUniq, ast.ConstraintUniqKey, ast.ConstraintUniqIndex:
			ret[constraint.Name] = make(map[string]struct{})
			for _, key := range constraint.Keys {
				if key.Column != nil {
					ret[constraint.Name][key.Column.Name.L] = struct{}{}
				} else {
					sb.Reset()
					err := key.Expr.Restore(restoreCtx)
					if err != nil {
						log.L().Warn("failed to restore expression", zap.Error(err))
						continue
					}
					ret[constraint.Name][sb.String()] = struct{}{}
				}
			}
		}
	}
	return ret
}

func stringSetEqual(a, b map[string]struct{}) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if _, ok := b[k]; !ok {
			return false
		}
	}
	return true
}

// getColumnsAndIgnorable return a map of COLUMN_NAME -> if this columns can be
// ignored when inserting data, which means it has default value or can be null.
func getColumnsAndIgnorable(stmt *ast.CreateTableStmt) map[string]bool {
	ret := make(map[string]bool)
	for _, col := range stmt.Cols {
		notNull := false
		hasDefaultValue := false
		for _, opt := range col.Options {
			switch opt.Tp {
			case ast.ColumnOptionNotNull:
				notNull = true
			case ast.ColumnOptionDefaultValue,
				ast.ColumnOptionAutoIncrement,
				ast.ColumnOptionAutoRandom,
				ast.ColumnOptionGenerated:
				// if the generated column has NOT NULL, its referring columns
				// must not be NULL. But even if we mark the referring columns
				// as not ignorable, the data may still be NULL so replication
				// is still failed. For simplicity, we just ignore this case.
				hasDefaultValue = true
			}
		}
		ret[col.Name.Name.L] = !notNull || hasDefaultValue
	}
	return ret
}
