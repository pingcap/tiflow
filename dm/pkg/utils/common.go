// Copyright 2019 PingCAP, Inc.
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

package utils

import (
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/dbutil"
	"github.com/pingcap/tidb/util/filter"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"
)

func init() {
	ZeroSessionCtx = NewSessionCtx(nil)
}

// TrimCtrlChars returns a slice of the string s with all leading
// and trailing control characters removed.
func TrimCtrlChars(s string) string {
	f := func(r rune) bool {
		// All entries in the ASCII table below code 32 (technically the C0 control code set) are of this kind,
		// including CR and LF used to separate lines of text. The code 127 (DEL) is also a control character.
		// Reference: https://en.wikipedia.org/wiki/Control_character
		return r < 32 || r == 127
	}

	return strings.TrimFunc(s, f)
}

// TrimQuoteMark tries to trim leading and tailing quote(") mark if exists
// only trim if leading and tailing quote matched as a pair.
func TrimQuoteMark(s string) string {
	if len(s) > 2 && s[0] == '"' && s[len(s)-1] == '"' {
		return s[1 : len(s)-1]
	}
	return s
}

// CompareShardingDDLs compares s and t ddls
// only concern in content, ignore order of ddl.
func CompareShardingDDLs(s, t []string) bool {
	if len(s) != len(t) {
		return false
	}

	ddls := make(map[string]struct{})
	for _, ddl := range s {
		ddls[ddl] = struct{}{}
	}

	for _, ddl := range t {
		if _, ok := ddls[ddl]; !ok {
			return false
		}
	}

	return true
}

// GenDDLLockID returns lock ID used in shard-DDL.
func GenDDLLockID(task, schema, table string) string {
	return fmt.Sprintf("%s-%s", task, dbutil.TableName(schema, table))
}

var lockIDPattern = regexp.MustCompile("(.*)\\-\\`(.*)\\`.\\`(.*)\\`")

// ExtractTaskFromLockID extract task from lockID.
func ExtractTaskFromLockID(lockID string) string {
	strs := lockIDPattern.FindStringSubmatch(lockID)
	// strs should be [full-lock-ID, task, db, table] if successful matched
	if len(strs) < 4 {
		return ""
	}
	return strs[1]
}

// ExtractDBAndTableFromLockID extract schema and table from lockID.
func ExtractDBAndTableFromLockID(lockID string) (string, string) {
	strs := lockIDPattern.FindStringSubmatch(lockID)
	// strs should be [full-lock-ID, task, db, table] if successful matched
	if len(strs) < 4 {
		return "", ""
	}
	return strs[2], strs[3]
}

// NonRepeatStringsEqual is used to compare two un-ordered, non-repeat-element string slice is equal.
func NonRepeatStringsEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	m := make(map[string]struct{}, len(a))
	for _, s := range a {
		m[s] = struct{}{}
	}
	for _, s := range b {
		if _, ok := m[s]; !ok {
			return false
		}
	}
	return true
}

// GenTableID generates table ID.
func GenTableID(table *filter.Table) string {
	return table.String()
}

// GenSchemaID generates schema ID.
func GenSchemaID(table *filter.Table) string {
	return "`" + table.Schema + "`"
}

// GenTableIDAndCheckSchemaOnly generates table ID and check if schema only.
func GenTableIDAndCheckSchemaOnly(table *filter.Table) (id string, isSchemaOnly bool) {
	return GenTableID(table), len(table.Name) == 0
}

// UnpackTableID unpacks table ID to <schema, table> pair.
func UnpackTableID(id string) *filter.Table {
	parts := strings.Split(id, "`.`")
	schema := strings.TrimLeft(parts[0], "`")
	table := strings.TrimRight(parts[1], "`")
	return &filter.Table{
		Schema: schema,
		Name:   table,
	}
}

type session struct {
	sessionctx.Context
	vars                 *variable.SessionVars
	values               map[fmt.Stringer]interface{}
	builtinFunctionUsage map[string]uint32
	mu                   sync.RWMutex
}

// GetSessionVars implements the sessionctx.Context interface.
func (se *session) GetSessionVars() *variable.SessionVars {
	return se.vars
}

// SetValue implements the sessionctx.Context interface.
func (se *session) SetValue(key fmt.Stringer, value interface{}) {
	se.mu.Lock()
	se.values[key] = value
	se.mu.Unlock()
}

// Value implements the sessionctx.Context interface.
func (se *session) Value(key fmt.Stringer) interface{} {
	se.mu.RLock()
	value := se.values[key]
	se.mu.RUnlock()
	return value
}

// GetInfoSchema implements the sessionctx.Context interface.
func (se *session) GetInfoSchema() sessionctx.InfoschemaMetaVersion {
	return nil
}

// GetBuiltinFunctionUsage implements the sessionctx.Context interface.
func (se *session) GetBuiltinFunctionUsage() map[string]uint32 {
	return se.builtinFunctionUsage
}

func (se *session) BuiltinFunctionUsageInc(scalarFuncSigName string) {}

// ZeroSessionCtx is used when the session variables is not important.
var ZeroSessionCtx sessionctx.Context

// NewSessionCtx return a session context with specified session variables.
func NewSessionCtx(vars map[string]string) sessionctx.Context {
	variables := variable.NewSessionVars(nil)
	for k, v := range vars {
		_ = variables.SetSystemVar(k, v)
		if strings.EqualFold(k, "time_zone") {
			loc, _ := ParseTimeZone(v)
			variables.StmtCtx.TimeZone = loc
		}
	}

	return &session{
		vars:                 variables,
		values:               make(map[fmt.Stringer]interface{}, 1),
		builtinFunctionUsage: make(map[string]uint32),
	}
}

// AdjustBinaryProtocolForDatum converts the data in binlog to TiDB datum.
func AdjustBinaryProtocolForDatum(ctx sessionctx.Context, data []interface{}, cols []*model.ColumnInfo) ([]types.Datum, error) {
	ret := make([]types.Datum, 0, len(data))
	for i, d := range data {
		datum := types.NewDatum(d)
		castDatum, err := table.CastValue(ctx, datum, cols[i], false, false)
		if err != nil {
			return nil, err
		}
		ret = append(ret, castDatum)
	}
	return ret, nil
}

// GoLogWrapper go routine wrapper, log error on panic.
func GoLogWrapper(logger log.Logger, fn func()) {
	defer func() {
		if err := recover(); err != nil {
			logger.Error("routine panic", zap.Any("err", err))
		}
	}()

	fn()
}
