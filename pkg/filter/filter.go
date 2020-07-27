// Copyright 2020 PingCAP, Inc.
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
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	filter "github.com/pingcap/tidb-tools/pkg/table-filter"

	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/cyclic/mark"
)

//errNoSuchMySQLType means the input mysql type is invalid
var errNoSuchMySQLType = errors.New("No such mysql type")

// Filter is a event filter implementation
type Filter struct {
	filter           filter.Filter
	ignoreTxnStartTs []uint64
	ignoreColumnType map[byte]struct{}
	ddlAllowlist     []model.ActionType
	isCyclicEnabled  bool
}

// NewFilter creates a filter
func NewFilter(cfg *config.ReplicaConfig) (*Filter, error) {
	var f filter.Filter
	var err error
	var ignoreColumnType map[byte]struct{}
	if len(cfg.Filter.Rules) == 0 && cfg.Filter.MySQLReplicationRules != nil {
		f, err = filter.ParseMySQLReplicationRules(cfg.Filter.MySQLReplicationRules)
	} else {
		rules := cfg.Filter.Rules
		if len(rules) == 0 {
			rules = []string{"*.*"}
		}
		f, err = filter.Parse(rules)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	ignoreColumnType, err = strsToColumnTypes(cfg.Filter.IgnoreColumnType)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !cfg.CaseSensitive {
		f = filter.CaseInsensitive(f)
	}
	return &Filter{
		filter:           f,
		ignoreTxnStartTs: cfg.Filter.IgnoreTxnStartTs,
		ignoreColumnType: ignoreColumnType,
		ddlAllowlist:     cfg.Filter.DDLAllowlist,
		isCyclicEnabled:  cfg.Cyclic.IsEnabled(),
	}, nil
}

func (f *Filter) shouldIgnoreStartTs(ts uint64) bool {
	for _, ignoreTs := range f.ignoreTxnStartTs {
		if ignoreTs == ts {
			return true
		}
	}
	return false
}

// ShouldIgnoreTable returns true if the specified table should be ignored by this change feed.
// Set `tbl` to an empty string to test against the whole database.
func (f *Filter) ShouldIgnoreTable(db, tbl string) bool {
	if IsSysSchema(db) {
		return true
	}
	if f.isCyclicEnabled && mark.IsMarkTable(db, tbl) {
		// Always replicate mark tables.
		return false
	}
	return !f.filter.MatchTable(db, tbl)
}

// ShouldIgnoreDMLEvent removes DMLs that's not wanted by this change feed.
// CDC only supports filtering by database/table now.
func (f *Filter) ShouldIgnoreDMLEvent(ts uint64, schema, table string, columnTypes []byte) bool {
	return f.shouldIgnoreStartTs(ts) || f.ShouldIgnoreTable(schema, table) || f.shouldIgnoreColumnTypes(columnTypes)
}

// ShouldIgnoreDDLEvent removes DDLs that's not wanted by this change feed.
// CDC only supports filtering by database/table now.
func (f *Filter) ShouldIgnoreDDLEvent(ts uint64, schema, table string, columnTypes []byte) bool {
	return f.shouldIgnoreStartTs(ts) || f.ShouldIgnoreTable(schema, table) || f.shouldIgnoreColumnTypes(columnTypes)
}

// ShouldDiscardDDL returns true if this DDL should be discarded
func (f *Filter) ShouldDiscardDDL(ddlType model.ActionType) bool {
	if !f.shouldDiscardByBuiltInDDLAllowlist(ddlType) {
		return false
	}
	for _, allowDDLType := range f.ddlAllowlist {
		if allowDDLType == ddlType {
			return false
		}
	}
	return true
}

func (f *Filter) shouldIgnoreColumnTypes(columnTypes []byte) bool {
	for _, typ := range columnTypes {
		if _, exist := f.ignoreColumnType[typ]; exist {
			return true
		}
	}
	return false
}

func (f *Filter) shouldDiscardByBuiltInDDLAllowlist(ddlType model.ActionType) bool {
	/* The following DDL will be filter:
	ActionAddForeignKey                 ActionType = 9
	ActionDropForeignKey                ActionType = 10
	ActionRebaseAutoID                  ActionType = 13
	ActionShardRowID                    ActionType = 16
	ActionLockTable                     ActionType = 27
	ActionUnlockTable                   ActionType = 28
	ActionRepairTable                   ActionType = 29
	ActionSetTiFlashReplica             ActionType = 30
	ActionUpdateTiFlashReplicaStatus    ActionType = 31
	ActionCreateSequence                ActionType = 34
	ActionAlterSequence                 ActionType = 35
	ActionDropSequence                  ActionType = 36
	*/
	switch ddlType {
	case model.ActionCreateSchema,
		model.ActionDropSchema,
		model.ActionCreateTable,
		model.ActionDropTable,
		model.ActionAddColumn,
		model.ActionDropColumn,
		model.ActionAddIndex,
		model.ActionDropIndex,
		model.ActionTruncateTable,
		model.ActionModifyColumn,
		model.ActionRenameTable,
		model.ActionSetDefaultValue,
		model.ActionModifyTableComment,
		model.ActionRenameIndex,
		model.ActionAddTablePartition,
		model.ActionDropTablePartition,
		model.ActionCreateView,
		model.ActionModifyTableCharsetAndCollate,
		model.ActionTruncateTablePartition,
		model.ActionDropView,
		model.ActionRecoverTable,
		model.ActionModifySchemaCharsetAndCollate,
		model.ActionAddPrimaryKey,
		model.ActionDropPrimaryKey:
		return false
	}
	return true
}

var str2Type = map[string]byte{
	"bit":         mysql.TypeBit,
	"text":        mysql.TypeBlob,
	"date":        mysql.TypeDate,
	"datetime":    mysql.TypeDatetime,
	"unspecified": mysql.TypeDecimal,
	"decimal":     mysql.TypeNewDecimal,
	"double":      mysql.TypeDouble,
	"enum":        mysql.TypeEnum,
	"float":       mysql.TypeFloat,
	"geometry":    mysql.TypeGeometry,
	"mediumint":   mysql.TypeInt24,
	"json":        mysql.TypeJSON,
	"int":         mysql.TypeLong,
	"bigint":      mysql.TypeLonglong,
	"longtext":    mysql.TypeLongBlob,
	"mediumtext":  mysql.TypeMediumBlob,
	"null":        mysql.TypeNull,
	"set":         mysql.TypeSet,
	"smallint":    mysql.TypeShort,
	"char":        mysql.TypeString,
	"time":        mysql.TypeDuration,
	"timestamp":   mysql.TypeTimestamp,
	"tinyint":     mysql.TypeTiny,
	"tinytext":    mysql.TypeTinyBlob,
	"varchar":     mysql.TypeVarchar,
	"var_string":  mysql.TypeVarString,
	"year":        mysql.TypeYear,
}

// StrType converts string to a type.
func StrType(str string) (r byte, err error) {
	typStr := strings.ToLower(str)
	if strings.Contains(typStr, "blob") {
		typStr = strings.ReplaceAll(typStr, "blob", "text")
	}
	if strings.Contains(typStr, "binary") {
		typStr = strings.ReplaceAll(typStr, "binary", "char")
	}
	if typ, exist := str2Type[typStr]; exist {
		return typ, nil
	}
	return 0, errNoSuchMySQLType
}

func strsToColumnTypes(columnTypeStr []string) (map[byte]struct{}, error) {
	columnTypeByte := make(map[byte]struct{}, len(columnTypeStr))
	for i := range columnTypeStr {
		typ, err := StrType(columnTypeStr[i])
		if err != nil {
			return nil, err
		}
		columnTypeByte[typ] = struct{}{}
	}
	return columnTypeByte, nil
}

// IsSysSchema returns true if the given schema is a system schema
func IsSysSchema(db string) bool {
	db = strings.ToUpper(db)
	for _, schema := range []string{"INFORMATION_SCHEMA", "PERFORMANCE_SCHEMA", "MYSQL", "METRIC_SCHEMA"} {
		if schema == db {
			return true
		}
	}
	return false
}
