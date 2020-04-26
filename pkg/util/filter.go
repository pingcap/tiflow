package util

import (
	"strings"

	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-tools/pkg/filter"
)

// Filter is a event filter implementation
type Filter struct {
	filter            *filter.Filter
	ignoreTxnCommitTs []uint64
	ddlWhitelist      []model.ActionType
}

// ReplicaConfig represents some addition replication config for a changefeed
type ReplicaConfig struct {
	DDLWhitelist        []model.ActionType `toml:"ddl-white-list" json:"ddl-white-list"`
	FilterCaseSensitive bool               `toml:"filter-case-sensitive" json:"filter-case-sensitive"`
	FilterRules         *filter.Rules      `toml:"filter-rules" json:"filter-rules"`
	IgnoreTxnCommitTs   []uint64           `toml:"ignore-txn-commit-ts" json:"ignore-txn-commit-ts"`
	SinkDispatchRules   []*DispatchRule    `toml:"sink-dispatch-rules" json:"sink-dispatch-rules"`
}

// DispatchRule represents partition rule for a table
type DispatchRule struct {
	filter.Table
	Rule string `toml:"rule" json:"rule"`
}

// NewFilter creates a filter
func NewFilter(config *ReplicaConfig) (*Filter, error) {
	filter, err := filter.New(config.FilterCaseSensitive, config.FilterRules)
	if err != nil {
		return nil, err
	}
	return &Filter{
		filter:            filter,
		ignoreTxnCommitTs: config.IgnoreTxnCommitTs,
		ddlWhitelist:      config.DDLWhitelist,
	}, nil
}

// ShouldIgnoreTxn returns true is the given txn should be ignored
func (f *Filter) shouldIgnoreCommitTs(ts uint64) bool {
	for _, ignoreTs := range f.ignoreTxnCommitTs {
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
	// TODO: Change filter to support simple check directly
	left := f.filter.ApplyOn([]*filter.Table{{Schema: db, Name: tbl}})
	return len(left) == 0
}

// ShouldIgnoreEvent removes DDL/DMLs that's not wanted by this change feed.
// CDC only supports filtering by database/table now.
func (f *Filter) ShouldIgnoreEvent(ts uint64, schema, table string) bool {
	return f.shouldIgnoreCommitTs(ts) || f.ShouldIgnoreTable(schema, table)
}

// ShouldDiscardDDL returns true if this kind of DDL should be discarded
func (f *Filter) ShouldDiscardDDL(ddlType model.ActionType) bool {
	if !f.shouldDiscardByBuiltInDDLWhitelist(ddlType) {
		return false
	}
	for _, whiteDDLType := range f.ddlWhitelist {
		if whiteDDLType == ddlType {
			return false
		}
	}
	return true
}

func (f *Filter) shouldDiscardByBuiltInDDLWhitelist(ddlType model.ActionType) bool {
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
