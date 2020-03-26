package util

import (
	"strings"

	"github.com/pingcap/log"
	"go.uber.org/zap"

	"github.com/pingcap/parser/model"

	"github.com/pingcap/tidb-tools/pkg/filter"
)

// Filter is a event filter implementation
type Filter struct {
	filter            *filter.Filter
	ignoreTxnCommitTs []uint64
	ddlWhitelist      []model.ActionType
	config            *ReplicaConfig
}

// ReplicaConfig represents some addition replication config for a changefeed
type ReplicaConfig struct {
	DDLWhitelist        []model.ActionType `toml:"ddl-white-list" json:"ddl-white-list"`
	FilterCaseSensitive bool               `toml:"filter-case-sensitive" json:"filter-case-sensitive"`
	FilterRules         *filter.Rules      `toml:"filter-rules" json:"filter-rules"`
	IgnoreTxnCommitTs   []uint64           `toml:"ignore-txn-commit-ts" json:"ignore-txn-commit-ts"`
}

// Clone clones a ReplicaConfig
func (c *ReplicaConfig) Clone() *ReplicaConfig {
	r := new(ReplicaConfig)
	if c.DDLWhitelist != nil {
		r.DDLWhitelist = make([]model.ActionType, len(c.DDLWhitelist))
		copy(r.DDLWhitelist, c.DDLWhitelist)
	}
	r.FilterCaseSensitive = c.FilterCaseSensitive
	r.FilterRules = cloneFilterRules(c.FilterRules)
	if c.IgnoreTxnCommitTs != nil {
		r.IgnoreTxnCommitTs = make([]uint64, len(c.IgnoreTxnCommitTs))
		copy(r.IgnoreTxnCommitTs, c.IgnoreTxnCommitTs)
	}
	return r
}

func cloneFilterRules(c *filter.Rules) *filter.Rules {
	if c == nil {
		return nil
	}
	r := new(filter.Rules)
	if c.DoTables != nil {
		r.DoTables = make([]*filter.Table, len(c.DoTables))
		for i, v := range c.DoTables {
			r.DoTables[i] = &filter.Table{Schema: v.Schema, Name: v.Name}
		}
	}
	if c.DoDBs != nil {
		r.DoDBs = make([]string, len(c.DoDBs))
		copy(r.DoDBs, c.DoDBs)
	}

	if c.IgnoreTables != nil {
		r.IgnoreTables = make([]*filter.Table, len(c.IgnoreTables))
		for i, v := range c.IgnoreTables {
			r.IgnoreTables[i] = &filter.Table{Schema: v.Schema, Name: v.Name}
		}
	}

	if c.IgnoreDBs != nil {
		r.IgnoreDBs = make([]string, len(c.IgnoreDBs))
		copy(r.IgnoreDBs, c.IgnoreDBs)
	}
	return r
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
		config:            config,
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

// Clone clones the Filter
func (f *Filter) Clone() *Filter {
	filter, err := NewFilter(f.config.Clone())
	if err != nil {
		log.Fatal("err must be nil", zap.Error(err))
	}
	return filter
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
