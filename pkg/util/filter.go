package util

import (
	"strings"

	"github.com/pingcap/tidb-tools/pkg/filter"
)

// Filter is a event filter implementation
type Filter struct {
	filter            *filter.Filter
	ignoreTxnCommitTs []uint64
}

// ReplicaConfig represents some addition replication config for a changefeed
type ReplicaConfig struct {
	FilterCaseSensitive bool          `toml:"filter-case-sensitive" json:"filter-case-sensitive"`
	FilterRules         *filter.Rules `toml:"filter-rules" json:"filter-rules"`
	IgnoreTxnCommitTs   []uint64      `toml:"ignore-txn-commit-ts" json:"ignore-txn-commit-ts"`
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
