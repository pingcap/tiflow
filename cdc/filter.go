package cdc

import (
	"strings"

	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/tidb-tools/pkg/filter"
)

type txnFilter struct {
	filter            *filter.Filter
	ignoreTxnCommitTs []uint64
}

func newTxnFilter(config *model.ReplicaConfig) *txnFilter {
	filter := filter.New(config.FilterCaseSensitive, config.FilterRules)
	return &txnFilter{
		filter:            filter,
		ignoreTxnCommitTs: config.IgnoreTxnCommitTs,
	}
}

// ShouldIgnoreTxn returns true is the given txn should be ignored
func (f *txnFilter) ShouldIgnoreTxn(t *model.Txn) bool {
	for _, ignoreTs := range f.ignoreTxnCommitTs {
		if ignoreTs == t.Ts {
			return true
		}
	}
	return false
}

// ShouldIgnoreTable returns true if the specified table should be ignored by this change feed.
// Set `tbl` to an empty string to test against the whole database.
func (f *txnFilter) ShouldIgnoreTable(db, tbl string) bool {
	if IsSysSchema(db) {
		return true
	}
	// TODO: Change filter to support simple check directly
	left := f.filter.ApplyOn([]*filter.Table{{Schema: db, Name: tbl}})
	return len(left) == 0
}

// FilterTxn removes DDL/DMLs that's not wanted by this change feed.
// CDC only supports filtering by database/table now.
func (f *txnFilter) FilterTxn(t *model.Txn) {
	if t.IsDDL() {
		if f.ShouldIgnoreTable(t.DDL.Database, t.DDL.Table) {
			t.DDL = nil
		}
	} else {
		var filteredDMLs []*model.DML
		for _, dml := range t.DMLs {
			if !f.ShouldIgnoreTable(dml.Database, dml.Table) {
				filteredDMLs = append(filteredDMLs, dml)
			}
		}
		t.DMLs = filteredDMLs
	}
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
