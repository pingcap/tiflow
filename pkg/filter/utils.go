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

package filter

import (
	"fmt"

	bf "github.com/pingcap/tidb-tools/pkg/binlog-filter"
	"github.com/pingcap/tidb/parser"
	timodel "github.com/pingcap/tidb/parser/model"
	tifilter "github.com/pingcap/tidb/util/filter"
	tfilter "github.com/pingcap/tidb/util/table-filter"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
)

// isSysSchema returns true if the given schema is a system schema
func isSysSchema(db string) bool {
	return tifilter.IsSystemSchema(db)
}

// VerifyTableRules checks the table filter rules in the configuration
// and returns an invalid rule error if the verification fails,
// otherwise it will return a table filter.
func VerifyTableRules(cfg *config.FilterConfig) (tfilter.Filter, error) {
	var f tfilter.Filter
	var err error
	if len(cfg.Rules) == 0 && cfg.MySQLReplicationRules != nil {
		f, err = tfilter.ParseMySQLReplicationRules(cfg.MySQLReplicationRules)
	} else {
		rules := cfg.Rules
		if len(rules) == 0 {
			rules = []string{"*.*"}
		}
		f, err = tfilter.Parse(rules)
	}
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrFilterRuleInvalid, err, cfg)
	}

	return f, nil
}

// ddlToEventType get event type from ddl query.
func ddlToEventType(p *parser.Parser, query string, jobType timodel.ActionType) (bf.EventType, error) {
	// Since `Parser` will return a AlterTable type `ast.StmtNode` for table partition related DDL,
	// we need to check the ActionType of a ddl at first.
	switch jobType {
	case timodel.ActionAddTablePartition:
		return bf.AddTablePartition, nil
	case timodel.ActionDropTablePartition:
		return bf.DropTablePartition, nil
	case timodel.ActionTruncateTablePartition:
		return bf.TruncateTablePartition, nil
	}
	stmt, err := p.ParseOneStmt(query, "", "")
	if err != nil {
		return bf.NullEvent, cerror.WrapError(cerror.ErrConvertDDLToEventTypeFailed, err, query)
	}
	et := bf.AstToDDLEvent(stmt)
	// `Parser` will return a `AlterTable` type `ast.StmtNode` for a query like:
	// `alter table t1 add index (xxx)` and will return a `CreateIndex` type
	// `ast.StmtNode` for a query like: `create index i on t1 (xxx)`.
	// So we cast index related DDL to `AlterTable` event type for the sake of simplicity.
	switch et {
	case bf.DropIndex:
		return bf.AlterTable, nil
	case bf.CreateIndex:
		return bf.AlterTable, nil
	}
	return et, nil
}

// SupportedEventTypes returns the supported event types.
func SupportedEventTypes() []bf.EventType {
	return supportedEventTypes
}

func completeExpression(suffix string) string {
	if suffix == "" {
		return suffix
	}
	return fmt.Sprintf("select * from t where %s", suffix)
}
