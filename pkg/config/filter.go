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

package config

import (
	bf "github.com/pingcap/tidb-tools/pkg/binlog-filter"
	filter "github.com/pingcap/tidb/util/table-filter"
)

// FilterConfig represents filter config for a changefeed
type FilterConfig struct {
	Rules []string `toml:"rules" json:"rules"`
	*filter.MySQLReplicationRules
	IgnoreTxnStartTs []uint64           `toml:"ignore-txn-start-ts" json:"ignore-txn-start-ts"`
	EventFilters     []*EventFilterRule `toml:"event-filters" json:"event-filters"`
}

// EventFilterRule is used by sql event filter and expression filter
type EventFilterRule struct {
	Matcher     []string       `toml:"matcher" json:"matcher"`
	IgnoreEvent []bf.EventType `toml:"ignore-event" json:"ignore-event"`
	// regular expression
	IgnoreSQL []string `toml:"ignore-sql" json:"ignore-sql"`
	// sql expression
	IgnoreInsertValueExpr    string `toml:"ignore-insert-value-expr" json:"ignore-insert-value-expr"`
	IgnoreUpdateNewValueExpr string `toml:"ignore-update-new-value-expr" json:"ignore-update-new-value-expr"`
	IgnoreUpdateOldValueExpr string `toml:"ignore-update-old-value-expr" json:"ignore-update-old-value-expr"`
	IgnoreDeleteValueExpr    string `toml:"ignore-delete-value-expr" json:"ignore-delete-value-expr"`
}
