// Copyright 2018 PingCAP, Inc.
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
	"regexp"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	selector "github.com/pingcap/tidb/pkg/util/table-rule-selector"
	"go.uber.org/zap"
)

// ActionType indicates how to handle matched items
type ActionType string

// show that how to handle rules
const (
	Ignore ActionType = "Ignore"
	Do     ActionType = "Do"
	Error  ActionType = "Error"
)

// EventType is DML/DDL Event type
type EventType string

// show DML/DDL Events
const (
	ddl             EventType = "ddl"
	dml             EventType = "dml"
	incompatibleDDL EventType = "incompatible DDL"

	// it indicates all dml/ddl events in rule
	AllEvent EventType = "all"
	AllDDL   EventType = "all ddl"
	AllDML   EventType = "all dml"

	// it indicates no any dml/ddl events in rule,
	// and equals empty rule.DDLEvent/DMLEvent
	NoneEvent EventType = "none"
	NoneDDL   EventType = "none ddl"
	NoneDML   EventType = "none dml"

	InsertEvent EventType = "insert"
	UpdateEvent EventType = "update"
	DeleteEvent EventType = "delete"

	CreateDatabase EventType = "create database"
	DropDatabase   EventType = "drop database"
	AlterDatabase  EventType = "alter database"
	CreateTable    EventType = "create table"
	DropTable      EventType = "drop table"
	TruncateTable  EventType = "truncate table"
	RenameTable    EventType = "rename table"
	CreateIndex    EventType = "create index"
	DropIndex      EventType = "drop index"
	CreateView     EventType = "create view"
	DropView       EventType = "drop view"
	AlterTable     EventType = "alter table"

	CreateSchema EventType = "create schema" // alias of CreateDatabase
	DropSchema   EventType = "drop schema"   // alias of DropDatabase
	AlterSchema  EventType = "alter schema"  // alias of AlterDatabase

	AddTablePartition      EventType = "add table partition"
	DropTablePartition     EventType = "drop table partition"
	TruncateTablePartition EventType = "truncate table partition"
	// if need, add more	AlertTableOption     = "alert table option"

	IncompatibleDDLChanges EventType = "incompatible ddl changes"
	ValueRangeDecrease     EventType = "value range decrease"
	PrecisionDecrease      EventType = "precision decrease"
	ModifyColumn           EventType = "modify column"
	RenameColumn           EventType = "rename column"
	RenameIndex            EventType = "rename index"
	DropColumn             EventType = "drop column"
	DropPrimaryKey         EventType = "drop primary key"
	DropUniqueKey          EventType = "drop unique key"
	ModifyDefaultValue     EventType = "modify default value"
	ModifyConstraint       EventType = "modify constraint"
	ModifyColumnsOrder     EventType = "modify columns order"
	ModifyCharset          EventType = "modify charset"
	ModifyCollation        EventType = "modify collation"
	RemoveAutoIncrement    EventType = "remove auto increment"
	ModifyStorageEngine    EventType = "modify storage engine"
	ReorganizePartition    EventType = "reorganize table partition"
	RebuildPartition       EventType = "rebuild table partition"
	CoalescePartition      EventType = "coalesce table partition"
	SplitPartition         EventType = "split table partition"
	ExchangePartition      EventType = "exchange table partition"

	ModifySchemaCharsetAndCollate EventType = "modify schema charset and collate"
	ModifyTableCharsetAndCollate  EventType = "modify table charset and collate"
	ModifyTableComment            EventType = "modify table comment"
	RecoverTable                  EventType = "recover table"
	AlterTablePartitioning        EventType = "alter table partitioning"
	RemovePartitioning            EventType = "remove table partitioning"
	AddColumn                     EventType = "add column"
	SetDefaultValue               EventType = "set default value"
	RebaseAutoID                  EventType = "rebase auto id"
	AddPrimaryKey                 EventType = "add primary key"
	AlterIndexVisibility          EventType = "alter index visibility"
	AlterTTLInfo                  EventType = "alter ttl info"
	AlterTTLRemove                EventType = "alter ttl remove"
	MultiSchemaChange             EventType = "multi schema change"

	// NullEvent is used to represents unsupported ddl event type when we
	// convert a ast.StmtNode or a string to EventType.
	NullEvent EventType = ""
)

// ClassifyEvent classify event into dml/ddl
func ClassifyEvent(event EventType) (EventType, error) {
	switch event {
	case InsertEvent,
		UpdateEvent,
		DeleteEvent:
		return dml, nil
	case CreateDatabase,
		AlterDatabase,
		AlterSchema,
		CreateTable,
		CreateIndex,
		CreateView,
		DropView,
		AlterTable,
		CreateSchema,
		AddTablePartition:
		return ddl, nil
	case NullEvent:
		return NullEvent, nil
	case ValueRangeDecrease,
		PrecisionDecrease,
		ModifyColumn,
		RenameColumn,
		RenameIndex,
		DropColumn,
		DropPrimaryKey,
		DropUniqueKey,
		ModifyDefaultValue,
		ModifyConstraint,
		ModifyColumnsOrder,
		ModifyCharset,
		ModifyCollation,
		RemoveAutoIncrement,
		ModifyStorageEngine,
		ReorganizePartition,
		RebuildPartition,
		CoalescePartition,
		SplitPartition,
		ExchangePartition,

		DropDatabase,
		DropTable,
		DropIndex,
		RenameTable,
		TruncateTable,
		DropSchema,
		DropTablePartition,
		TruncateTablePartition,

		ModifySchemaCharsetAndCollate,
		ModifyTableCharsetAndCollate,
		ModifyTableComment,
		RecoverTable,
		AlterTablePartitioning,
		RemovePartitioning,
		AddColumn,
		SetDefaultValue,
		RebaseAutoID,
		AddPrimaryKey,
		AlterIndexVisibility,
		AlterTTLInfo,
		AlterTTLRemove,
		MultiSchemaChange:
		return incompatibleDDL, nil
	default:
		return NullEvent, errors.NotValidf("event type %s", event)
	}
}

// BinlogEventRule is a rule to filter binlog events
type BinlogEventRule struct {
	SchemaPattern string      `json:"schema-pattern" toml:"schema-pattern" yaml:"schema-pattern"`
	TablePattern  string      `json:"table-pattern" toml:"table-pattern" yaml:"table-pattern"`
	Events        []EventType `json:"events" toml:"events" yaml:"events"`
	SQLPattern    []string    `json:"sql-pattern" toml:"sql-pattern" yaml:"sql-pattern"` // regular expression
	sqlRegularExp *regexp.Regexp

	Action ActionType `json:"action" toml:"action" yaml:"action"`
}

// ToLower covert schema/table pattern to lower case
func (b *BinlogEventRule) ToLower() {
	b.SchemaPattern = strings.ToLower(b.SchemaPattern)
	b.TablePattern = strings.ToLower(b.TablePattern)
}

// Valid checks validity of rule.
func (b *BinlogEventRule) Valid() error {
	if len(b.SQLPattern) > 0 {
		reg, err := regexp.Compile("(?i)" + strings.Join(b.SQLPattern, "|"))
		if err != nil {
			return errors.Annotatef(err, "compile regular expression %+v", b.SQLPattern)
		}
		b.sqlRegularExp = reg
	}

	if b.Action != Do && b.Action != Ignore && b.Action != Error {
		return errors.Errorf("action of binlog event rule %+v should not be empty", b)
	}

	for i := range b.Events {
		et, err := toEventType(string(b.Events[i]))
		if err != nil {
			return errors.NotValidf("event type %s", b.Events[i])
		}
		b.Events[i] = et
	}
	return nil
}

// BinlogEvent filters binlog events by given rules
type BinlogEvent struct {
	selector.Selector

	caseSensitive bool
}

// NewBinlogEvent returns a binlog event filter
func NewBinlogEvent(caseSensitive bool, rules []*BinlogEventRule) (*BinlogEvent, error) {
	b := &BinlogEvent{
		Selector:      selector.NewTrieSelector(),
		caseSensitive: caseSensitive,
	}

	for _, rule := range rules {
		if err := b.AddRule(rule); err != nil {
			log.Error("invalid binlog event rule", zap.Any("rule", rule), zap.Error(err))
		}
	}

	return b, nil
}

// AddRule adds a rule into binlog event filter
func (b *BinlogEvent) AddRule(rule *BinlogEventRule) error {
	if b == nil || rule == nil {
		return nil
	}
	err := rule.Valid()
	if err != nil {
		return errors.Trace(err)
	}
	if !b.caseSensitive {
		rule.ToLower()
	}

	err = b.Insert(rule.SchemaPattern, rule.TablePattern, rule, selector.Insert)
	if err != nil {
		return errors.Annotatef(err, "add rule %+v into binlog event filter", rule)
	}

	return nil
}

// UpdateRule updates binlog event filter rule
func (b *BinlogEvent) UpdateRule(rule *BinlogEventRule) error {
	if b == nil || rule == nil {
		return nil
	}
	err := rule.Valid()
	if err != nil {
		return errors.Trace(err)
	}
	if !b.caseSensitive {
		rule.ToLower()
	}

	err = b.Insert(rule.SchemaPattern, rule.TablePattern, rule, selector.Replace)
	if err != nil {
		return errors.Annotatef(err, "update rule %+v into binlog event filter", rule)
	}

	return nil
}

// RemoveRule removes a rule from binlog event filter
func (b *BinlogEvent) RemoveRule(rule *BinlogEventRule) error {
	if b == nil || rule == nil {
		return nil
	}
	if !b.caseSensitive {
		rule.ToLower()
	}

	err := b.Remove(rule.SchemaPattern, rule.TablePattern)
	if err != nil {
		return errors.Annotatef(err, "remove rule %+v", rule)
	}

	return nil
}

// Filter filters events or queries by given rules
// returns action and error
func (b *BinlogEvent) Filter(schema, table string, event EventType, rawQuery string) (ActionType, error) {
	if b == nil {
		return Do, nil
	}

	tp, err := ClassifyEvent(event)
	if err != nil {
		return Ignore, errors.Trace(err)
	}

	schemaL, tableL := schema, table
	if !b.caseSensitive {
		schemaL, tableL = strings.ToLower(schema), strings.ToLower(table)
	}

	rules := b.Match(schemaL, tableL)
	if len(rules) == 0 {
		return Do, nil
	}

	for _, rule := range rules {
		binlogEventRule, ok := rule.(*BinlogEventRule)
		if !ok {
			return "", errors.NotValidf("rule %+v", rule)
		}

		if tp != NullEvent {
			matched := b.matchEvent(tp, event, binlogEventRule.Events)

			if matched {
				// ignore has highest priority
				if binlogEventRule.Action == Ignore {
					return Ignore, nil
				}
				if binlogEventRule.Action == Error {
					return Error, nil
				}
			} else {
				if binlogEventRule.Action == Do {
					return Ignore, nil
				}
			}
		}

		if len(rawQuery) > 0 {
			if len(binlogEventRule.SQLPattern) == 0 {
				// sql pattern is disabled , just continue
				continue
			}

			matched := binlogEventRule.sqlRegularExp.FindStringIndex(rawQuery) != nil
			if matched {
				// Ignore has highest priority
				if binlogEventRule.Action == Ignore {
					return Ignore, nil
				}
				if binlogEventRule.Action == Error {
					return Error, nil
				}
			} else {
				if binlogEventRule.Action == Do {
					return Ignore, nil
				}
			}
		}
	}

	return Do, nil
}

func (b *BinlogEvent) matchEvent(tp, event EventType, rules []EventType) bool {
	for _, rule := range rules {
		if rule == AllEvent {
			return true
		}

		if rule == NoneEvent {
			return false
		}

		if tp == ddl || tp == incompatibleDDL {
			if rule == AllDDL {
				return true
			}

			if rule == NoneDDL {
				return false
			}
		}

		if tp == dml {
			if rule == AllDML {
				return true
			}

			if rule == NoneDML {
				return false
			}
		}

		if tp == incompatibleDDL {
			if rule == IncompatibleDDLChanges {
				return true
			}
		}

		if rule == event {
			return true
		}
	}

	return false
}
