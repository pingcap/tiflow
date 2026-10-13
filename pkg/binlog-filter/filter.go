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
	AddForeignKey          EventType = "add foreign key"
	DropForeignKey         EventType = "drop foreign key"
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

		AddForeignKey,
		DropForeignKey,

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

// strictness says how much of what a build cannot apply it refuses outright,
// rather than logging the problem and leaving the rule out of the filter.
type strictness int

const (
	// tolerateAll never fails on rule content. Paths loading an
	// already-persisted configuration need this.
	tolerateAll strictness = iota
	// rejectUnusable fails on a rule that cannot be applied at all, such as
	// one naming an unknown event or carrying an uncompilable sql-pattern.
	rejectUnusable
	// rejectAll also fails on a rule that cannot compose with the rule
	// already at its key.
	rejectAll
)

// NewBinlogEvent returns a binlog event filter built from rules. Rules sharing
// a (schema-pattern, table-pattern) key all apply when all of them are Ignore,
// since each one only narrows what gets skipped. Any rule the filter cannot
// apply is left out with a warning.
//
// Leaving a rule out rather than failing is deliberate: every path that loads
// a persisted configuration builds its filter here, and those rules were
// never validated back when the configuration was accepted. Submission paths
// call ValidateRules instead.
func NewBinlogEvent(caseSensitive bool, rules []*BinlogEventRule) (*BinlogEvent, error) {
	return newBinlogEvent(caseSensitive, rules, tolerateAll)
}

// ValidateRules returns an error describing the first rule NewBinlogEvent
// would leave out. Only call it where a configuration is being submitted: one
// already persisted must stay loadable.
func ValidateRules(caseSensitive bool, rules []*BinlogEventRule) error {
	_, err := newBinlogEvent(caseSensitive, rules, rejectAll)
	return err
}

func newBinlogEvent(caseSensitive bool, rules []*BinlogEventRule, strict strictness) (*BinlogEvent, error) {
	b := &BinlogEvent{
		Selector:      selector.NewTrieSelector(),
		caseSensitive: caseSensitive,
	}

	rulesAtKey := make(map[[2]string][]*BinlogEventRule, len(rules))
	for _, rule := range rules {
		if rule == nil {
			continue
		}

		err := b.AddRule(rule)
		// AddRule case-folded rule before inserting, so this is that key.
		key := [2]string{rule.SchemaPattern, rule.TablePattern}

		if err == nil {
			rulesAtKey[key] = append(rulesAtKey[key], rule)
			continue
		}

		if !errors.IsAlreadyExists(errors.Cause(err)) {
			if strict >= rejectUnusable {
				return nil, err
			}
			log.Warn("skip unusable binlog event rule", zap.Any("rule", rule), zap.Error(err))
			continue
		}

		existing := rulesAtKey[key]
		// Empty means AddRule collided with a key spelled differently.
		if len(existing) == 0 || rule.Action != Ignore || !allIgnoreRules(existing) {
			if strict >= rejectAll {
				return nil, errors.Errorf(
					"multiple rules at the same key (schema-pattern=%q, table-pattern=%q) "+
						"are only supported when all of them are Ignore rules",
					rule.SchemaPattern, rule.TablePattern)
			}
			log.Warn("leave out binlog event rule, it cannot compose with the rule already at its key",
				zap.Any("rule", rule))
			continue
		}
		if err = b.Insert(rule.SchemaPattern, rule.TablePattern, rule, selector.Append); err != nil {
			return nil, errors.Annotatef(err, "add rule %+v into binlog event filter", rule)
		}
		rulesAtKey[key] = append(rulesAtKey[key], rule)
	}

	return b, nil
}

// MergeLowerPriorityRules merges rules into b, where b's existing rules win
// the keys they occupy: rules colliding there are overwritten, and the rest
// are added, composing same-key Ignore rules as NewBinlogEvent does. Both
// returned slices follow rules' order. Unlike NewBinlogEvent, a rule that
// cannot be used at all fails the merge rather than being skipped, so that a
// malformed rule is not quietly missing from the result.
func (b *BinlogEvent) MergeLowerPriorityRules(rules []*BinlogEventRule) (added, overwritten []*BinlogEventRule, err error) {
	lower, err := newBinlogEvent(b.caseSensitive, rules, rejectUnusable)
	if err != nil {
		return nil, nil, err
	}
	groupOf := lower.groupRules()

	addedGroups := make(map[int]bool, len(groupOf))
	for _, rule := range rules {
		group, ok := groupOf[rule]
		if !ok {
			continue // nil, or left out as non-composable
		}

		if addedGroups[group] {
			// b has none at this key, and lower proved the group composable.
			if err = b.Insert(rule.SchemaPattern, rule.TablePattern, rule, selector.Append); err != nil {
				return nil, nil, errors.Annotatef(err, "add rule %+v into binlog event filter", rule)
			}
			added = append(added, rule)
			continue
		}

		err = b.AddRule(rule)
		switch {
		case err == nil:
			addedGroups[group] = true
			added = append(added, rule)
		case errors.IsAlreadyExists(errors.Cause(err)):
			overwritten = append(overwritten, rule)
		default:
			return nil, nil, err
		}
	}

	return added, overwritten, nil
}

// groupRules maps rules to an id shared by exactly the rules stored under the
// same key. The grouping comes from the trie because a pattern string does not
// name a key uniquely: "[ab]" and "[ba]" land on one node.
func (b *BinlogEvent) groupRules() map[*BinlogEventRule]int {
	schemaRules, tableRules := b.AllRules()

	groups := make(map[*BinlogEventRule]int)
	group := 0
	collect := func(raw []any) {
		for _, r := range raw {
			if rule, ok := r.(*BinlogEventRule); ok {
				groups[rule] = group
			}
		}
		group++
	}
	for _, raw := range schemaRules {
		collect(raw)
	}
	for _, tables := range tableRules {
		for _, raw := range tables {
			collect(raw)
		}
	}
	return groups
}

// AddRule adds a rule into binlog event filter. Unlike NewBinlogEvent and
// MergeLowerPriorityRules it never composes: a rule already present at the
// same (schema-pattern, table-pattern) key is always an error.
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

	if err = b.Insert(rule.SchemaPattern, rule.TablePattern, rule, selector.Insert); err != nil {
		return errors.Annotatef(err, "add rule %+v into binlog event filter", rule)
	}

	return nil
}

func allIgnoreRules(rules []*BinlogEventRule) bool {
	for _, r := range rules {
		if r.Action != Ignore {
			return false
		}
	}
	return true
}

// UpdateRule updates binlog event filter rule. This replaces every rule at
// the (schema-pattern, table-pattern) key with just rule, including sibling
// Ignore rules composed there.
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

// RemoveRule removes a rule from binlog event filter. This clears every
// rule at the (schema-pattern, table-pattern) key, not just rule itself,
// including sibling Ignore rules composed there.
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
