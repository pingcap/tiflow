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
	"fmt"
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

// NewBinlogEvent returns a binlog event filter built from one rule set,
// such as one task configuration's or one source configuration's rules.
//
// Rules within that set may share a (schema-pattern, table-pattern) key
// only when all of them are Ignore rules: they narrow what gets skipped,
// so applying all of them is unambiguous. Sharing a key with any other
// action is rejected, rather than silently keeping one of the rules.
//
// A rule that cannot be used at all, such as one naming an unknown event
// or carrying an uncompilable sql-pattern, is skipped with a warning.
// Configurations holding such a rule have always loaded, and must keep
// loading, since they may already be persisted.
func NewBinlogEvent(caseSensitive bool, rules []*BinlogEventRule) (*BinlogEvent, error) {
	b := &BinlogEvent{
		Selector:      selector.NewTrieSelector(),
		caseSensitive: caseSensitive,
	}

	for _, rule := range rules {
		conflict, err := b.addRuleComposingIgnores(rule)
		if conflict {
			return nil, err
		}
		if err != nil {
			log.Warn("skip unusable binlog event rule", zap.Any("rule", rule), zap.Error(err))
		}
	}

	return b, nil
}

// MergeLowerPriorityRules merges rules into b, where b's existing rules take
// precedence: every rule whose key b already uses is skipped, whatever its
// action. Rules at the remaining keys are added, with same-key Ignore rules
// composed as in NewBinlogEvent. Both returned slices follow the order of
// rules.
func (b *BinlogEvent) MergeLowerPriorityRules(rules []*BinlogEventRule) (added, skipped []*BinlogEventRule, err error) {
	lower, err := NewBinlogEvent(b.caseSensitive, rules)
	if err != nil {
		return nil, nil, err
	}
	// Group by trie node rather than by pattern string: reconstructed
	// patterns do not always round-trip, so "[ab]" and "[ba]" name one key
	// but read back as two.
	groupOf := lower.ruleGroups()

	claimed := make(map[int]bool, len(groupOf))
	for _, rule := range rules {
		group, ok := groupOf[rule]
		if !ok {
			continue // skipped by NewBinlogEvent above
		}

		if claimed[group] {
			// Already proven free of b's rules, and lower validated the
			// group as composable, so this cannot conflict.
			if err = b.Insert(rule.SchemaPattern, rule.TablePattern, rule, selector.Append); err != nil {
				return nil, nil, errors.Annotatef(err, "add rule %+v into binlog event filter", rule)
			}
			added = append(added, rule)
			continue
		}

		// Let the trie decide whether b already uses this key.
		err = b.AddRule(rule)
		switch {
		case err == nil:
			claimed[group] = true
			added = append(added, rule)
		case errors.IsAlreadyExists(errors.Cause(err)):
			skipped = append(skipped, rule)
		default:
			return nil, nil, err
		}
	}

	return added, skipped, nil
}

// ruleGroups maps each rule in b to an id shared by exactly the rules
// stored under the same key.
func (b *BinlogEvent) ruleGroups() map[*BinlogEventRule]int {
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

// AddRule adds a rule into binlog event filter. A rule already present at
// the same (schema-pattern, table-pattern) key is always reported as an
// error here, regardless of action. See NewBinlogEvent for the same-key
// Ignore composition applied when building a filter from a whole rule set,
// and MergeLowerPriorityRules for merging one set into another.
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

// addRuleComposingIgnores behaves like AddRule, except that a collision with
// an existing same-key rule composes same-key Ignore rules instead of always
// failing. It reports whether the error, if any, is such a collision that
// cannot be composed.
func (b *BinlogEvent) addRuleComposingIgnores(rule *BinlogEventRule) (conflict bool, err error) {
	err = b.AddRule(rule)
	if err == nil {
		return false, nil
	}
	if !errors.IsAlreadyExists(errors.Cause(err)) {
		return false, err
	}

	// AddRule already validated and normalized rule (case-folding included)
	// before hitting the collision, so rule.SchemaPattern/TablePattern below
	// are the same key AddRule just failed to insert.
	existing := b.rulesAtKey(rule.SchemaPattern, rule.TablePattern)
	// An empty result means the lookup disagrees with the trie about this
	// key, since AddRule just found it occupied. Composing would then risk
	// joining rules we cannot see, so refuse instead.
	if len(existing) == 0 || rule.Action != Ignore || !allIgnoreRules(existing) {
		return true, errors.Errorf(
			"rule %s conflicts with existing rule(s) %s at the same key (schema-pattern=%q, table-pattern=%q): "+
				"multiple rules at the same key are only supported when all of them are Ignore rules",
			formatRule(rule), formatRules(existing), rule.SchemaPattern, rule.TablePattern)
	}

	if err = b.Insert(rule.SchemaPattern, rule.TablePattern, rule, selector.Append); err != nil {
		return false, errors.Annotatef(err, "add rule %+v into binlog event filter", rule)
	}

	return false, nil
}

// rulesAtKey returns the rules currently stored at the exact
// (schemaPattern, tablePattern) key, without wildcard matching against real
// schema/table values.
func (b *BinlogEvent) rulesAtKey(schemaPattern, tablePattern string) []*BinlogEventRule {
	schemaRules, tableRules := b.AllRules()

	var raw []any
	if len(tablePattern) == 0 {
		raw = schemaRules[schemaPattern]
	} else {
		raw = tableRules[schemaPattern][tablePattern]
	}

	rules := make([]*BinlogEventRule, 0, len(raw))
	for _, r := range raw {
		if br, ok := r.(*BinlogEventRule); ok {
			rules = append(rules, br)
		}
	}
	return rules
}

func allIgnoreRules(rules []*BinlogEventRule) bool {
	for _, r := range rules {
		if r.Action != Ignore {
			return false
		}
	}
	return true
}

// formatRule renders a rule's user-facing fields for error messages,
// skipping the unexported compiled regexp.
func formatRule(rule *BinlogEventRule) string {
	return fmt.Sprintf("{schema-pattern:%s table-pattern:%s events:%v sql-pattern:%v action:%s}",
		rule.SchemaPattern, rule.TablePattern, rule.Events, rule.SQLPattern, rule.Action)
}

func formatRules(rules []*BinlogEventRule) string {
	parts := make([]string, 0, len(rules))
	for _, r := range rules {
		parts = append(parts, formatRule(r))
	}
	return "[" + strings.Join(parts, ", ") + "]"
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
