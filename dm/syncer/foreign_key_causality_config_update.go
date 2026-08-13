// Copyright 2026 PingCAP, Inc.
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

package syncer

import (
	"reflect"
	"sort"
	"strings"

	"github.com/pingcap/tidb/pkg/util/filter"
	router "github.com/pingcap/tidb/pkg/util/table-router"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	bf "github.com/pingcap/tiflow/pkg/binlog-filter"
)

func (s *Syncer) checkForeignKeyCausalityConfigUpdate(newCfg *config.SubTaskConfig) error {
	oldNeedFKCausality := config.IsForeignKeyChecksEnabled(s.cfg.To.Session) && s.cfg.WorkerCount > 1
	newNeedFKCausality := config.IsForeignKeyChecksEnabled(newCfg.To.Session) && newCfg.WorkerCount > 1
	if !oldNeedFKCausality && !newNeedFKCausality {
		return nil
	}

	if s.cfg.WorkerCount != newCfg.WorkerCount {
		return s.newForeignKeyCausalityConfigUpdateError("worker-count")
	}
	if s.cfg.CaseSensitive != newCfg.CaseSensitive {
		return s.newForeignKeyCausalityConfigUpdateError("case-sensitive")
	}
	// Compare route/filter semantics through normalized snapshots instead of
	// runtime objects that may contain compiled regex or cache fields.
	if !sameRouteRules(s.cfg.CaseSensitive, s.cfg.RouteRules, newCfg.RouteRules) {
		return s.newForeignKeyCausalityConfigUpdateError("route rules")
	}
	if !sameTableFilterRules(s.cfg.CaseSensitive, s.cfg.BAList, newCfg.BAList) {
		return s.newForeignKeyCausalityConfigUpdateError("block-allow-list")
	}
	if !sameTableFilterRules(s.cfg.CaseSensitive, s.cfg.BWList, newCfg.BWList) {
		return s.newForeignKeyCausalityConfigUpdateError("black-white-list")
	}
	if !sameBinlogFilterRules(s.cfg.CaseSensitive, s.cfg.FilterRules, newCfg.FilterRules) {
		return s.newForeignKeyCausalityConfigUpdateError("binlog filter rules")
	}
	if config.IsForeignKeyChecksEnabled(s.cfg.To.Session) != config.IsForeignKeyChecksEnabled(newCfg.To.Session) {
		return s.newForeignKeyCausalityConfigUpdateError("foreign_key_checks")
	}
	return nil
}

func (s *Syncer) newForeignKeyCausalityConfigUpdateError(field string) error {
	return terror.ErrWorkerUpdateSubTaskConfig.Generatef(
		"can't update %s when foreign_key_checks=1 and worker-count>1, task: %s; please stop and restart the task with the new config",
		field,
		s.cfg.Name,
	)
}

// The snapshot types below keep only public config fields that affect FK
// causality hot-update semantics. Runtime structs can carry compiled regex or
// cache fields, so comparing them directly would make this check depend on
// runtime state instead of config semantics.
type routeRuleSnapshot struct {
	Nil             bool
	TableExtractor  *routeExtractorSnapshot
	SchemaExtractor *routeExtractorSnapshot
	SourceExtractor *routeExtractorSnapshot
	SchemaPattern   string
	TablePattern    string
	TargetSchema    string
	TargetTable     string
}

type routeExtractorSnapshot struct {
	TargetColumn string
	Regexp       string
}

type tableFilterRulesSnapshot struct {
	DoTables     []tableNameSnapshot
	DoDBs        []string
	IgnoreTables []tableNameSnapshot
	IgnoreDBs    []string
}

type tableNameSnapshot struct {
	Schema string
	Name   string
}

type binlogFilterRuleSnapshot struct {
	Nil           bool
	SchemaPattern string
	TablePattern  string
	Events        []string
	SQLPattern    []string
	Action        bf.ActionType
}

func sameRouteRules(caseSensitive bool, a, b []*router.TableRule) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}
	return reflect.DeepEqual(
		normalizeRouteRulesForCompare(caseSensitive, a),
		normalizeRouteRulesForCompare(caseSensitive, b),
	)
}

func normalizeRouteRulesForCompare(caseSensitive bool, rules []*router.TableRule) []routeRuleSnapshot {
	if len(rules) == 0 {
		return nil
	}
	result := make([]routeRuleSnapshot, 0, len(rules))
	for _, rule := range rules {
		if rule == nil {
			result = append(result, routeRuleSnapshot{Nil: true})
			continue
		}
		schemaPattern, tablePattern := rule.SchemaPattern, rule.TablePattern
		if !caseSensitive {
			schemaPattern = strings.ToLower(schemaPattern)
			tablePattern = strings.ToLower(tablePattern)
		}
		result = append(result, routeRuleSnapshot{
			TableExtractor:  tableExtractorSnapshot(rule.TableExtractor),
			SchemaExtractor: schemaExtractorSnapshot(rule.SchemaExtractor),
			SourceExtractor: sourceExtractorSnapshot(rule.SourceExtractor),
			SchemaPattern:   schemaPattern,
			TablePattern:    tablePattern,
			TargetSchema:    rule.TargetSchema,
			TargetTable:     rule.TargetTable,
		})
	}
	return result
}

func tableExtractorSnapshot(extractor *router.TableExtractor) *routeExtractorSnapshot {
	if extractor == nil {
		return nil
	}
	return &routeExtractorSnapshot{
		TargetColumn: extractor.TargetColumn,
		Regexp:       extractor.TableRegexp,
	}
}

func schemaExtractorSnapshot(extractor *router.SchemaExtractor) *routeExtractorSnapshot {
	if extractor == nil {
		return nil
	}
	return &routeExtractorSnapshot{
		TargetColumn: extractor.TargetColumn,
		Regexp:       extractor.SchemaRegexp,
	}
}

func sourceExtractorSnapshot(extractor *router.SourceExtractor) *routeExtractorSnapshot {
	if extractor == nil {
		return nil
	}
	return &routeExtractorSnapshot{
		TargetColumn: extractor.TargetColumn,
		Regexp:       extractor.SourceRegexp,
	}
}

func sameTableFilterRules(caseSensitive bool, a, b *filter.Rules) bool {
	if isEmptyTableFilterRules(a) && isEmptyTableFilterRules(b) {
		return true
	}
	return reflect.DeepEqual(
		normalizeTableFilterRulesForCompare(caseSensitive, a),
		normalizeTableFilterRulesForCompare(caseSensitive, b),
	)
}

func isEmptyTableFilterRules(r *filter.Rules) bool {
	return r == nil ||
		len(r.DoTables) == 0 &&
			len(r.DoDBs) == 0 &&
			len(r.IgnoreTables) == 0 &&
			len(r.IgnoreDBs) == 0
}

func normalizeTableFilterRulesForCompare(caseSensitive bool, rules *filter.Rules) tableFilterRulesSnapshot {
	if isEmptyTableFilterRules(rules) {
		return tableFilterRulesSnapshot{}
	}
	doTables := normalizeFilterTablesForCompare(caseSensitive, rules.DoTables)
	sortTableNamesForCompare(doTables)
	doDBs := normalizeFilterSchemasForCompare(caseSensitive, rules.DoDBs)
	sort.Strings(doDBs)
	ignoreTables := normalizeFilterTablesForCompare(caseSensitive, rules.IgnoreTables)
	sortTableNamesForCompare(ignoreTables)
	ignoreDBs := normalizeFilterSchemasForCompare(caseSensitive, rules.IgnoreDBs)
	sort.Strings(ignoreDBs)

	return tableFilterRulesSnapshot{
		DoTables:     doTables,
		DoDBs:        doDBs,
		IgnoreTables: ignoreTables,
		IgnoreDBs:    ignoreDBs,
	}
}

func sortTableNamesForCompare(tables []tableNameSnapshot) {
	sort.Slice(tables, func(i, j int) bool {
		if tables[i].Schema != tables[j].Schema {
			return tables[i].Schema < tables[j].Schema
		}
		return tables[i].Name < tables[j].Name
	})
}

func normalizeFilterTablesForCompare(caseSensitive bool, tables []*filter.Table) []tableNameSnapshot {
	if len(tables) == 0 {
		return nil
	}
	result := make([]tableNameSnapshot, 0, len(tables))
	for _, table := range tables {
		if table == nil {
			result = append(result, tableNameSnapshot{})
			continue
		}
		schema, name := table.Schema, table.Name
		if !caseSensitive {
			schema = strings.ToLower(schema)
			name = strings.ToLower(name)
		}
		result = append(result, tableNameSnapshot{Schema: schema, Name: name})
	}
	return result
}

func normalizeFilterSchemasForCompare(caseSensitive bool, schemas []string) []string {
	if len(schemas) == 0 {
		return nil
	}
	result := make([]string, 0, len(schemas))
	for _, schema := range schemas {
		if !caseSensitive {
			schema = strings.ToLower(schema)
		}
		result = append(result, schema)
	}
	return result
}

func sameBinlogFilterRules(caseSensitive bool, a, b []*bf.BinlogEventRule) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}
	return reflect.DeepEqual(
		normalizeBinlogFilterRulesForCompare(caseSensitive, a),
		normalizeBinlogFilterRulesForCompare(caseSensitive, b),
	)
}

func normalizeBinlogFilterRulesForCompare(caseSensitive bool, rules []*bf.BinlogEventRule) []binlogFilterRuleSnapshot {
	if len(rules) == 0 {
		return nil
	}
	result := make([]binlogFilterRuleSnapshot, 0, len(rules))
	for _, rule := range rules {
		if rule == nil {
			result = append(result, binlogFilterRuleSnapshot{Nil: true})
			continue
		}
		schemaPattern, tablePattern := rule.SchemaPattern, rule.TablePattern
		if !caseSensitive {
			schemaPattern = strings.ToLower(schemaPattern)
			tablePattern = strings.ToLower(tablePattern)
		}
		sqlPattern := append([]string(nil), rule.SQLPattern...)
		sort.Strings(sqlPattern)
		result = append(result, binlogFilterRuleSnapshot{
			SchemaPattern: schemaPattern,
			TablePattern:  tablePattern,
			Events:        normalizeBinlogFilterEventsForCompare(rule.Events),
			SQLPattern:    sqlPattern,
			Action:        rule.Action,
		})
	}
	return result
}

func normalizeBinlogFilterEventsForCompare(events []bf.EventType) []string {
	if len(events) == 0 {
		return nil
	}
	result := make([]string, 0, len(events))
	for _, event := range events {
		result = append(result, strings.ToLower(string(event)))
	}
	if !containsNoneBinlogFilterEvent(result) {
		sort.Strings(result)
	}
	return result
}

func containsNoneBinlogFilterEvent(events []string) bool {
	for _, event := range events {
		switch event {
		case string(bf.NoneEvent), string(bf.NoneDDL), string(bf.NoneDML):
			return true
		}
	}
	return false
}
