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

package regexprrouter

import (
	"regexp"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-tools/pkg/filter"
	oldrouter "github.com/pingcap/tidb-tools/pkg/table-router"
	"github.com/pingcap/tiflow/dm/pkg/terror"
)

type (
	TableRule  = oldrouter.TableRule
	Table      = filter.Table
	FilterRule = filter.Rules
)

type FilterType = int32

const (
	TblFilter  int32 = 1
	SchmFilter int32 = 2
)

type FilterWrapper struct {
	Filter *filter.Filter
	Type   FilterType
	Target Table

	rawRule *TableRule
}

type RegExprTable struct {
	filters       []*FilterWrapper
	caseSensitive bool
}

func NewRegExprRouter(caseSensitive bool, rules []*TableRule) (*RegExprTable, error) {
	r := &RegExprTable{
		filters:       make([]*FilterWrapper, 0),
		caseSensitive: caseSensitive,
	}
	for _, rule := range rules {
		if err := r.AddRule(rule); err != nil {
			return nil, err
		}
	}
	return r, nil
}

func (r *RegExprTable) AddRule(rule *TableRule) error {
	err := rule.Valid()
	if err != nil {
		return errors.Trace(err)
	}
	if !r.caseSensitive {
		rule.ToLower()
	}
	newFilter := &FilterWrapper{
		rawRule: rule,
	}
	newFilter.Target = Table{
		Schema: rule.TargetSchema,
		Name:   rule.TargetTable,
	}
	if len(rule.TablePattern) == 0 {
		// raw schema rule
		newFilter.Type = SchmFilter
		rawFilter, err := filter.New(r.caseSensitive, &FilterRule{
			DoDBs: []string{rule.SchemaPattern},
		})
		if err != nil {
			return errors.Annotatef(err, "add rule %+v into table router", rule)
		}
		newFilter.Filter = rawFilter
	} else {
		newFilter.Type = TblFilter
		rawFilter, err := filter.New(r.caseSensitive, &FilterRule{
			DoTables: []*Table{
				{
					Schema: rule.SchemaPattern,
					Name:   rule.TablePattern,
				},
			},
			DoDBs: []string{rule.SchemaPattern},
		})
		if err != nil {
			return errors.Annotatef(err, "add rule %+v into table router", rule)
		}
		newFilter.Filter = rawFilter
	}
	r.filters = append(r.filters, newFilter)
	return nil
}

func (r *RegExprTable) Route(schema, table string) (string, string, error) {
	curTable := &Table{
		Schema: schema,
		Name:   table,
	}
	tblRules := make([]*FilterWrapper, 0)
	schmRules := make([]*FilterWrapper, 0)
	for _, filterWrapper := range r.filters {
		if filterWrapper.Filter.Match(curTable) {
			if filterWrapper.Type == TblFilter {
				tblRules = append(tblRules, filterWrapper)
			} else {
				schmRules = append(schmRules, filterWrapper)
			}
		}
	}
	var (
		targetSchema string
		targetTable  string
	)
	if len(table) == 0 || len(tblRules) == 0 {
		// 1. no need to match table or
		// 2. match no table
		if len(schmRules) > 1 {
			return "", "", terror.ErrWorkerRouteTableDupMatch.Generate(schema, table)
		}
		if len(schmRules) == 1 {
			targetSchema, targetTable = schmRules[0].Target.Schema, schmRules[0].Target.Name
		}
	} else {
		if len(tblRules) > 1 {
			return "", "", terror.ErrWorkerRouteTableDupMatch.Generate(schema, table)
		}
		targetSchema, targetTable = tblRules[0].Target.Schema, tblRules[0].Target.Name
	}
	if len(targetSchema) == 0 {
		targetSchema = schema
	}
	if len(targetTable) == 0 {
		targetTable = table
	}
	return targetSchema, targetTable, nil
}

func (r *RegExprTable) AllRules() ([]TableRule, []TableRule) {
	var (
		schmRouteRules  []TableRule
		tableRouteRules []TableRule
	)
	for _, filter := range r.filters {
		if len(filter.rawRule.TablePattern) == 0 {
			schmRouteRules = append(schmRouteRules, *filter.rawRule)
		} else {
			tableRouteRules = append(tableRouteRules, *filter.rawRule)
		}
	}
	return schmRouteRules, tableRouteRules
}

func (r *RegExprTable) FetchExtendColumn(schema, table, source string) ([]string, []string) {
	var cols []string
	var vals []string
	rules := []*FilterWrapper{}
	curTable := &Table{
		Schema: schema,
		Name:   table,
	}
	for _, filter := range r.filters {
		if filter.Filter.Match(curTable) {
			rules = append(rules, filter)
		}
	}
	var (
		schemaRules = make([]*TableRule, 0, len(rules))
		tableRules  = make([]*TableRule, 0, len(rules))
	)
	for i := range rules {
		rule := rules[i].rawRule
		if len(rule.TablePattern) == 0 {
			schemaRules = append(schemaRules, rule)
		} else {
			tableRules = append(tableRules, rule)
		}
	}
	if len(tableRules) == 0 && len(schemaRules) == 0 {
		return cols, vals
	}
	var rule *TableRule
	if len(tableRules) == 0 {
		rule = schemaRules[0]
	} else {
		rule = tableRules[0]
	}
	if rule.TableExtractor != nil {
		cols = append(cols, rule.TableExtractor.TargetColumn)
		vals = append(vals, extractVal(table, rule.TableExtractor))
	}

	if rule.SchemaExtractor != nil {
		cols = append(cols, rule.SchemaExtractor.TargetColumn)
		vals = append(vals, extractVal(schema, rule.SchemaExtractor))
	}

	if rule.SourceExtractor != nil {
		cols = append(cols, rule.SourceExtractor.TargetColumn)
		vals = append(vals, extractVal(source, rule.SourceExtractor))
	}
	return cols, vals
}

func extractVal(s string, ext interface{}) string {
	var params []string
	switch e := ext.(type) {
	case *oldrouter.TableExtractor:
		if regExpr, err := regexp.Compile(e.TableRegexp); err == nil {
			params = regExpr.FindStringSubmatch(s)
		} else {
			return ""
		}
	case *oldrouter.SchemaExtractor:
		if regExpr, err := regexp.Compile(e.SchemaRegexp); err == nil {
			params = regExpr.FindStringSubmatch(s)
		} else {
			return ""
		}
	case *oldrouter.SourceExtractor:
		if regExpr, err := regexp.Compile(e.SourceRegexp); err == nil {
			params = regExpr.FindStringSubmatch(s)
		} else {
			return ""
		}
	}
	var val strings.Builder
	for idx, param := range params {
		if idx > 0 {
			val.WriteString(param)
		}
	}
	return val.String()
}
