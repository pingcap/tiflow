// Copyright 2025 PingCAP, Inc.
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

package dispatcher

import (
	"github.com/pingcap/log"
	filter "github.com/pingcap/tidb/pkg/util/table-filter"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

// SinkRouter routes source schema/table names to target schema/table names for sinks.
// It determines the destination schema and table name for MySQL/TiDB sinks based on dispatch rules.
type SinkRouter struct {
	rules []struct {
		dispatcher Dispatcher
		filter.Filter
	}
}

// NewSinkRouter creates a new SinkRouter from replica config.
// It processes dispatch rules that have SchemaRule or TableRule configured.
// Returns nil if no routing rules are configured.
func NewSinkRouter(cfg *config.ReplicaConfig) (*SinkRouter, error) {
	if cfg == nil || cfg.Sink == nil {
		return nil, nil
	}

	rules := make([]struct {
		dispatcher Dispatcher
		filter.Filter
	}, 0)

	// Process dispatch rules that have schema/table routing configured
	for _, ruleConfig := range cfg.Sink.DispatchRules {
		// Skip rules that don't have schema/table routing
		if ruleConfig.SchemaRule == "" && ruleConfig.TableRule == "" {
			continue
		}

		// Validate the schema and table expressions
		if err := ValidateExpression(ruleConfig.SchemaRule); err != nil {
			return nil, cerror.WrapError(cerror.ErrInvalidReplicaConfig, err)
		}
		if err := ValidateExpression(ruleConfig.TableRule); err != nil {
			return nil, cerror.WrapError(cerror.ErrInvalidReplicaConfig, err)
		}

		// Parse the matcher filter
		f, err := filter.Parse(ruleConfig.Matcher)
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrFilterRuleInvalid, err, ruleConfig.Matcher)
		}
		if !cfg.CaseSensitive {
			f = filter.CaseInsensitive(f)
		}

		// Create the dispatcher for this rule
		d := NewDynamicSchemaDispatcher(ruleConfig.SchemaRule, ruleConfig.TableRule)

		rules = append(rules, struct {
			dispatcher Dispatcher
			filter.Filter
		}{dispatcher: d, Filter: f})
	}

	// Return nil if no routing rules were configured
	if len(rules) == 0 {
		return nil, nil
	}

	return &SinkRouter{rules: rules}, nil
}

// Route returns the target schema and table for a given source schema and table.
// If no routing rule matches, returns the source schema and table unchanged.
func (r *SinkRouter) Route(sourceSchema, sourceTable string) (string, string) {
	if r == nil || len(r.rules) == 0 {
		return sourceSchema, sourceTable
	}

	d := r.matchDispatcher(sourceSchema, sourceTable)
	if d == nil {
		return sourceSchema, sourceTable
	}

	targetSchema, targetTable := d.Substitute(sourceSchema, sourceTable)

	log.Debug("sink routing applied",
		zap.String("sourceSchema", sourceSchema),
		zap.String("sourceTable", sourceTable),
		zap.String("targetSchema", targetSchema),
		zap.String("targetTable", targetTable),
		zap.String("dispatcher", d.String()),
	)

	return targetSchema, targetTable
}

// matchDispatcher returns the dispatcher if the table matches a routing rule.
// Returns nil if no rule matches.
func (r *SinkRouter) matchDispatcher(schema, table string) Dispatcher {
	for _, rule := range r.rules {
		if rule.MatchTable(schema, table) {
			return rule.dispatcher
		}
	}
	return nil
}
