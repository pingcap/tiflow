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

package dispatcher

import (
	"strings"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/config"
	filter "github.com/pingcap/tidb-tools/pkg/table-filter"
	"go.uber.org/zap"
)

// Dispatcher is an abstraction for dispatching rows into different partitions
type Dispatcher interface {
	// Dispatch returns a index of partitions according to RowChangedEvent
	Dispatch(row *model.RowChangedEvent) int32
}

type dispatchRule int

const (
	dispatchRuleDefault dispatchRule = iota
	dispatchRuleRowID
	dispatchRuleTS
	dispatchRuleTable
	dispatchRuleIndexValue
)

func (r *dispatchRule) fromString(rule string) {
	switch strings.ToLower(rule) {
	case "default":
		*r = dispatchRuleDefault
	case "rowid":
		*r = dispatchRuleRowID
	case "ts":
		*r = dispatchRuleTS
	case "table":
		*r = dispatchRuleTable
	case "index-value":
		*r = dispatchRuleIndexValue
	default:
		*r = dispatchRuleDefault
		log.Warn("can't support dispatch rule, using default rule", zap.String("rule", rule))
	}
}

type dispatcherSwitcher struct {
	rules []struct {
		Dispatcher
		filter.Filter
	}
}

func (s *dispatcherSwitcher) Dispatch(row *model.RowChangedEvent) int32 {
	return s.matchDispatcher(row).Dispatch(row)
}

func (s *dispatcherSwitcher) matchDispatcher(row *model.RowChangedEvent) Dispatcher {
	for _, rule := range s.rules {
		if !rule.MatchTable(row.Table.Schema, row.Table.Table) {
			continue
		}
		return rule.Dispatcher
	}
	log.Fatal("the dispatch rule must cover all tables")
	return nil
}

// NewDispatcher creates a new dispatcher
func NewDispatcher(cfg *config.ReplicaConfig, partitionNum int32) (Dispatcher, error) {
	ruleConfigs := append(cfg.Sink.DispatchRules, &config.DispatchRule{
		Matcher:    []string{"*.*"},
		Dispatcher: "default",
	})
	rules := make([]struct {
		Dispatcher
		filter.Filter
	}, 0, len(ruleConfigs))

	for _, ruleConfig := range ruleConfigs {
		f, err := filter.Parse(ruleConfig.Matcher)
		if err != nil {
			return nil, err
		}
		if !cfg.CaseSensitive {
			f = filter.CaseInsensitive(f)
		}
		var d Dispatcher
		var rule dispatchRule
		rule.fromString(ruleConfig.Dispatcher)
		switch rule {
		case dispatchRuleRowID, dispatchRuleIndexValue:
			d = newIndexValueDispatcher(partitionNum)
		case dispatchRuleTS:
			d = newTsDispatcher(partitionNum)
		case dispatchRuleTable:
			d = newTsDispatcher(partitionNum)
		case dispatchRuleDefault:
			d = newDefaultDispatcher(partitionNum)
		}
		rules = append(rules, struct {
			Dispatcher
			filter.Filter
		}{Dispatcher: d, Filter: f})
	}
	return &dispatcherSwitcher{
		rules: rules,
	}, nil
}
