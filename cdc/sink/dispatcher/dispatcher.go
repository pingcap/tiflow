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
	"strconv"
	"strings"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/config"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	filter "github.com/pingcap/tidb-tools/pkg/table-filter"
	"go.uber.org/zap"
)

// Dispatcher is an abstraction for dispatching rows into different target destination.
type Dispatcher interface {
	// Dispatch returns an index which can be used to select a target dispatch destination.
	Dispatch(row *model.RowChangedEvent) int32
}

type dispatchRule int

const (
	dispatchRuleDefault dispatchRule = iota
	dispatchRuleRowID
	dispatchRuleTS
	dispatchRuleTable
	dispatchRuleIndexValue // alias of RowID
	dispatchRulePK         // alias of RowID
	dispatchRuleByColumns
	dispatchRuleByPartitionIndex
)

var rules = map[string]dispatchRule{
	"default":     dispatchRuleDefault,
	"rowid":       dispatchRuleRowID,
	"ts":          dispatchRuleTS,
	"table":       dispatchRuleTable,
	"index-value": dispatchRuleIndexValue,
	"pk":          dispatchRulePK,
}

// fromString decide which partition dispatcher rule to use by parsing the given `s`.
// if no one matched, use default.
func (r *dispatchRule) fromString(s string) {
	s = strings.ToLower(s)
	rule, ok := rules[s]
	if ok {
		*r = rule
		return
	}

	if tryGetDispatchRuleByPartitionIndex(s) {
		*r = dispatchRuleByPartitionIndex
		return
	}

	if tryGetDispatchRuleByColumns(s) {
		*r = dispatchRuleByColumns
		return
	}

	*r = dispatchRuleDefault
	log.Warn("can't support dispatch rule, using default rule", zap.String("rule", s))
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
		if rule.MatchTable(row.Table.Schema, row.Table.Table) {
			return rule.Dispatcher
		}
	}
	log.Panic("the dispatch rule must cover all tables")
	return nil
}

// NewDispatcher creates a new dispatcher
func NewDispatcher(cfg *config.ReplicaConfig, partitionNum int32) (Dispatcher, error) {
	ruleConfigs := append(cfg.Sink.DispatchRules, &config.DispatchRule{
		Matcher: []string{"*.*"},
		// todo (Ling Jin): use `table` as the default dispatch rule in the future version.
		Dispatcher: "default",
	})
	rules := make([]struct {
		Dispatcher
		filter.Filter
	}, 0, len(ruleConfigs))

	for _, ruleConfig := range ruleConfigs {
		f, err := filter.Parse(ruleConfig.Matcher)
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrFilterRuleInvalid, err)
		}
		if !cfg.CaseSensitive {
			f = filter.CaseInsensitive(f)
		}

		var (
			d    Dispatcher
			rule dispatchRule
		)
		partitionRule := ruleConfig.GetPartitionRule()
		rule.fromString(partitionRule)

		switch rule {
		case dispatchRuleRowID, dispatchRuleIndexValue, dispatchRulePK:
			if cfg.EnableOldValue {
				log.Warn("This index-value distribution mode " +
					"does not guarantee row-level orderliness when " +
					"switching on the old value, so please use caution!")
			}
			d = newIndexValueDispatcher(partitionNum)
		case dispatchRuleTS:
			d = newTsDispatcher(partitionNum)
		case dispatchRuleTable:
			d = newTableDispatcher(partitionNum)
		case dispatchRuleDefault:
			d = newDefaultDispatcher(partitionNum, cfg.EnableOldValue)
		case dispatchRuleByPartitionIndex:
			targetPartition, err := strconv.Atoi(partitionRule)
			if err != nil {
				return nil, cerror.WrapError(cerror.ErrFilterRuleInvalid, err)
			}
			if err := validateTargetPartitionIndex(int32(targetPartition), partitionNum); err != nil {
				return nil, err
			}
			d = newPartitionIndexDispatcher(int32(targetPartition))
		case dispatchRuleByColumns:
			d = newColumnsDispatcher(partitionNum, partitionRule)
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

// for by partition number rule, s should be able to be parsed as a positive int.
// this function only check if s an int, we have to check it value further.
func tryGetDispatchRuleByPartitionIndex(s string) bool {
	_, err := strconv.Atoi(s)
	return err == nil
}

func validateTargetPartitionIndex(target, partitionNum int32) error {
	if target < 0 {
		return cerror.ErrFilterRuleInvalid.GenWithStack(
			"can't create partition dispatcher by target partition not a positive integer")
	}

	if target >= partitionNum {
		return cerror.ErrFilterRuleInvalid.GenWithStack(
			"can't create partition dispatcher by target partition(%d) >= partitionNum(%d)",
			target, partitionNum)
	}

	return nil
}

// for by columns rule, s should have the format "[a, b, c]"
func tryGetDispatchRuleByColumns(s string) bool {
	return strings.HasPrefix(s, "[") && strings.HasSuffix(s, "]")
}
