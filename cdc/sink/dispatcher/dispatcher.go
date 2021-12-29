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
	"fmt"
	"strings"

	"github.com/pingcap/log"
	filter "github.com/pingcap/tidb-tools/pkg/table-filter"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

// For reuse the dispatcher in both mysql sink and mq sink, we abstract the single unit as txn
// As for MySQL sink, it can be txn with multi-rows
// As for MQ sink, it can be txn with single-row
// Dispatcher is an abstraction for dispatching txn into different partitions
type Dispatcher interface {
	// Dispatch returns an index of partitions according to RowChangedEvent
	Dispatch(tbTxn *model.RawTableTxn) int32
}

type dispatchRule int

const (
	dispatchRuleDefault dispatchRule = iota
	dispatchRuleRowID
	dispatchRuleTS
	dispatchRuleTable
	dispatchRuleIndexValue
	dispatchRuleCausality
)

// To keep dispatcher rule unchanged for MQ sink, MySQL sink need use different default tag `causality`
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
	case "causality":
		*r = dispatchRuleCausality
	default: // [TODO] check
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

func (s *dispatcherSwitcher) Dispatch(tbTxn *model.RawTableTxn) int32 {
	return s.matchDispatcher(tbTxn).Dispatch(tbTxn)
}

func (s *dispatcherSwitcher) matchDispatcher(tbTxn *model.RawTableTxn) Dispatcher {
	for _, rule := range s.rules {
		if !rule.MatchTable(tbTxn.Table.Schema, tbTxn.Table.Table) {
			continue
		}
		return rule.Dispatcher
	}
	log.Panic("the dispatch rule must cover all tables")
	return nil
}

// NewDispatcher creates a new dispatcher
func NewDispatcher(cfg *config.ReplicaConfig, partitionNum int32, sinkTp model.sinkType) (Dispatcher, error) {
	if sinkTp != model.SinkTypeMQ || sinkTp != model.SinkTypeMQ {
		return nil, cerror.WrapError(cerror.ErrSinkTypeForDispatcher, sinkTp)
	}

	var ruleConfigs []*DispatchRule
	// NOTICE: New sink need to add default dispatcher here
	if model.SinkTypeMySQL == sinkTp {
		ruleConfigs = append(cfg.Sink.DispatchRules, &config.DispatchRule{
			Matcher:    []string{"*.*"},
			Dispatcher: "causality",
		})
	} else if model.SinkTypeMQ == sinkTp {
		ruleConfigs = append(cfg.Sink.DispatchRules, &config.DispatchRule{
			Matcher:    []string{"*.*"},
			Dispatcher: "default",
		})
	}

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
		var d Dispatcher
		var rule dispatchRule
		rule.fromString(ruleConfig.Dispatcher)
		if sinkTp == SinkTypeMySQL && rule != dispatcherRuleTable && rule != dispatchRuleCausality {
			log.Error("MySQL sink only support `causality` and `table` dispatch rule.", zap.String("user dispatch rule", ruleConfig.Dispatcher))
			return nil, cerror.WrapError(cerror.ErrFilterRuleUnsupported, fmt.Sprintf("%s for MySQL sink", ruleConfig.Dispatcher))
		}
		if sinkTp == SinkTypeMQ && rule == dispatcherRuleCausality {
			log.Error("MQ sink can't support `causality` dispatch rule.", zap.String("user dispatch rule", ruleConfig.Dispatcher))
			return nil, cerror.WrapError(cerror.ErrFilterRuleUnsupported, fmt.Sprintf("%s for MQ sink", ruleConfig.Dispatcher))
		}

		switch rule {
		case dispatchRuleRowID, dispatchRuleIndexValue:
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
		case dispatchRuleCausality:
			d = newCausalityDispatcher(partitionNum)
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
