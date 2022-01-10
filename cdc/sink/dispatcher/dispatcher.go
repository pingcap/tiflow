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
	dispatchRuleUnknown dispatchRule = iota
	dispatchRuleDefault
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
	default:
		*r = dispatchRuleUnknown
	}
}

type mqDispatcherSwitcher struct {
	rules []struct {
		Dispatcher
		filter.Filter
	}
}

func (s *mqDispatcherSwitcher) Dispatch(tbTxn *model.RawTableTxn) int32 {
	return s.matchDispatcher(tbTxn).Dispatch(tbTxn)
}

func (s *mqDispatcherSwitcher) matchDispatcher(tbTxn *model.RawTableTxn) Dispatcher {
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
func NewMQDispatcher(cfg *config.ReplicaConfig, partitionNum int32) (Dispatcher, error) {
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
			return nil, cerror.WrapError(cerror.ErrFilterRuleInvalid, err)
		}
		if !cfg.CaseSensitive {
			f = filter.CaseInsensitive(f)
		}
		var d Dispatcher
		var rule dispatchRule
		rule.fromString(ruleConfig.Dispatcher)
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
		default:
			d = newDefaultDispatcher(partitionNum, cfg.EnableOldValue)
			log.Warn("this dispatch rule is not supported by MQ sink, use default dispatch rule instead.", zap.String("unsupported dispatch rule", ruleConfig.Dispatcher))
		}
		rules = append(rules, struct {
			Dispatcher
			filter.Filter
		}{Dispatcher: d, Filter: f})
	}
	return &mqDispatcherSwitcher{
		rules: rules,
	}, nil
}

type mysqlDispatcherSwitcher struct {
	rules []struct {
		Dispatcher
		filter.Filter
	}
}

func (s *mysqlDispatcherSwitcher) Dispatch(tbTxn *model.RawTableTxn) int32 {
	return s.matchDispatcher(tbTxn).Dispatch(tbTxn)
}

func (s *mysqlDispatcherSwitcher) matchDispatcher(tbTxn *model.RawTableTxn) Dispatcher {
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
func NewMySQLDispatcher(cfg *config.ReplicaConfig, partitionNum int32) (Dispatcher, error) {
	ruleConfigs := append(cfg.Sink.DispatchRules, &config.DispatchRule{
		Matcher:    []string{"*.*"},
		Dispatcher: "causality",
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
		var d Dispatcher
		var rule dispatchRule
		rule.fromString(ruleConfig.Dispatcher)
		switch rule {
		case dispatchRuleTable:
			d = newTableDispatcher(partitionNum)
		case dispatchRuleCausality:
			d = newCausalityDispatcher(partitionNum)
		default:
			d = newCausalityDispatcher(partitionNum)
			log.Warn("this dispatch rule is not supported by MySQL sink, use causality dispatch rule instead.", zap.String("unsupported dispatch rule", ruleConfig.Dispatcher))
		}
		rules = append(rules, struct {
			Dispatcher
			filter.Filter
		}{Dispatcher: d, Filter: f})
	}
	return &mysqlDispatcherSwitcher{
		rules: rules,
	}, nil
}

func NewDispatcher(cfg *config.ReplicaConfig, partitionNum int32, sinkTp model.SinkType) (Dispatcher, error) {
	if model.SinkTypeMySQL == sinkTp {
		return NewMySQLDispatcher(cfg, partitionNum)
	} else if model.SinkTypeMQ == sinkTp {
		return NewMQDispatcher(cfg, partitionNum)
	}

	return nil, cerror.ErrSinkTypeForDispatcher.GenWithStackByArgs(sinkTp.String())
}
