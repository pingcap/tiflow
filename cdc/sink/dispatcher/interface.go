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
	"github.com/pingcap/ticdc/cdc/entry"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/filter"
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
	default:
		*r = dispatchRuleDefault
		log.Warn("can't support dispatch rule, using default rule", zap.String("rule", rule))
	}
}

type dispatcherSwitcher struct {
	rules             map[entry.TableName]Dispatcher
	caseSensitive     bool
	partitionNum      int32
	defaultDispatcher Dispatcher
}

func (s *dispatcherSwitcher) Dispatch(row *model.RowChangedEvent) int32 {
	tableName := entry.TableName{Schema: row.Table.Schema, Table: row.Table.Table}
	if !s.caseSensitive {
		tableName.Schema = strings.ToLower(tableName.Schema)
		tableName.Table = strings.ToLower(tableName.Table)
	}
	dispatcher, exist := s.rules[tableName]
	if !exist {
		return s.defaultDispatcher.Dispatch(row)
	}
	return dispatcher.Dispatch(row)
}

// NewDispatcher creates a new dispatcher
func NewDispatcher(config *filter.ReplicaConfig, partitionNum int32) Dispatcher {
	p := &dispatcherSwitcher{
		caseSensitive:     config.FilterCaseSensitive,
		partitionNum:      partitionNum,
		rules:             make(map[entry.TableName]Dispatcher, len(config.SinkDispatchRules)),
		defaultDispatcher: &defaultDispatcher{partitionNum: partitionNum},
	}
	for _, ruleConfig := range config.SinkDispatchRules {
		tableName := entry.TableName{Schema: ruleConfig.Schema, Table: ruleConfig.Name}
		if !p.caseSensitive {
			tableName.Schema = strings.ToLower(tableName.Schema)
			tableName.Table = strings.ToLower(tableName.Table)
		}
		var rule dispatchRule
		rule.fromString(ruleConfig.Rule)
		switch rule {
		case dispatchRuleRowID:
			p.rules[tableName] = &rowIDDispatcher{partitionNum: partitionNum}
		case dispatchRuleTS:
			p.rules[tableName] = &tsDispatcher{partitionNum: partitionNum}
		case dispatchRuleTable:
			p.rules[tableName] = &tableDispatcher{partitionNum: partitionNum}
		}
	}
	return p
}
