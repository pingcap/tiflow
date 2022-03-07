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

package dispatcher

import (
	"strings"

	"github.com/pingcap/log"
	filter "github.com/pingcap/tidb-tools/pkg/table-filter"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/dispatcher/partition"
	"github.com/pingcap/tiflow/cdc/sink/dispatcher/topic"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

type partitionDispatchRule int

const (
	partitionDispatchRuleDefault partitionDispatchRule = iota
	partitionDispatchRuleRowID
	partitionDispatchRuleTS
	partitionDispatchRuleTable
	partitionDispatchRuleIndexValue
)

func (r *partitionDispatchRule) fromString(rule string) {
	switch strings.ToLower(rule) {
	case "default":
		*r = partitionDispatchRuleDefault
	case "rowid":
		*r = partitionDispatchRuleRowID
	case "ts":
		*r = partitionDispatchRuleTS
	case "table":
		*r = partitionDispatchRuleTable
	case "index-value":
		*r = partitionDispatchRuleIndexValue
	default:
		*r = partitionDispatchRuleDefault
		log.Warn("can't support dispatch rule, using default rule", zap.String("rule", rule))
	}
}

// EventRouter is a router, it determines which topic and which partition
// an event should be dispatched to.
type EventRouter struct {
	defaultTopic string
	rules        []struct {
		partitionDispatcher partition.Dispatcher
		topicDispatcher     topic.Dispatcher
		filter.Filter
	}
}

// NewEventRouter creates a new EventRouter
func NewEventRouter(cfg *config.ReplicaConfig, defaultTopic string) (*EventRouter, error) {
	// If an event does not match any dispatching rules in the config file,
	// it will be dispatched by the default partition dispatcher and
	// static topic dispatcher because it matches *.* rule.
	ruleConfigs := append(cfg.Sink.DispatchRules, &config.DispatchRule{
		Matcher:       []string{"*.*"},
		PartitionRule: "default",
		TopicRule:     "",
	})
	rules := make([]struct {
		partitionDispatcher partition.Dispatcher
		topicDispatcher     topic.Dispatcher
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

		d := getPartitionDispatcher(ruleConfig, cfg.EnableOldValue)
		t, err := getTopicDispatcher(ruleConfig, defaultTopic)
		if err != nil {
			return nil, err
		}
		rules = append(rules, struct {
			partitionDispatcher partition.Dispatcher
			topicDispatcher     topic.Dispatcher
			filter.Filter
		}{partitionDispatcher: d, topicDispatcher: t, Filter: f})
	}
	return &EventRouter{
		defaultTopic: defaultTopic,
		rules:        rules,
	}, nil
}

// DispatchRowChangedEvent returns the target topic and partition.
func (s *EventRouter) DispatchRowChangedEvent(row *model.RowChangedEvent, partitionNum int32) (string, int32) {
	topicDispatcher, partitionDispatcher := s.matchDispatcher(row.Table.Schema, row.Table.Table)
	topic := topicDispatcher.DispatchRowChangedEvent(row)
	partition := partitionDispatcher.DispatchRowChangedEvent(row, partitionNum)

	return topic, partition
}

// GetActiveTopics returns a list of the corresponding topics
// for the tables that are actively synchronized.
func (s *EventRouter) GetActiveTopics(activeTables []model.TableName) []string {
	topics := make([]string, 0)
	topicsMap := make(map[string]bool, len(activeTables))
	for _, table := range activeTables {
		topicDispatcher, _ := s.matchDispatcher(table.Schema, table.Table)
		topicName := topicDispatcher.Substitute(table.Schema, table.Table)
		if topicName == s.defaultTopic {
			log.Warn("topic name corresponding to the table is the same as the default topic name",
				zap.String("table", table.String()),
				zap.String("defaultTopic", s.defaultTopic),
				zap.String("topicDispatcherExpression", topicDispatcher.String()),
			)
		}
		if !topicsMap[topicName] {
			topicsMap[topicName] = true
			topics = append(topics, topicName)
		}
	}

	// We also need to add the default topic.
	if !topicsMap[s.defaultTopic] {
		topics = append(topics, s.defaultTopic)
	}

	return topics
}

// matchDispatcher returns the target topic dispatcher and partition dispatcher if a
// row changed event matches a specific table filter.
func (s *EventRouter) matchDispatcher(schema, table string) (topic.Dispatcher, partition.Dispatcher) {
	for _, rule := range s.rules {
		if !rule.MatchTable(schema, table) {
			continue
		}
		return rule.topicDispatcher, rule.partitionDispatcher
	}
	log.Panic("the dispatch rule must cover all tables")
	return nil, nil
}

// getPartitionDispatcher returns the partition dispatcher for a specific partition rule.
func getPartitionDispatcher(ruleConfig *config.DispatchRule,
	enableOldValue bool) partition.Dispatcher {
	var (
		d    partition.Dispatcher
		rule partitionDispatchRule
	)
	rule.fromString(ruleConfig.PartitionRule)
	switch rule {
	case partitionDispatchRuleRowID, partitionDispatchRuleIndexValue:
		if enableOldValue {
			log.Warn("This index-value distribution mode " +
				"does not guarantee row-level orderliness when " +
				"switching on the old value, so please use caution!")
		}
		d = partition.NewIndexValueDispatcher()
	case partitionDispatchRuleTS:
		d = partition.NewTsDispatcher()
	case partitionDispatchRuleTable:
		d = partition.NewTableDispatcher()
	case partitionDispatchRuleDefault:
		d = partition.NewDefaultDispatcher(enableOldValue)
	}

	return d
}

// getTopicDispatcher returns the topic dispatcher for a specific topic rule (aka topic expression).
func getTopicDispatcher(ruleConfig *config.DispatchRule, defaultTopic string) (topic.Dispatcher, error) {
	if ruleConfig.TopicRule == "" {
		return topic.NewStaticTopicDispatcher(defaultTopic), nil
	}

	// check if this rule is a valid topic expression
	topicExpr := topic.Expression(ruleConfig.TopicRule)
	err := topicExpr.Validate()
	if err != nil {
		return nil, err
	}
	return topic.NewDynamicTopicDispatcher(defaultTopic, topicExpr), nil
}
