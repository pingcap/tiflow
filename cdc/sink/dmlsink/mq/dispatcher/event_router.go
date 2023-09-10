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
	filter "github.com/pingcap/tidb/util/table-filter"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink/mq/dispatcher/partition"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink/mq/dispatcher/topic"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink"
	"github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

// DDLDispatchRule is the dispatch rule for DDL event.
type DDLDispatchRule int

const (
	// PartitionAll means the DDL event will be broadcast to all the partitions.
	PartitionAll DDLDispatchRule = -1
	// PartitionZero means the DDL event will be dispatched to partition 0.
	// NOTICE: Only for canal and canal-json protocol.
	PartitionZero = 0
)

type partitionDispatchRule int

const (
	partitionDispatchRuleDefault partitionDispatchRule = iota
	partitionDispatchRuleTS
	partitionDispatchRuleTable
	partitionDispatchRuleIndexValue
	partitionDispatchRuleKey
)

func (r *partitionDispatchRule) fromString(rule string) {
	switch strings.ToLower(rule) {
	case "default":
		*r = partitionDispatchRuleDefault
	case "ts":
		*r = partitionDispatchRuleTS
	case "table":
		*r = partitionDispatchRuleTable
	case "rowid":
		*r = partitionDispatchRuleIndexValue
		log.Warn("rowid is deprecated, please use index-value instead.")
	case "index-value":
		*r = partitionDispatchRuleIndexValue
	default:
		*r = partitionDispatchRuleKey
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

// NewEventRouter creates a new EventRouter.
func NewEventRouter(cfg *config.ReplicaConfig, defaultTopic, schema string) (*EventRouter, error) {
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
			return nil, cerror.WrapError(cerror.ErrFilterRuleInvalid, err, ruleConfig.Matcher)
		}
		if !cfg.CaseSensitive {
			f = filter.CaseInsensitive(f)
		}

		d := getPartitionDispatcher(ruleConfig, schema)
		t, err := getTopicDispatcher(ruleConfig, defaultTopic,
			util.GetOrZero(cfg.Sink.Protocol), schema)
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

// GetTopicForRowChange returns the target topic for row changes.
func (s *EventRouter) GetTopicForRowChange(row *model.RowChangedEvent) string {
	topicDispatcher, _ := s.matchDispatcher(row.Table.Schema, row.Table.Table)
	return topicDispatcher.Substitute(row.Table.Schema, row.Table.Table)
}

// GetTopicForDDL returns the target topic for DDL.
func (s *EventRouter) GetTopicForDDL(ddl *model.DDLEvent) string {
	var schema, table string
	if ddl.PreTableInfo != nil {
		if ddl.PreTableInfo.TableName.Table == "" {
			return s.defaultTopic
		}
		schema = ddl.PreTableInfo.TableName.Schema
		table = ddl.PreTableInfo.TableName.Table
	} else {
		if ddl.TableInfo.TableName.Table == "" {
			return s.defaultTopic
		}
		schema = ddl.TableInfo.TableName.Schema
		table = ddl.TableInfo.TableName.Table
	}

	topicDispatcher, _ := s.matchDispatcher(schema, table)
	return topicDispatcher.Substitute(schema, table)
}

// GetPartitionForRowChange returns the target partition for row changes.
func (s *EventRouter) GetPartitionForRowChange(
	row *model.RowChangedEvent,
	partitionNum int32,
) (int32, string) {
	_, partitionDispatcher := s.matchDispatcher(
		row.Table.Schema, row.Table.Table,
	)

	return partitionDispatcher.DispatchRowChangedEvent(
		row, partitionNum,
	)
}

// GetDLLDispatchRuleByProtocol returns the DDL
// distribution rule according to the protocol.
func (s *EventRouter) GetDLLDispatchRuleByProtocol(
	protocol config.Protocol,
) DDLDispatchRule {
	if protocol == config.ProtocolCanal || protocol == config.ProtocolCanalJSON {
		return PartitionZero
	}
	return PartitionAll
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
			log.Debug("topic name corresponding to the table is the same as the default topic name",
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

// GetDefaultTopic returns the default topic name.
func (s *EventRouter) GetDefaultTopic() string {
	return s.defaultTopic
}

// matchDispatcher returns the target topic dispatcher and partition dispatcher if a
// row changed event matches a specific table filter.
func (s *EventRouter) matchDispatcher(
	schema, table string,
) (topic.Dispatcher, partition.Dispatcher) {
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
func getPartitionDispatcher(ruleConfig *config.DispatchRule, schema string) partition.Dispatcher {
	var (
		d    partition.Dispatcher
		rule partitionDispatchRule
	)
	rule.fromString(ruleConfig.PartitionRule)
	switch rule {
	case partitionDispatchRuleIndexValue:
		d = partition.NewIndexValueDispatcher()
	case partitionDispatchRuleTS:
		d = partition.NewTsDispatcher()
	case partitionDispatchRuleTable:
		d = partition.NewTableDispatcher()
	case partitionDispatchRuleDefault:
		d = partition.NewDefaultDispatcher()
	case partitionDispatchRuleKey:
		if sink.IsPulsarScheme(schema) {
			d = partition.NewKeyDispatcher(ruleConfig.PartitionRule)
		} else {
			log.Warn("the partition dispatch rule is not default/ts/table/index-value," +
				" use the default rule instead.")
			d = partition.NewDefaultDispatcher()
		}
	}

	return d
}

// getTopicDispatcher returns the topic dispatcher for a specific topic rule (aka topic expression).
func getTopicDispatcher(
	ruleConfig *config.DispatchRule, defaultTopic, protocol, schema string,
) (topic.Dispatcher, error) {
	if ruleConfig.TopicRule == "" {
		return topic.NewStaticTopicDispatcher(defaultTopic), nil
	}

	// check if this rule is a valid topic expression
	topicExpr := topic.Expression(ruleConfig.TopicRule)

	// validate the topic expression for pulsar sink
	if sink.IsPulsarScheme(schema) {
		err := topicExpr.PulsarValidate()
		if err != nil {
			return nil, err
		}
	}

	// validate the topic expression for kafka sink
	var p config.Protocol
	var err error
	if protocol != "" {
		p, err = config.ParseSinkProtocolFromString(protocol)
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrKafkaInvalidConfig, err)
		}
	}
	if p == config.ProtocolAvro {
		err = topicExpr.ValidateForAvro()
		if err != nil {
			return nil, err
		}
	} else {
		err = topicExpr.Validate()
		if err != nil {
			return nil, err
		}
	}
	return topic.NewDynamicTopicDispatcher(topicExpr), nil
}
