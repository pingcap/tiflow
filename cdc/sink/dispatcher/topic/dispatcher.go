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

package topic

import (
	"fmt"

	"github.com/pingcap/tiflow/cdc/model"
)

// Dispatcher is an abstraction for dispatching rows and ddls into different topics.
type Dispatcher interface {
	fmt.Stringer
	DispatchRowChangedEvent(row *model.RowChangedEvent) string
	DispatchDDLEvent(ddl *model.DDLEvent) string
	Substitute(schema, table string) string
}

// StaticTopicDispatcher is a topic dispatcher which dispatches rows and ddls to the default topic.
type StaticTopicDispatcher struct {
	defaultTopic string
}

// NewStaticTopicDispatcher returns a StaticTopicDispatcher.
func NewStaticTopicDispatcher(defaultTopic string) *StaticTopicDispatcher {
	return &StaticTopicDispatcher{
		defaultTopic: defaultTopic,
	}
}

// DispatchRowChangedEvent returns the target topic to which a row should be dispatched to.
func (s *StaticTopicDispatcher) DispatchRowChangedEvent(row *model.RowChangedEvent) string {
	return s.defaultTopic
}

// DispatchDDLEvent returns the target topic to which a ddl should be dispatched.
func (s *StaticTopicDispatcher) DispatchDDLEvent(ddl *model.DDLEvent) string {
	return s.defaultTopic
}

// Substitute converts schema/table name in a topic expression to kafka topic name.
func (s *StaticTopicDispatcher) Substitute(schema, table string) string {
	return s.defaultTopic
}

func (s *StaticTopicDispatcher) String() string {
	return s.defaultTopic
}

// DynamicTopicDispatcher is a topic dispatcher which dispatches rows and ddls
// dynamically to the target topics.
type DynamicTopicDispatcher struct {
	defaultTopic string
	expression   Expression
}

// NewDynamicTopicDispatcher creates a DynamicTopicDispatcher.
func NewDynamicTopicDispatcher(defaultTopic string, topicExpr Expression) *DynamicTopicDispatcher {
	return &DynamicTopicDispatcher{
		defaultTopic: defaultTopic,
		expression:   topicExpr,
	}
}

// DispatchRowChangedEvent returns the target topic to which a row should be dispatched to.
func (d *DynamicTopicDispatcher) DispatchRowChangedEvent(row *model.RowChangedEvent) string {
	return d.expression.Substitute(row.Table.Schema, row.Table.Table)
}

// DispatchDDLEvent returns the target topic to which a ddl should be dispatched.
// If the ddl is a schema-level event such as 'CREATE DATABASE', 'DROP DATABASE', etc.,
// that event will be dispatched to default topic.
func (d *DynamicTopicDispatcher) DispatchDDLEvent(ddl *model.DDLEvent) string {
	if len(ddl.TableInfo.Table) == 0 {
		return d.defaultTopic
	}
	return d.expression.Substitute(ddl.TableInfo.Schema, ddl.TableInfo.Table)
}

// Substitute converts schema/table name in a topic expression to kafka topic name.
func (d *DynamicTopicDispatcher) Substitute(schema, table string) string {
	return d.expression.Substitute(schema, table)
}

func (d *DynamicTopicDispatcher) String() string {
	return string(d.expression)
}
