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
)

// Dispatcher is an abstraction for dispatching rows and ddls into different topics.
type Dispatcher interface {
	fmt.Stringer
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
	expression Expression
}

// NewDynamicTopicDispatcher creates a DynamicTopicDispatcher.
func NewDynamicTopicDispatcher(topicExpr Expression) *DynamicTopicDispatcher {
	return &DynamicTopicDispatcher{
		expression: topicExpr,
	}
}

// Substitute converts schema/table name in a topic expression to kafka topic name.
func (d *DynamicTopicDispatcher) Substitute(schema, table string) string {
	return d.expression.Substitute(schema, table)
}

func (d *DynamicTopicDispatcher) String() string {
	return string(d.expression)
}
