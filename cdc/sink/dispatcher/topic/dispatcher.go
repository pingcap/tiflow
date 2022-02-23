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
	"github.com/pingcap/tiflow/cdc/model"
)

type Dispatcher interface {
	DispatchRowChangedEvent(row *model.RowChangedEvent) string
	DispatchDDLEvent(ddl *model.DDLEvent) string
}

type StaticTopicDispatcher struct {
	defaultTopic string
}

func NewStaticTopicDispatcher(defaultTopic string) *StaticTopicDispatcher {
	return &StaticTopicDispatcher{
		defaultTopic: defaultTopic,
	}
}

func (s *StaticTopicDispatcher) DispatchRowChangedEvent(row *model.RowChangedEvent) string {
	return s.defaultTopic
}

func (s *StaticTopicDispatcher) DispatchDDLEvent(ddl *model.DDLEvent) string {
	return s.defaultTopic
}

type DynamicTopicDispatcher struct {
	defaultTopic string
	expression   Expression
}

func NewDynamicTopicDispatcher(defaultTopic string, topicExpr Expression) *DynamicTopicDispatcher {
	return &DynamicTopicDispatcher{
		defaultTopic: defaultTopic,
		expression:   topicExpr,
	}
}

func (d *DynamicTopicDispatcher) DispatchRowChangedEvent(row *model.RowChangedEvent) string {
	return d.expression.Substitute(row.Table.Schema, row.Table.Table)
}

func (d *DynamicTopicDispatcher) DispatchDDLEvent(ddl *model.DDLEvent) string {
	if len(ddl.TableInfo.Table) == 0 {
		return d.defaultTopic
	}
	return d.expression.Substitute(ddl.TableInfo.Schema, ddl.TableInfo.Table)
}
