// Copyright 2023 PingCAP, Inc.
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

package mq

import (
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	pulsarPublishedDDLSchemaTableCount      = "published_DDL_schema_table_count"
	pulsarPublishedDMLSchemaTableCount      = "published_DML_schema_table_count"
	pulsarPublishedMessageTypeResolvedCount = "published_message_type_resolved_count"
)

var (
	// pulsar producer DDL scheme, type: count/success/fail, May there are too many tables
	publishedDDLSchemaTableCountMetric = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "pulsar",
			Name:      pulsarPublishedDDLSchemaTableCount,
			Help:      "pulsar published schema count",
		}, []string{"topic", "changefeed", "schema", "type"})

	// pulsar producer DML scheme, type:count/success/fail , May there are too many tables
	publishedDMLSchemaTableCountMetric = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "pulsar",
			Name:      pulsarPublishedDMLSchemaTableCount,
			Help:      "pulsar published schema count ",
		}, []string{"topic", "changefeed", "schema", "type"})

	// pulsar producer WATER count, type:count/success/fail , May there are too many tables
	pulsarPublishedMessageTypeResolvedCountMetric = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "pulsar",
			Name:      pulsarPublishedMessageTypeResolvedCount,
			Help:      "pulsar published message type resolved count ",
		}, []string{"topic", "changefeed", "schema", "type"})
)

// IncPublishedDDLCount DDL
func IncPublishedDDLCount(topic, changefeed string, message *common.Message) {
	if message.Type == model.MessageTypeDDL {
		publishedDDLSchemaTableCountMetric.WithLabelValues(topic, changefeed, message.GetSchema(), "count").Inc()
	}
	if message.Type == model.MessageTypeResolved {
		pulsarPublishedMessageTypeResolvedCountMetric.WithLabelValues(topic, changefeed, message.GetSchema(), "count").Inc()
	}
}

// IncPublishedDDLSuccess success
func IncPublishedDDLSuccess(topic, changefeed string, message *common.Message) {
	if message.Type == model.MessageTypeDDL {
		publishedDDLSchemaTableCountMetric.WithLabelValues(topic, changefeed, message.GetSchema(), "success").Inc()
	}
	if message.Type == model.MessageTypeResolved {
		pulsarPublishedMessageTypeResolvedCountMetric.WithLabelValues(topic, changefeed, message.GetSchema(), "success").Inc()
	}
}

// IncPublishedDDLFail fail
func IncPublishedDDLFail(topic, changefeed string, message *common.Message) {
	if message.Type == model.MessageTypeDDL {
		publishedDDLSchemaTableCountMetric.WithLabelValues(topic, changefeed, message.GetSchema(), "fail").Inc()
	}
	if message.Type == model.MessageTypeResolved {
		pulsarPublishedMessageTypeResolvedCountMetric.WithLabelValues(topic, changefeed, message.GetSchema(), "fail").Inc()
	}
}

// IncPublishedDMLCount count
func IncPublishedDMLCount(topic, changefeed, schema string) {
	publishedDMLSchemaTableCountMetric.WithLabelValues(topic, changefeed, schema, "count").Inc()
}

// IncPublishedDMLSuccess success
func IncPublishedDMLSuccess(topic, changefeed, schema string) {
	publishedDMLSchemaTableCountMetric.WithLabelValues(topic, changefeed, schema, "success").Inc()
}

// IncPublishedDMLFail fail
func IncPublishedDMLFail(topic, changefeed, schema string) {
	publishedDMLSchemaTableCountMetric.WithLabelValues(topic, changefeed, schema, "fail").Inc()
}
