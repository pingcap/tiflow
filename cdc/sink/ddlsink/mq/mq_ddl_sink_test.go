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

package mq

import (
	"context"
	"fmt"
	"net/url"
	"testing"

	mm "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/ddlsink/mq/ddlproducer"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/sink/kafka"
	"github.com/stretchr/testify/require"
)

func TestNewKafkaDDLSinkFailed(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	uriTemplate := "kafka://%s/%s?kafka-version=0.9.0.0&max-batch-size=1" +
		"&max-message-bytes=1048576&partition-num=1" +
		"&kafka-client-id=unit-test&auto-create-topic=false&compression=gzip&protocol=avro"
	uri := fmt.Sprintf(uriTemplate, "127.0.0.1:9092", kafka.DefaultMockTopicName)

	sinkURI, err := url.Parse(uri)
	require.NoError(t, err)
	replicaConfig := config.GetDefaultReplicaConfig()
	require.NoError(t, replicaConfig.ValidateAndAdjust(sinkURI))

	ctx = context.WithValue(ctx, "testing.T", t)
	s, err := NewKafkaDDLSink(ctx, sinkURI, replicaConfig,
		kafka.NewMockFactory, ddlproducer.NewMockDDLProducer)
	require.ErrorContains(t, err, "Avro protocol requires parameter \"schema-registry\"",
		"should report error when protocol is avro but schema-registry is not set")
	require.Nil(t, s)
}

func TestWriteDDLEventToAllPartitions(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// partition-number is 2, so only send DDL events to 2 partitions.
	uriTemplate := "kafka://%s/%s?kafka-version=0.9.0.0&max-batch-size=1" +
		"&max-message-bytes=1048576&partition-num=2" +
		"&kafka-client-id=unit-test&auto-create-topic=false&compression=gzip&protocol=open-protocol"
	uri := fmt.Sprintf(uriTemplate, "127.0.0.1:9092", kafka.DefaultMockTopicName)

	sinkURI, err := url.Parse(uri)
	require.NoError(t, err)
	replicaConfig := config.GetDefaultReplicaConfig()
	require.NoError(t, replicaConfig.ValidateAndAdjust(sinkURI))

	ctx = context.WithValue(ctx, "testing.T", t)
	s, err := NewKafkaDDLSink(ctx, sinkURI, replicaConfig,
		kafka.NewMockFactory,
		ddlproducer.NewMockDDLProducer)
	require.NoError(t, err)
	require.NotNil(t, s)

	ddl := &model.DDLEvent{
		CommitTs: 417318403368288260,
		TableInfo: &model.TableInfo{
			TableName: model.TableName{
				Schema: "cdc", Table: "person",
			},
		},
		Query: "create table person(id int, name varchar(32), primary key(id))",
		Type:  mm.ActionCreateTable,
	}
	err = s.WriteDDLEvent(ctx, ddl)
	require.NoError(t, err)
	require.Len(t, s.producer.(*ddlproducer.MockDDLProducer).GetAllEvents(),
		2, "All partitions should be broadcast")
	require.Len(t, s.producer.(*ddlproducer.MockDDLProducer).GetEvents("mock_topic", 0), 1)
	require.Len(t, s.producer.(*ddlproducer.MockDDLProducer).GetEvents("mock_topic", 1), 1)
}

func TestWriteDDLEventToZeroPartition(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	uriTemplate := "kafka://%s/%s?kafka-version=0.9.0.0&max-batch-size=1" +
		"&max-message-bytes=1048576&partition-num=1" +
		"&kafka-client-id=unit-test&auto-create-topic=false&compression=gzip&protocol=canal-json"
	uri := fmt.Sprintf(uriTemplate, "127.0.0.1:9092", kafka.DefaultMockTopicName)

	sinkURI, err := url.Parse(uri)
	require.NoError(t, err)
	replicaConfig := config.GetDefaultReplicaConfig()
	require.NoError(t, replicaConfig.ValidateAndAdjust(sinkURI))

	ctx = context.WithValue(ctx, "testing.T", t)
	s, err := NewKafkaDDLSink(ctx,
		sinkURI, replicaConfig,
		kafka.NewMockFactory,
		ddlproducer.NewMockDDLProducer)
	require.NoError(t, err)
	require.NotNil(t, s)

	ddl := &model.DDLEvent{
		CommitTs: 417318403368288260,
		TableInfo: &model.TableInfo{
			TableName: model.TableName{
				Schema: "cdc", Table: "person",
			},
		},
		Query: "create table person(id int, name varchar(32), primary key(id))",
		Type:  mm.ActionCreateTable,
	}
	err = s.WriteDDLEvent(ctx, ddl)
	require.NoError(t, err)
	require.Len(t, s.producer.(*ddlproducer.MockDDLProducer).GetAllEvents(),
		1, "Only zero partition")
	require.Len(t, s.producer.(*ddlproducer.MockDDLProducer).GetEvents("mock_topic", 0), 1)
	require.Len(t, s.producer.(*ddlproducer.MockDDLProducer).GetEvents("mock_topic", 1), 0)
	require.Len(t, s.producer.(*ddlproducer.MockDDLProducer).GetEvents("mock_topic", 2), 0)
}

func TestWriteCheckpointTsToDefaultTopic(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// partition-num is set to 2, so send checkpoint to 2 partitions.
	uriTemplate := "kafka://%s/%s?kafka-version=0.9.0.0&max-batch-size=1" +
		"&max-message-bytes=1048576&partition-num=2" +
		"&kafka-client-id=unit-test&auto-create-topic=false&compression=gzip" +
		"&protocol=canal-json&enable-tidb-extension=true"
	uri := fmt.Sprintf(uriTemplate, "127.0.0.1:9092", kafka.DefaultMockTopicName)

	sinkURI, err := url.Parse(uri)
	require.Nil(t, err)
	replicaConfig := config.GetDefaultReplicaConfig()
	require.Nil(t, replicaConfig.ValidateAndAdjust(sinkURI))

	ctx = context.WithValue(ctx, "testing.T", t)
	s, err := NewKafkaDDLSink(ctx,
		sinkURI, replicaConfig,
		kafka.NewMockFactory,
		ddlproducer.NewMockDDLProducer)
	require.Nil(t, err)
	require.NotNil(t, s)

	checkpointTs := uint64(417318403368288260)
	var tables []*model.TableInfo
	err = s.WriteCheckpointTs(ctx, checkpointTs, tables)
	require.Nil(t, err)

	require.Len(t, s.producer.(*ddlproducer.MockDDLProducer).GetAllEvents(),
		2, "All partitions should be broadcast")
	require.Len(t, s.producer.(*ddlproducer.MockDDLProducer).GetEvents("mock_topic", 0), 1)
	require.Len(t, s.producer.(*ddlproducer.MockDDLProducer).GetEvents("mock_topic", 1), 1)
}

func TestWriteCheckpointTsToTableTopics(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Notice: auto create topic is true. Auto created topic will have 1 partition.
	uriTemplate := "kafka://%s/%s?kafka-version=0.9.0.0&max-batch-size=1" +
		"&max-message-bytes=1048576&partition-num=1" +
		"&kafka-client-id=unit-test&auto-create-topic=true&compression=gzip" +
		"&protocol=canal-json&enable-tidb-extension=true"
	uri := fmt.Sprintf(uriTemplate, "127.0.0.1:9092", kafka.DefaultMockTopicName)

	sinkURI, err := url.Parse(uri)
	require.NoError(t, err)
	replicaConfig := config.GetDefaultReplicaConfig()
	require.NoError(t, replicaConfig.ValidateAndAdjust(sinkURI))
	replicaConfig.Sink.DispatchRules = []*config.DispatchRule{
		{
			Matcher:   []string{"*.*"},
			TopicRule: "{schema}_{table}",
		},
	}

	ctx = context.WithValue(ctx, "testing.T", t)
	s, err := NewKafkaDDLSink(ctx,
		sinkURI, replicaConfig,
		kafka.NewMockFactory,
		ddlproducer.NewMockDDLProducer)
	require.NoError(t, err)
	require.NotNil(t, s)

	checkpointTs := uint64(417318403368288260)
	tables := []*model.TableInfo{
		{
			TableName: model.TableName{
				Schema: "cdc",
				Table:  "person",
			},
		},
		{
			TableName: model.TableName{
				Schema: "cdc",
				Table:  "person1",
			},
		},
		{
			TableName: model.TableName{
				Schema: "cdc",
				Table:  "person2",
			},
		},
	}

	err = s.WriteCheckpointTs(ctx, checkpointTs, tables)
	require.NoError(t, err)

	require.Len(t, s.producer.(*ddlproducer.MockDDLProducer).GetAllEvents(),
		4, "All topics and partitions should be broadcast")
	require.Len(t, s.producer.(*ddlproducer.MockDDLProducer).GetEvents("mock_topic", 0), 1)
	require.Len(t, s.producer.(*ddlproducer.MockDDLProducer).GetEvents("cdc_person", 0), 1)
	require.Len(t, s.producer.(*ddlproducer.MockDDLProducer).GetEvents("cdc_person1", 0), 1)
	require.Len(t, s.producer.(*ddlproducer.MockDDLProducer).GetEvents("cdc_person2", 0), 1)
}

func TestWriteCheckpointTsWhenCanalJsonTiDBExtensionIsDisable(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Notice: tidb extension is disabled.
	uriTemplate := "kafka://%s/%s?kafka-version=0.9.0.0&max-batch-size=1" +
		"&max-message-bytes=1048576&partition-num=1" +
		"&kafka-client-id=unit-test&auto-create-topic=false&compression=gzip" +
		"&protocol=canal-json&enable-tidb-extension=false"
	uri := fmt.Sprintf(uriTemplate, "127.0.0.1:9092", kafka.DefaultMockTopicName)

	sinkURI, err := url.Parse(uri)
	require.NoError(t, err)
	replicaConfig := config.GetDefaultReplicaConfig()
	require.NoError(t, replicaConfig.ValidateAndAdjust(sinkURI))

	ctx = context.WithValue(ctx, "testing.T", t)
	s, err := NewKafkaDDLSink(ctx,
		sinkURI, replicaConfig,
		kafka.NewMockFactory,
		ddlproducer.NewMockDDLProducer)
	require.NoError(t, err)
	require.NotNil(t, s)

	checkpointTs := uint64(417318403368288260)
	var tables []*model.TableInfo
	err = s.WriteCheckpointTs(ctx, checkpointTs, tables)
	require.NoError(t, err)

	require.Len(t, s.producer.(*ddlproducer.MockDDLProducer).GetAllEvents(),
		0, "No topic and partition should be broadcast")
}
