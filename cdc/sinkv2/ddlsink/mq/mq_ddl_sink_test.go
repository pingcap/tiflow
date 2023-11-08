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

	"github.com/Shopify/sarama"
	mm "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tiflow/cdc/model"
	mqv1 "github.com/pingcap/tiflow/cdc/sink/mq"
	"github.com/pingcap/tiflow/cdc/sinkv2/ddlsink/mq/ddlproducer"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/sink/kafka"
	"github.com/stretchr/testify/require"
)

//nolint:unparam
func initBroker(t *testing.T, partitionNum int) (*sarama.MockBroker, string) {
	topic := kafka.DefaultMockTopicName
	leader := sarama.NewMockBroker(t, 1)

	metadataResponse := sarama.NewMockMetadataResponse(t)
	metadataResponse.SetBroker(leader.Addr(), leader.BrokerID())
	for i := 0; i < partitionNum; i++ {
		metadataResponse.SetLeader(topic, int32(i), leader.BrokerID())
	}

	prodSuccess := sarama.NewMockProduceResponse(t)
	handlerMap := make(map[string]sarama.MockResponse)
	handlerMap["MetadataRequest"] = metadataResponse
	handlerMap["ProduceRequest"] = prodSuccess
	leader.SetHandlerByMap(handlerMap)

	return leader, topic
}

func TestNewKafkaDDLSinkFailed(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	leader, topic := initBroker(t, kafka.DefaultMockPartitionNum)
	defer leader.Close()
	uriTemplate := "kafka://%s/%s?kafka-version=0.9.0.0&max-batch-size=1" +
		"&max-message-bytes=1048576&partition-num=1" +
		"&kafka-client-id=unit-test&auto-create-topic=false&compression=gzip&protocol=avro"
	uri := fmt.Sprintf(uriTemplate, leader.Addr(), topic)

	sinkURI, err := url.Parse(uri)
	require.Nil(t, err)
	replicaConfig := config.GetDefaultReplicaConfig()
	require.Nil(t, replicaConfig.ValidateAndAdjust(sinkURI))

	s, err := NewKafkaDDLSink(ctx, sinkURI, replicaConfig,
		kafka.NewMockAdminClient, ddlproducer.NewMockDDLProducer)
	require.ErrorContains(t, err, "Avro protocol requires parameter \"schema-registry\"",
		"should report error when protocol is avro but schema-registry is not set")
	require.Nil(t, s)
}

func TestWriteDDLEventToAllPartitions(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	leader, topic := initBroker(t, kafka.DefaultMockPartitionNum)
	defer leader.Close()
	// partition-number is 2, so only send DDL events to 2 partitions.
	uriTemplate := "kafka://%s/%s?kafka-version=0.9.0.0&max-batch-size=1" +
		"&max-message-bytes=1048576&partition-num=2" +
		"&kafka-client-id=unit-test&auto-create-topic=false&compression=gzip&protocol=open-protocol"
	uri := fmt.Sprintf(uriTemplate, leader.Addr(), topic)

	sinkURI, err := url.Parse(uri)
	require.Nil(t, err)
	replicaConfig := config.GetDefaultReplicaConfig()
	require.Nil(t, replicaConfig.ValidateAndAdjust(sinkURI))

	s, err := NewKafkaDDLSink(ctx, sinkURI, replicaConfig,
		kafka.NewMockAdminClient, ddlproducer.NewMockDDLProducer)
	require.Nil(t, err)
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
	require.Nil(t, err)
	require.Len(t, s.producer.(*ddlproducer.MockDDLProducer).GetAllEvents(),
		2, "All partitions should be broadcast")
	require.Len(t, s.producer.(*ddlproducer.MockDDLProducer).GetEvents(mqv1.TopicPartitionKey{
		Topic:     "mock_topic",
		Partition: 0,
	}), 1)
	require.Len(t, s.producer.(*ddlproducer.MockDDLProducer).GetEvents(mqv1.TopicPartitionKey{
		Topic:     "mock_topic",
		Partition: 1,
	}), 1)
}

func TestWriteDDLEventToZeroPartition(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	leader, topic := initBroker(t, kafka.DefaultMockPartitionNum)
	defer leader.Close()
	uriTemplate := "kafka://%s/%s?kafka-version=0.9.0.0&max-batch-size=1" +
		"&max-message-bytes=1048576&partition-num=1" +
		"&kafka-client-id=unit-test&auto-create-topic=false&compression=gzip&protocol=canal-json"
	uri := fmt.Sprintf(uriTemplate, leader.Addr(), topic)

	sinkURI, err := url.Parse(uri)
	require.Nil(t, err)
	replicaConfig := config.GetDefaultReplicaConfig()
	require.Nil(t, replicaConfig.ValidateAndAdjust(sinkURI))

	s, err := NewKafkaDDLSink(ctx, sinkURI, replicaConfig,
		kafka.NewMockAdminClient, ddlproducer.NewMockDDLProducer)
	require.Nil(t, err)
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
	require.Nil(t, err)
	require.Len(t, s.producer.(*ddlproducer.MockDDLProducer).GetAllEvents(),
		1, "Only zero partition")
	require.Len(t, s.producer.(*ddlproducer.MockDDLProducer).GetEvents(mqv1.TopicPartitionKey{
		Topic:     "mock_topic",
		Partition: 0,
	}), 1)
	require.Len(t, s.producer.(*ddlproducer.MockDDLProducer).GetEvents(mqv1.TopicPartitionKey{
		Topic:     "mock_topic",
		Partition: 1,
	}), 0)
	require.Len(t, s.producer.(*ddlproducer.MockDDLProducer).GetEvents(mqv1.TopicPartitionKey{
		Topic:     "mock_topic",
		Partition: 2,
	}), 0)
}

func TestWriteCheckpointTsToDefaultTopic(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	leader, topic := initBroker(t, kafka.DefaultMockPartitionNum)
	defer leader.Close()
	// partition-num is set to 2, so send checkpoint to 2 partitions.
	uriTemplate := "kafka://%s/%s?kafka-version=0.9.0.0&max-batch-size=1" +
		"&max-message-bytes=1048576&partition-num=2" +
		"&kafka-client-id=unit-test&auto-create-topic=false&compression=gzip" +
		"&protocol=canal-json&enable-tidb-extension=true"
	uri := fmt.Sprintf(uriTemplate, leader.Addr(), topic)

	sinkURI, err := url.Parse(uri)
	require.Nil(t, err)
	replicaConfig := config.GetDefaultReplicaConfig()
	require.Nil(t, replicaConfig.ValidateAndAdjust(sinkURI))

	s, err := NewKafkaDDLSink(ctx, sinkURI, replicaConfig,
		kafka.NewMockAdminClient, ddlproducer.NewMockDDLProducer)
	require.Nil(t, err)
	require.NotNil(t, s)

	checkpointTs := uint64(417318403368288260)
	var tables []*model.TableInfo
	err = s.WriteCheckpointTs(ctx, checkpointTs, tables)
	require.Nil(t, err)

	require.Len(t, s.producer.(*ddlproducer.MockDDLProducer).GetAllEvents(),
		2, "All partitions should be broadcast")
	require.Len(t, s.producer.(*ddlproducer.MockDDLProducer).GetEvents(mqv1.TopicPartitionKey{
		Topic:     "mock_topic",
		Partition: 0,
	}), 1)
	require.Len(t, s.producer.(*ddlproducer.MockDDLProducer).GetEvents(mqv1.TopicPartitionKey{
		Topic:     "mock_topic",
		Partition: 1,
	}), 1)
}

func TestWriteCheckpointTsToTableTopics(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	leader, topic := initBroker(t, kafka.DefaultMockPartitionNum)
	defer leader.Close()
	// Notice: auto create topic is true. Auto created topic will have 1 partition.
	uriTemplate := "kafka://%s/%s?kafka-version=0.9.0.0&max-batch-size=1" +
		"&max-message-bytes=1048576&partition-num=1" +
		"&kafka-client-id=unit-test&auto-create-topic=true&compression=gzip" +
		"&protocol=canal-json&enable-tidb-extension=true"
	uri := fmt.Sprintf(uriTemplate, leader.Addr(), topic)

	sinkURI, err := url.Parse(uri)
	require.Nil(t, err)
	replicaConfig := config.GetDefaultReplicaConfig()
	require.Nil(t, replicaConfig.ValidateAndAdjust(sinkURI))
	replicaConfig.Sink.DispatchRules = []*config.DispatchRule{
		{
			Matcher:   []string{"*.*"},
			TopicRule: "{schema}_{table}",
		},
	}

	s, err := NewKafkaDDLSink(ctx, sinkURI, replicaConfig,
		kafka.NewMockAdminClient, ddlproducer.NewMockDDLProducer)
	require.Nil(t, err)
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
	require.Nil(t, err)

	require.Len(t, s.producer.(*ddlproducer.MockDDLProducer).GetAllEvents(),
		4, "All topics and partitions should be broadcast")
	require.Len(t, s.producer.(*ddlproducer.MockDDLProducer).GetEvents(mqv1.TopicPartitionKey{
		Topic:     "mock_topic",
		Partition: 0,
	}), 1)
	require.Len(t, s.producer.(*ddlproducer.MockDDLProducer).GetEvents(mqv1.TopicPartitionKey{
		Topic:     "cdc_person",
		Partition: 0,
	}), 1)
	require.Len(t, s.producer.(*ddlproducer.MockDDLProducer).GetEvents(mqv1.TopicPartitionKey{
		Topic:     "cdc_person1",
		Partition: 0,
	}), 1)
	require.Len(t, s.producer.(*ddlproducer.MockDDLProducer).GetEvents(mqv1.TopicPartitionKey{
		Topic:     "cdc_person2",
		Partition: 0,
	}), 1)
}

func TestWriteCheckpointTsWhenCanalJsonTiDBExtensionIsDisable(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	leader, topic := initBroker(t, kafka.DefaultMockPartitionNum)
	defer leader.Close()
	// Notice: tidb extension is disabled.
	uriTemplate := "kafka://%s/%s?kafka-version=0.9.0.0&max-batch-size=1" +
		"&max-message-bytes=1048576&partition-num=1" +
		"&kafka-client-id=unit-test&auto-create-topic=false&compression=gzip" +
		"&protocol=canal-json&enable-tidb-extension=false"
	uri := fmt.Sprintf(uriTemplate, leader.Addr(), topic)

	sinkURI, err := url.Parse(uri)
	require.Nil(t, err)
	replicaConfig := config.GetDefaultReplicaConfig()
	require.Nil(t, replicaConfig.ValidateAndAdjust(sinkURI))

	s, err := NewKafkaDDLSink(ctx, sinkURI, replicaConfig,
		kafka.NewMockAdminClient, ddlproducer.NewMockDDLProducer)
	require.Nil(t, err)
	require.NotNil(t, s)

	checkpointTs := uint64(417318403368288260)
	var tables []*model.TableInfo
	err = s.WriteCheckpointTs(ctx, checkpointTs, tables)
	require.Nil(t, err)

	require.Len(t, s.producer.(*ddlproducer.MockDDLProducer).GetAllEvents(),
		0, "No topic and partition should be broadcast")
}
