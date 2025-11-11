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
	"sync"
	"testing"

	mm "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/ddlsink/mq/ddlproducer"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/sink/kafka"
	"github.com/stretchr/testify/require"
)

func TestNewKafkaDDLSinkFailed(t *testing.T) {
	t.Parallel()

	changefeedID := model.DefaultChangeFeedID("test")
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
	s, err := NewKafkaDDLSink(ctx, changefeedID, sinkURI, replicaConfig,
		kafka.NewMockFactory, ddlproducer.NewMockDDLProducer)
	require.ErrorContains(t, err, "Avro protocol requires parameter \"schema-registry\"",
		"should report error when protocol is avro but schema-registry is not set")
	require.Nil(t, s)
}

func TestWriteDDLEventToAllPartitions(t *testing.T) {
	t.Parallel()

	changefeedID := model.DefaultChangeFeedID("test")
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
	s, err := NewKafkaDDLSink(ctx, changefeedID, sinkURI, replicaConfig,
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
	s, err := NewKafkaDDLSink(ctx, model.DefaultChangeFeedID("test"),
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
	s, err := NewKafkaDDLSink(ctx, model.DefaultChangeFeedID("test"),
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
	s, err := NewKafkaDDLSink(ctx, model.DefaultChangeFeedID("test"),
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
	replicaConfig.Sink.KafkaConfig = &config.KafkaConfig{
		LargeMessageHandle: config.NewDefaultLargeMessageHandleConfig(),
	}
	require.NoError(t, replicaConfig.ValidateAndAdjust(sinkURI))

	ctx = context.WithValue(ctx, "testing.T", t)
	s, err := NewKafkaDDLSink(ctx, model.DefaultChangeFeedID("test"),
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

func TestGetDLLDispatchRuleByProtocol(t *testing.T) {
	t.Parallel()

	require.Equal(t, PartitionZero, getDDLDispatchRule(config.ProtocolCanal))
	require.Equal(t, PartitionZero, getDDLDispatchRule(config.ProtocolCanalJSON))

	require.Equal(t, PartitionAll, getDDLDispatchRule(config.ProtocolOpen))
	require.Equal(t, PartitionAll, getDDLDispatchRule(config.ProtocolDefault))
	require.Equal(t, PartitionAll, getDDLDispatchRule(config.ProtocolAvro))
	require.Equal(t, PartitionAll, getDDLDispatchRule(config.ProtocolMaxwell))
	require.Equal(t, PartitionAll, getDDLDispatchRule(config.ProtocolCraft))
	require.Equal(t, PartitionAll, getDDLDispatchRule(config.ProtocolSimple))
}

// mockSyncProducer is used to count the calls to HeartbeatBrokers.
type mockSyncProducer struct {
	kafka.MockSaramaSyncProducer
	heartbeatCount int
	mu             sync.Mutex
}

func (m *mockSyncProducer) HeartbeatBrokers() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.heartbeatCount++
}

func (m *mockSyncProducer) GetHeartbeatCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.heartbeatCount
}

// mockEncoder is a mock implementation of codec.RowEventEncoder.
// It is used to prevent the test from needing to set up a real, complex encoder.
type mockEncoder struct{}

// This line ensures at compile time that mockEncoder correctly implements the interface.
var _ codec.RowEventEncoder = (*mockEncoder)(nil)

// A predefined error that our mock encoder will return.
var errMockEncoder = errors.New("mock encoder error")

// EncodeCheckpointEvent returns a specific error to halt the execution
// of the function under test right after the heartbeat logic.
func (m *mockEncoder) EncodeCheckpointEvent(ts uint64) (*common.Message, error) {
	return nil, errMockEncoder
}

// The following methods are part of the RowEventEncoder interface.
// They can be left with a minimal implementation as they are not called
// in the tested code path.
func (m *mockEncoder) AppendRowChangedEvent(
	_ context.Context, _ string, _ *model.RowChangedEvent, _ func(),
) error {
	return nil
}

func (m *mockEncoder) Build() []*common.Message {
	return nil
}

func (m *mockEncoder) EncodeDDLEvent(event *model.DDLEvent) (*common.Message, error) {
	return nil, nil
}
func (m *mockEncoder) SetMaxMessageBytes(bytes int) {}

func TestDDLSinkHeartbeat(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	changefeedID := model.DefaultChangeFeedID("test-ddl-sink")
	// Protocol is needed by newDDLSink but not used in the tested path.
	proto := config.ProtocolOpen
	// encoder := &mockEncoder{}
	builder := &codec.MockRowEventEncoderBuilder{}

	// Case 1: DDL Sink with a connection refresher.
	t.Run("DDLSinkWithConnectionRefresher", func(t *testing.T) {
		t.Parallel()
		producer := &mockSyncProducer{}
		// Other dependencies for newDDLSink can be nil as they are not used
		// before our mock encoder returns an error.
		ddlSink := newDDLSink(changefeedID, nil, nil, nil, nil, builder, proto, producer)

		require.Equal(t, 0, producer.GetHeartbeatCount())

		// WriteCheckpointTs should first call HeartbeatBrokers, then fail at the encoder.
		err := ddlSink.WriteCheckpointTs(ctx, 12345, nil)

		// Assert that the heartbeat was called exactly once.
		require.Equal(t, 1, producer.GetHeartbeatCount())
		// Assert that the function failed with the mock encoder's specific error.
		require.NoError(t, err)
	})

	// Case 2: DDLSink with a nil connection refresher (e.g., for Pulsar).
	t.Run("DDLSinkWithNilConnectionRefresher", func(t *testing.T) {
		t.Parallel()
		// Create the sink with a nil refresher.
		ddlSinkNilRefresher := newDDLSink(changefeedID, nil, nil, nil, nil, builder, proto, nil)

		// The call should not panic and should fail with the mock encoder's error.
		err := ddlSinkNilRefresher.WriteCheckpointTs(ctx, 12347, nil)
		require.NoError(t, err)
	})
}
