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

package sink

import (
	"context"
	"fmt"
	"net/url"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/codec"
	kafkap "github.com/pingcap/tiflow/cdc/sink/producer/kafka"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/filter"
	"github.com/pingcap/tiflow/pkg/kafka"
	"github.com/pingcap/tiflow/pkg/util/testleak"
	"github.com/stretchr/testify/require"
)

func TestKafkaSink(t *testing.T) {
	defer testleak.AfterTest(t)()
	ctx, cancel := context.WithCancel(context.Background())

	topic := kafka.DefaultMockTopicName
	leader := sarama.NewMockBroker(t, 1)
	defer leader.Close()
	metadataResponse := new(sarama.MetadataResponse)
	metadataResponse.AddBroker(leader.Addr(), leader.BrokerID())
	metadataResponse.AddTopicPartition(topic, 0, leader.BrokerID(), nil, nil, nil, sarama.ErrNoError)
	leader.Returns(metadataResponse)
	leader.Returns(metadataResponse)

	prodSuccess := new(sarama.ProduceResponse)
	prodSuccess.AddTopicPartition(topic, 0, sarama.ErrNoError)

	uriTemplate := "kafka://%s/%s?kafka-version=0.9.0.0&max-batch-size=1" +
		"&max-message-bytes=1048576&partition-num=1" +
		"&kafka-client-id=unit-test&auto-create-topic=false&compression=gzip&protocol=open-protocol"
	uri := fmt.Sprintf(uriTemplate, leader.Addr(), topic)
	sinkURI, err := url.Parse(uri)
	require.Nil(t, err)
	replicaConfig := config.GetDefaultReplicaConfig()
	fr, err := filter.NewFilter(replicaConfig)
	require.Nil(t, err)
	opts := map[string]string{}
	errCh := make(chan error, 1)

	kafkap.NewAdminClientImpl = kafka.NewMockAdminClient
	defer func() {
		kafkap.NewAdminClientImpl = kafka.NewSaramaAdminClient
	}()

	sink, err := newKafkaSaramaSink(ctx, sinkURI, fr, replicaConfig, opts, errCh)
	require.Nil(t, err)

	encoder := sink.encoderBuilder.Build()

	require.IsType(t, &codec.JSONEventBatchEncoder{}, encoder)
	require.Equal(t, 1, encoder.(*codec.JSONEventBatchEncoder).GetMaxBatchSize())
	require.Equal(t, 1048576, encoder.(*codec.JSONEventBatchEncoder).GetMaxMessageBytes())

	// mock kafka broker processes 1 row changed event
	leader.Returns(prodSuccess)
	tableID := model.TableID(1)
	row := &model.RowChangedEvent{
		Table: &model.TableName{
			Schema:  "test",
			Table:   "t1",
			TableID: tableID,
		},
		StartTs:  100,
		CommitTs: 120,
		Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "aa"}},
	}
	err = sink.EmitRowChangedEvents(ctx, row)
	require.Nil(t, err)
	checkpointTs, err := sink.FlushRowChangedEvents(ctx, tableID, uint64(120))
	require.Nil(t, err)
	require.Equal(t, uint64(120), checkpointTs)
	// flush older resolved ts
	checkpointTs, err = sink.FlushRowChangedEvents(ctx, tableID, uint64(110))
	require.Nil(t, err)
	require.Equal(t, uint64(120), checkpointTs)

	// mock kafka broker processes 1 checkpoint ts event
	leader.Returns(prodSuccess)
	err = sink.EmitCheckpointTs(ctx, uint64(120), []model.TableName{{
		Schema: "test",
		Table:  "t1",
	}})
	require.Nil(t, err)

	// mock kafka broker processes 1 ddl event
	leader.Returns(prodSuccess)
	ddl := &model.DDLEvent{
		StartTs:  130,
		CommitTs: 140,
		TableInfo: &model.SimpleTableInfo{
			Schema: "a", Table: "b",
		},
		Query: "create table a",
		Type:  1,
	}
	err = sink.EmitDDLEvent(ctx, ddl)
	require.Nil(t, err)

	cancel()
	err = sink.EmitRowChangedEvents(ctx, row)
	if err != nil {
		require.Equal(t, context.Canceled, errors.Cause(err))
	}
	err = sink.EmitDDLEvent(ctx, ddl)
	if err != nil {
		require.Equal(t, context.Canceled, errors.Cause(err))
	}
	err = sink.EmitCheckpointTs(ctx, uint64(140), nil)
	if err != nil {
		require.Equal(t, context.Canceled, errors.Cause(err))
	}

	err = sink.Close(ctx)
	if err != nil {
		require.Equal(t, context.Canceled, errors.Cause(err))
	}
}

func TestKafkaSinkFilter(t *testing.T) {
	defer testleak.AfterTest(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	topic := kafka.DefaultMockTopicName
	leader := sarama.NewMockBroker(t, 1)
	defer leader.Close()
	metadataResponse := new(sarama.MetadataResponse)
	metadataResponse.AddBroker(leader.Addr(), leader.BrokerID())
	metadataResponse.AddTopicPartition(topic, 0, leader.BrokerID(), nil, nil, nil, sarama.ErrNoError)
	leader.Returns(metadataResponse)
	leader.Returns(metadataResponse)

	prodSuccess := new(sarama.ProduceResponse)
	prodSuccess.AddTopicPartition(topic, 0, sarama.ErrNoError)

	uriTemplate := "kafka://%s/%s?kafka-version=0.9.0.0&auto-create-topic=false&protocol=open-protocol"
	uri := fmt.Sprintf(uriTemplate, leader.Addr(), topic)
	sinkURI, err := url.Parse(uri)
	require.Nil(t, err)
	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Filter = &config.FilterConfig{
		Rules: []string{"test.*"},
	}
	fr, err := filter.NewFilter(replicaConfig)
	require.Nil(t, err)
	opts := map[string]string{}
	errCh := make(chan error, 1)

	kafkap.NewAdminClientImpl = kafka.NewMockAdminClient
	defer func() {
		kafkap.NewAdminClientImpl = kafka.NewSaramaAdminClient
	}()

	sink, err := newKafkaSaramaSink(ctx, sinkURI, fr, replicaConfig, opts, errCh)
	require.Nil(t, err)

	row := &model.RowChangedEvent{
		Table: &model.TableName{
			Schema: "order",
			Table:  "t1",
		},
		StartTs:  100,
		CommitTs: 120,
	}
	err = sink.EmitRowChangedEvents(ctx, row)
	require.Nil(t, err)
	require.Equal(t, uint64(0), sink.statistics.TotalRowsCount())

	ddl := &model.DDLEvent{
		StartTs:  130,
		CommitTs: 140,
		TableInfo: &model.SimpleTableInfo{
			Schema: "lineitem", Table: "t2",
		},
		Query: "create table lineitem.t2",
		Type:  1,
	}
	err = sink.EmitDDLEvent(ctx, ddl)
	require.True(t, cerror.ErrDDLEventIgnored.Equal(err))

	err = sink.Close(ctx)
	if err != nil {
		require.Equal(t, context.Canceled, errors.Cause(err))
	}
}

func TestPulsarSinkEncoderConfig(t *testing.T) {
	defer testleak.AfterTest(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := failpoint.Enable("github.com/pingcap/tiflow/cdc/sink/producer/pulsar/MockPulsar", "return(true)")
	require.Nil(t, err)

	uri := "pulsar://127.0.0.1:1234/kafka-test?" +
		"max-message-bytes=4194304&max-batch-size=1"

	sinkURI, err := url.Parse(uri)
	require.Nil(t, err)
	replicaConfig := config.GetDefaultReplicaConfig()
	fr, err := filter.NewFilter(replicaConfig)
	require.Nil(t, err)
	opts := map[string]string{}
	errCh := make(chan error, 1)

	sink, err := newPulsarSink(ctx, sinkURI, fr, replicaConfig, opts, errCh)
	require.Nil(t, err)

	encoder := sink.encoderBuilder.Build()
	require.IsType(t, &codec.JSONEventBatchEncoder{}, encoder)
	require.Equal(t, 1, encoder.(*codec.JSONEventBatchEncoder).GetMaxBatchSize())
	require.Equal(t, 4194304, encoder.(*codec.JSONEventBatchEncoder).GetMaxMessageBytes())
}

func TestFlushRowChangedEvents(t *testing.T) {
	defer testleak.AfterTest(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	topic := kafka.DefaultMockTopicName
	leader := sarama.NewMockBroker(t, 1)
	defer leader.Close()

	metadataResponse := new(sarama.MetadataResponse)
	metadataResponse.AddBroker(leader.Addr(), leader.BrokerID())
	metadataResponse.AddTopicPartition(topic, 0, leader.BrokerID(), nil, nil, nil, sarama.ErrNoError)
	leader.Returns(metadataResponse)
	leader.Returns(metadataResponse)

	prodSuccess := new(sarama.ProduceResponse)
	prodSuccess.AddTopicPartition(topic, 0, sarama.ErrNoError)

	uriTemplate := "kafka://%s/%s?kafka-version=0.9.0.0&max-batch-size=1" +
		"&max-message-bytes=1048576&partition-num=1" +
		"&kafka-client-id=unit-test&auto-create-topic=false&compression=gzip&protocol=open-protocol"
	uri := fmt.Sprintf(uriTemplate, leader.Addr(), topic)
	sinkURI, err := url.Parse(uri)
	require.Nil(t, err)
	replicaConfig := config.GetDefaultReplicaConfig()
	fr, err := filter.NewFilter(replicaConfig)
	require.Nil(t, err)
	opts := map[string]string{}
	errCh := make(chan error, 1)

	kafkap.NewAdminClientImpl = kafka.NewMockAdminClient
	defer func() {
		kafkap.NewAdminClientImpl = kafka.NewSaramaAdminClient
	}()

	sink, err := newKafkaSaramaSink(ctx, sinkURI, fr, replicaConfig, opts, errCh)
	require.Nil(t, err)

	// mock kafka broker processes 1 row changed event
	leader.Returns(prodSuccess)
	tableID1 := model.TableID(1)
	row1 := &model.RowChangedEvent{
		Table: &model.TableName{
			Schema:  "test",
			Table:   "t1",
			TableID: tableID1,
		},
		StartTs:  100,
		CommitTs: 120,
		Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "aa"}},
	}
	err = sink.EmitRowChangedEvents(ctx, row1)
	require.Nil(t, err)

	tableID2 := model.TableID(2)
	row2 := &model.RowChangedEvent{
		Table: &model.TableName{
			Schema:  "test",
			Table:   "t2",
			TableID: tableID2,
		},
		StartTs:  90,
		CommitTs: 110,
		Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "aa"}},
	}
	err = sink.EmitRowChangedEvents(ctx, row2)
	require.Nil(t, err)

	tableID3 := model.TableID(3)
	row3 := &model.RowChangedEvent{
		Table: &model.TableName{
			Schema:  "test",
			Table:   "t3",
			TableID: tableID3,
		},
		StartTs:  110,
		CommitTs: 130,
		Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "aa"}},
	}

	err = sink.EmitRowChangedEvents(ctx, row3)
	require.Nil(t, err)

	// mock kafka broker processes 1 row resolvedTs event
	leader.Returns(prodSuccess)
	checkpointTs1, err := sink.FlushRowChangedEvents(ctx, tableID1, row1.CommitTs)
	require.Nil(t, err)
	require.Equal(t, row1.CommitTs, checkpointTs1)

	checkpointTs2, err := sink.FlushRowChangedEvents(ctx, tableID2, row2.CommitTs)
	require.Nil(t, err)
	require.Equal(t, row2.CommitTs, checkpointTs2)

	checkpointTs3, err := sink.FlushRowChangedEvents(ctx, tableID3, row3.CommitTs)
	require.Nil(t, err)
	require.Equal(t, row3.CommitTs, checkpointTs3)

	// flush older resolved ts
	checkpointTsOld, err := sink.FlushRowChangedEvents(ctx, tableID1, uint64(110))
	require.Nil(t, err)
	require.Equal(t, row1.CommitTs, checkpointTsOld)

	err = sink.Close(ctx)
	if err != nil {
		require.Equal(t, context.Canceled, errors.Cause(err))
	}
}
