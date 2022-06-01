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

	"github.com/Shopify/sarama"
	"github.com/pingcap/check"
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
)

type mqSinkSuite struct{}

var _ = check.Suite(&mqSinkSuite{})

func (s mqSinkSuite) TestKafkaSink(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx, cancel := context.WithCancel(context.Background())

	topic := kafka.DefaultMockTopicName
	leader := sarama.NewMockBroker(c, 1)
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
	c.Assert(err, check.IsNil)
	replicaConfig := config.GetDefaultReplicaConfig()
	fr, err := filter.NewFilter(replicaConfig)
	c.Assert(err, check.IsNil)
	opts := map[string]string{}
	errCh := make(chan error, 1)

	kafkap.NewAdminClientImpl = kafka.NewMockAdminClient
	defer func() {
		kafkap.NewAdminClientImpl = kafka.NewSaramaAdminClient
	}()

	sink, err := newKafkaSaramaSink(ctx, sinkURI, fr, replicaConfig, opts, errCh)
	c.Assert(err, check.IsNil)

	encoder := sink.encoderBuilder.Build()

	c.Assert(encoder, check.FitsTypeOf, &codec.JSONEventBatchEncoder{})
	c.Assert(encoder.(*codec.JSONEventBatchEncoder).GetMaxBatchSize(), check.Equals, 1)
	c.Assert(encoder.(*codec.JSONEventBatchEncoder).GetMaxMessageBytes(), check.Equals, 1048576)

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
	c.Assert(err, check.IsNil)
	checkpointTs, err := sink.FlushRowChangedEvents(ctx, tableID, uint64(120))
	c.Assert(err, check.IsNil)
	c.Assert(checkpointTs, check.Equals, uint64(120))
	// flush older resolved ts
	checkpointTs, err = sink.FlushRowChangedEvents(ctx, tableID, uint64(110))
	c.Assert(err, check.IsNil)
	c.Assert(checkpointTs, check.Equals, uint64(120))

	// mock kafka broker processes 1 checkpoint ts event
<<<<<<< HEAD:cdc/sink/mq_test.go
	leader.Returns(prodSuccess)
	// TODO(hi-rustin): fix it after topic manager is ready.
	err = sink.EmitCheckpointTs(ctx, uint64(120), nil)
	c.Assert(err, check.IsNil)
=======
	err = sink.EmitCheckpointTs(ctx, uint64(120), []model.TableName{{
		Schema: "test",
		Table:  "t1",
	}})
	require.Nil(t, err)
	defer func() {
		err = sink.Close(ctx)
		if err != nil {
			require.Equal(t, context.Canceled, errors.Cause(err))
		}
	}()
>>>>>>> 2b758b1a3 (sink/mq(ticdc): make EmitCheckpointTs and FlushRowChangedEvents non-blocking (#5675)):cdc/sink/mq/mq_test.go

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
	c.Assert(err, check.IsNil)

	cancel()
	err = sink.EmitRowChangedEvents(ctx, row)
	if err != nil {
		c.Assert(errors.Cause(err), check.Equals, context.Canceled)
	}
	err = sink.EmitDDLEvent(ctx, ddl)
	if err != nil {
		c.Assert(errors.Cause(err), check.Equals, context.Canceled)
	}
	err = sink.EmitCheckpointTs(ctx, uint64(140), nil)
	if err != nil {
		c.Assert(errors.Cause(err), check.Equals, context.Canceled)
	}
<<<<<<< HEAD:cdc/sink/mq_test.go

	err = sink.Close(ctx)
	if err != nil {
		c.Assert(errors.Cause(err), check.Equals, context.Canceled)
	}
=======
>>>>>>> 2b758b1a3 (sink/mq(ticdc): make EmitCheckpointTs and FlushRowChangedEvents non-blocking (#5675)):cdc/sink/mq/mq_test.go
}

func (s mqSinkSuite) TestKafkaSinkFilter(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx, cancel := context.WithCancel(context.Background())
<<<<<<< HEAD:cdc/sink/mq_test.go
	defer cancel()

	topic := kafka.DefaultMockTopicName
	leader := sarama.NewMockBroker(c, 1)
=======
	leader, topic := initBroker(t, kafka.DefaultMockPartitionNum)
>>>>>>> 2b758b1a3 (sink/mq(ticdc): make EmitCheckpointTs and FlushRowChangedEvents non-blocking (#5675)):cdc/sink/mq/mq_test.go
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
	c.Assert(err, check.IsNil)
	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Filter = &config.FilterConfig{
		Rules: []string{"test.*"},
	}
	fr, err := filter.NewFilter(replicaConfig)
	c.Assert(err, check.IsNil)
	opts := map[string]string{}
	errCh := make(chan error, 1)

	kafkap.NewAdminClientImpl = kafka.NewMockAdminClient
	defer func() {
		kafkap.NewAdminClientImpl = kafka.NewSaramaAdminClient
	}()

	sink, err := newKafkaSaramaSink(ctx, sinkURI, fr, replicaConfig, opts, errCh)
	c.Assert(err, check.IsNil)

	row := &model.RowChangedEvent{
		Table: &model.TableName{
			Schema: "order",
			Table:  "t1",
		},
		StartTs:  100,
		CommitTs: 120,
	}
	err = sink.EmitRowChangedEvents(ctx, row)
	c.Assert(err, check.IsNil)
	c.Assert(sink.statistics.TotalRowsCount(), check.Equals, uint64(0))

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
	c.Assert(cerror.ErrDDLEventIgnored.Equal(err), check.IsTrue)

	cancel()
	err = sink.Close(ctx)
	if err != nil {
		c.Assert(errors.Cause(err), check.Equals, context.Canceled)
	}
}

func (s mqSinkSuite) TestPulsarSinkEncoderConfig(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx, cancel := context.WithCancel(context.Background())

	err := failpoint.Enable("github.com/pingcap/tiflow/cdc/sink/producer/pulsar/MockPulsar", "return(true)")
	c.Assert(err, check.IsNil)

	uri := "pulsar://127.0.0.1:1234/kafka-test?" +
		"max-message-bytes=4194304&max-batch-size=1"

	sinkURI, err := url.Parse(uri)
	c.Assert(err, check.IsNil)
	replicaConfig := config.GetDefaultReplicaConfig()
	fr, err := filter.NewFilter(replicaConfig)
	c.Assert(err, check.IsNil)
	opts := map[string]string{}
	errCh := make(chan error, 1)

	sink, err := newPulsarSink(ctx, sinkURI, fr, replicaConfig, opts, errCh)
	c.Assert(err, check.IsNil)

	encoder := sink.encoderBuilder.Build()
<<<<<<< HEAD:cdc/sink/mq_test.go
	c.Assert(encoder, check.FitsTypeOf, &codec.JSONEventBatchEncoder{})
	c.Assert(encoder.(*codec.JSONEventBatchEncoder).GetMaxBatchSize(), check.Equals, 1)
	c.Assert(encoder.(*codec.JSONEventBatchEncoder).GetMaxMessageBytes(), check.Equals, 4194304)
=======
	require.IsType(t, &codec.JSONEventBatchEncoder{}, encoder)
	require.Equal(t, 1, encoder.(*codec.JSONEventBatchEncoder).GetMaxBatchSize())
	require.Equal(t, 4194304, encoder.(*codec.JSONEventBatchEncoder).GetMaxMessageBytes())

	// FIXME: mock pulsar client doesn't support close,
	// so we can't call sink.Close() to close it.
	// We will leak goroutine if we don't close it.
	cancel()
	sink.flushWorker.close()
	sink.resolvedBuffer.Close()
>>>>>>> 2b758b1a3 (sink/mq(ticdc): make EmitCheckpointTs and FlushRowChangedEvents non-blocking (#5675)):cdc/sink/mq/mq_test.go
}

func (s mqSinkSuite) TestFlushRowChangedEvents(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx, cancel := context.WithCancel(context.Background())

	topic := kafka.DefaultMockTopicName
	leader := sarama.NewMockBroker(c, 1)
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
	c.Assert(err, check.IsNil)
	replicaConfig := config.GetDefaultReplicaConfig()
	fr, err := filter.NewFilter(replicaConfig)
	c.Assert(err, check.IsNil)
	opts := map[string]string{}
	errCh := make(chan error, 1)

	kafkap.NewAdminClientImpl = kafka.NewMockAdminClient
	defer func() {
		kafkap.NewAdminClientImpl = kafka.NewSaramaAdminClient
	}()

	sink, err := newKafkaSaramaSink(ctx, sinkURI, fr, replicaConfig, opts, errCh)
	c.Assert(err, check.IsNil)

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
	c.Assert(err, check.IsNil)

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
	c.Assert(err, check.IsNil)

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
	c.Assert(err, check.IsNil)

	// mock kafka broker processes 1 row resolvedTs event
	leader.Returns(prodSuccess)
	checkpointTs1, err := sink.FlushRowChangedEvents(ctx, tableID1, row1.CommitTs)
	c.Assert(err, check.IsNil)
	c.Assert(checkpointTs1, check.Equals, row1.CommitTs)

	checkpointTs2, err := sink.FlushRowChangedEvents(ctx, tableID2, row2.CommitTs)
	c.Assert(err, check.IsNil)
	c.Assert(checkpointTs2, check.Equals, row2.CommitTs)

	checkpointTs3, err := sink.FlushRowChangedEvents(ctx, tableID3, row3.CommitTs)
	c.Assert(err, check.IsNil)
	c.Assert(checkpointTs3, check.Equals, row3.CommitTs)

	// flush older resolved ts
	checkpointTsOld, err := sink.FlushRowChangedEvents(ctx, tableID1, uint64(110))
	c.Assert(err, check.IsNil)
	c.Assert(checkpointTsOld, check.Equals, row1.CommitTs)

	cancel()
	err = sink.Close(ctx)
	if err != nil {
		c.Assert(errors.Cause(err), check.Equals, context.Canceled)
	}
}
