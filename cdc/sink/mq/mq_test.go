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

package mq

import (
	"context"
	"fmt"
	"net/url"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/codec/open"
	kafkap "github.com/pingcap/tiflow/cdc/sink/mq/producer/kafka"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/retry"
	"github.com/pingcap/tiflow/pkg/sink/kafka"
	"github.com/stretchr/testify/require"
)

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

func waitCheckpointTs(t *testing.T, s *mqSink, tableID int64, target uint64) uint64 {
	var checkpointTs uint64
	err := retry.Do(context.Background(), func() error {
		if v, ok := s.tableCheckpointTsMap.Load(tableID); ok {
			checkpointTs = v.(model.ResolvedTs).Ts
		}
		if checkpointTs >= target {
			return nil
		}
		return errors.Errorf("current checkponitTs %d is not larger than %d", checkpointTs, target)
	}, retry.WithBackoffBaseDelay(10), retry.WithMaxTries(20))

	require.Nil(t, err)
	return checkpointTs
}

func TestKafkaSink(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	leader, topic := initBroker(t, kafka.DefaultMockPartitionNum)
	defer leader.Close()

	uriTemplate := "kafka://%s/%s?kafka-version=0.9.0.0&max-batch-size=1" +
		"&max-message-bytes=1048576&partition-num=1" +
		"&kafka-client-id=unit-test&auto-create-topic=false&compression=gzip&protocol=open-protocol"
	uri := fmt.Sprintf(uriTemplate, leader.Addr(), topic)
	sinkURI, err := url.Parse(uri)
	require.Nil(t, err)
	replicaConfig := config.GetDefaultReplicaConfig()
	errCh := make(chan error, 1)

	kafkap.NewAdminClientImpl = kafka.NewMockAdminClient
	defer func() {
		kafkap.NewAdminClientImpl = kafka.NewSaramaAdminClient
	}()

	changefeedID := model.DefaultChangeFeedID("changefeed-test")
	require.Nil(t, replicaConfig.ValidateAndAdjust(sinkURI))
	sink, err := NewKafkaSaramaSink(ctx, sinkURI, replicaConfig, errCh, changefeedID)
	require.Nil(t, err)

	encoder := sink.encoderBuilder.Build()

	require.IsType(t, &open.BatchEncoder{}, encoder)

	// mock kafka broker processes 1 row changed event
	tableID := model.TableID(1)
	row := &model.RowChangedEvent{
		Table: &model.TableName{
			Schema:  "test",
			Table:   "t1",
			TableID: tableID,
		},
		StartTs:  100,
		CommitTs: 120,
		Columns: []*model.Column{{
			Name:  "col1",
			Type:  mysql.TypeVarchar,
			Value: []byte("aa"),
		}},
	}
	err = sink.EmitRowChangedEvents(ctx, row)
	require.Nil(t, err)
	_, err = sink.FlushRowChangedEvents(ctx, tableID, model.NewResolvedTs(uint64(120)))
	require.Nil(t, err)
	checkpointTs := waitCheckpointTs(t, sink, tableID, uint64(120))
	require.Equal(t, uint64(120), checkpointTs)
	// flush older resolved ts
	checkpoint, err := sink.FlushRowChangedEvents(ctx, tableID, model.NewResolvedTs(uint64(110)))
	require.Nil(t, err)
	require.Equal(t, uint64(120), checkpoint.Ts)

	// mock kafka broker processes 1 checkpoint ts event
	err = sink.EmitCheckpointTs(ctx, uint64(120), []*model.TableInfo{{
		TableName: model.TableName{
			Schema: "test",
			Table:  "t1",
		},
	}})
	require.Nil(t, err)
	defer func() {
		err = sink.Close(ctx)
		if err != nil {
			require.Equal(t, context.Canceled, errors.Cause(err))
		}
	}()

	// mock kafka broker processes 1 ddl event
	ddl := &model.DDLEvent{
		StartTs:  130,
		CommitTs: 140,
		TableInfo: &model.TableInfo{
			TableName: model.TableName{
				Schema: "a", Table: "b",
			},
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
}

func TestFlushRowChangedEvents(t *testing.T) {
	t.Skip("skip because of race introduced by #9026")
	ctx, cancel := context.WithCancel(context.Background())

	leader, topic := initBroker(t, kafka.DefaultMockPartitionNum)
	defer leader.Close()

	uriTemplate := "kafka://%s/%s?kafka-version=0.9.0.0&max-batch-size=1" +
		"&max-message-bytes=1048576&partition-num=1" +
		"&kafka-client-id=unit-test&auto-create-topic=false&compression=gzip&protocol=open-protocol"
	uri := fmt.Sprintf(uriTemplate, leader.Addr(), topic)
	sinkURI, err := url.Parse(uri)
	require.Nil(t, err)
	replicaConfig := config.GetDefaultReplicaConfig()
	errCh := make(chan error, 1)

	kafkap.NewAdminClientImpl = kafka.NewMockAdminClient
	defer func() {
		kafkap.NewAdminClientImpl = kafka.NewSaramaAdminClient
	}()

	require.Nil(t, replicaConfig.ValidateAndAdjust(sinkURI))

	changefeedID := model.DefaultChangeFeedID("changefeed-test")
	sink, err := NewKafkaSaramaSink(ctx, sinkURI, replicaConfig, errCh, changefeedID)
	require.Nil(t, err)

	// mock kafka broker processes 1 row changed event
	tableID1 := model.TableID(1)
	row1 := &model.RowChangedEvent{
		Table: &model.TableName{
			Schema:  "test",
			Table:   "t1",
			TableID: tableID1,
		},
		StartTs:  100,
		CommitTs: 120,
		Columns: []*model.Column{{
			Name:  "col1",
			Type:  mysql.TypeVarchar,
			Value: []byte("aa"),
		}},
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
		Columns: []*model.Column{{
			Name:  "col1",
			Type:  mysql.TypeVarchar,
			Value: []byte("aa"),
		}},
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
		Columns: []*model.Column{{
			Name:  "col1",
			Type:  mysql.TypeVarchar,
			Value: []byte("aa"),
		}},
	}

	err = sink.EmitRowChangedEvents(ctx, row3)
	require.Nil(t, err)

	// mock kafka broker processes 1 row resolvedTs event
	_, err = sink.FlushRowChangedEvents(ctx, tableID1, model.NewResolvedTs(row1.CommitTs))
	require.Nil(t, err)
	checkpointTs1 := waitCheckpointTs(t, sink, tableID1, row1.CommitTs)
	require.Equal(t, row1.CommitTs, checkpointTs1)

	_, err = sink.FlushRowChangedEvents(ctx, tableID2, model.NewResolvedTs(row2.CommitTs))
	require.Nil(t, err)
	checkpointTs2 := waitCheckpointTs(t, sink, tableID2, row2.CommitTs)
	require.Equal(t, row2.CommitTs, checkpointTs2)

	_, err = sink.FlushRowChangedEvents(ctx, tableID3, model.NewResolvedTs(row3.CommitTs))
	require.Nil(t, err)
	checkpointTs3 := waitCheckpointTs(t, sink, tableID3, row3.CommitTs)
	require.Equal(t, row3.CommitTs, checkpointTs3)

	// flush older resolved ts
	_, err = sink.FlushRowChangedEvents(ctx, tableID1, model.NewResolvedTs(uint64(110)))
	require.Nil(t, err)
	checkpointTsOld := waitCheckpointTs(t, sink, tableID1, row1.CommitTs)
	require.Equal(t, row1.CommitTs, checkpointTsOld)

	cancel()
	err = sink.Close(ctx)
	if err != nil {
		require.Equal(t, context.Canceled, errors.Cause(err))
	}
}
