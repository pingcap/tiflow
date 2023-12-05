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

//go:build intest
// +build intest

package mq

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink/mq/dmlproducer"
	"github.com/pingcap/tiflow/cdc/sink/tablesink/state"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/sink/kafka"
	"github.com/stretchr/testify/require"
)

func TestNewKafkaDMLSinkFailed(t *testing.T) {
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

	errCh := make(chan error, 1)
	s, err := NewKafkaDMLSink(ctx, sinkURI, replicaConfig, errCh,
		kafka.NewMockFactory, dmlproducer.NewDMLMockProducer)
	require.ErrorContains(t, err, "Avro protocol requires parameter \"schema-registry\"",
		"should report error when protocol is avro but schema-registry is not set")
	require.Nil(t, s)
}

func TestWriteEvents(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	uriTemplate := "kafka://%s/%s?kafka-version=0.9.0.0&max-batch-size=1" +
		"&max-message-bytes=1048576&partition-num=1" +
		"&kafka-client-id=unit-test&auto-create-topic=false&compression=gzip&protocol=open-protocol"
	uri := fmt.Sprintf(uriTemplate, "127.0.0.1:9092", kafka.DefaultMockTopicName)

	sinkURI, err := url.Parse(uri)
	require.NoError(t, err)
	replicaConfig := config.GetDefaultReplicaConfig()
	require.NoError(t, replicaConfig.ValidateAndAdjust(sinkURI))
	errCh := make(chan error, 1)

	ctx = context.WithValue(ctx, "testing.T", t)
	s, err := NewKafkaDMLSink(ctx, sinkURI, replicaConfig, errCh,
		kafka.NewMockFactory, dmlproducer.NewDMLMockProducer)
	require.NoError(t, err)
	require.NotNil(t, s)
	defer s.Close()

	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	sql := `create table test.t(a varchar(255) primary key)`
	job := helper.DDL2Job(sql)
	tableInfo := model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)
	_, _, colInfo := tableInfo.GetRowColInfos()

	tableStatus := state.TableSinkSinking
	row := &model.RowChangedEvent{
		CommitTs:  1,
		Table:     &model.TableName{Schema: "test", Table: "t"},
		TableInfo: tableInfo,
		Columns:   []*model.Column{{Name: "col1", Type: mysql.TypeVarchar, Value: "aa"}},
		ColInfos:  colInfo,
	}

	events := make([]*dmlsink.CallbackableEvent[*model.SingleTableTxn], 0, 3000)
	for i := 0; i < 3000; i++ {
		events = append(events, &dmlsink.TxnCallbackableEvent{
			Event: &model.SingleTableTxn{
				Rows: []*model.RowChangedEvent{row},
			},
			Callback:  func() {},
			SinkState: &tableStatus,
		})
	}

	err = s.WriteEvents(events...)
	// Wait for the events to be received by the worker.
	time.Sleep(time.Second)
	require.NoError(t, err)
	require.Len(t, errCh, 0)
	require.Len(t, s.alive.worker.producer.(*dmlproducer.MockDMLProducer).GetAllEvents(), 3000)
}
