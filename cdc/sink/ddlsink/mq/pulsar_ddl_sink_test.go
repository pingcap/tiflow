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
	"context"
	"fmt"
	"net/url"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	mm "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/ddlsink/mq/ddlproducer"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink/mq/manager"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/sink"
	pulsarConfig "github.com/pingcap/tiflow/pkg/sink/pulsar"
	"github.com/stretchr/testify/require"
)

const (
	// MockPulsarTopic is the mock topic for pulsar
	MockPulsarTopic = "pulsar_test"
)

var pulsarSchemaList = []string{sink.PulsarScheme, sink.PulsarSSLScheme, sink.PulsarHTTPScheme, sink.PulsarHTTPSScheme}

// newPulsarConfig set config
func newPulsarConfig(t *testing.T, schema string) (*config.PulsarConfig, *url.URL) {
	sinkURL := fmt.Sprintf("%s://127.0.0.1:6650/persistent://public/default/test?", schema) +
		"protocol=canal-json&pulsar-version=v2.10.0&enable-tidb-extension=true&" +
		"authentication-token=eyJhbcGcixxxxxxxxxxxxxx"
	sinkURI, err := url.Parse(sinkURL)
	require.NoError(t, err)
	replicaConfig := config.GetDefaultReplicaConfig()
	require.NoError(t, replicaConfig.ValidateAndAdjust(sinkURI))
	c, err := pulsarConfig.NewPulsarConfig(sinkURI, replicaConfig.Sink.PulsarConfig)
	require.NoError(t, err)
	return c, sinkURI
}

// TestNewPulsarDDLSink tests the NewPulsarDDLSink
func TestNewPulsarDDLSink(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, schema := range pulsarSchemaList {
		_, sinkURI := newPulsarConfig(t, schema)
		changefeedID := model.DefaultChangeFeedID("test")
		replicaConfig := config.GetDefaultReplicaConfig()
		replicaConfig.Sink = &config.SinkConfig{
			Protocol: aws.String("canal-json"),
		}

		ctx = context.WithValue(ctx, "testing.T", t)
		ddlSink, err := NewPulsarDDLSink(ctx, changefeedID, sinkURI, replicaConfig,
			manager.NewMockPulsarTopicManager, pulsarConfig.NewMockCreatorFactory, ddlproducer.NewMockPulsarProducerDDL)

		require.NoError(t, err)
		require.NotNil(t, ddlSink)

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

		err = ddlSink.WriteCheckpointTs(ctx, checkpointTs, tables)
		require.NoError(t, err)

		events := ddlSink.producer.(*ddlproducer.PulsarMockProducers).GetAllEvents()
		require.Len(t, events, 1, "All topics and partitions should be broadcast")
	}
}

// TestPulsarDDLSinkNewSuccess tests the NewPulsarDDLSink write a event to pulsar
func TestPulsarDDLSinkNewSuccess(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, schema := range pulsarSchemaList {
		_, sinkURI := newPulsarConfig(t, schema)
		changefeedID := model.DefaultChangeFeedID("test")
		replicaConfig := config.GetDefaultReplicaConfig()
		replicaConfig.Sink = &config.SinkConfig{
			Protocol: aws.String("canal-json"),
		}

		ctx = context.WithValue(ctx, "testing.T", t)
		s, err := NewPulsarDDLSink(ctx, changefeedID, sinkURI, replicaConfig, manager.NewMockPulsarTopicManager,
			pulsarConfig.NewMockCreatorFactory, ddlproducer.NewMockPulsarProducerDDL)
		require.NoError(t, err)
		require.NotNil(t, s)
	}
}

func TestPulsarWriteDDLEventToZeroPartition(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, schema := range pulsarSchemaList {
		_, sinkURI := newPulsarConfig(t, schema)
		changefeedID := model.DefaultChangeFeedID("test")
		replicaConfig := config.GetDefaultReplicaConfig()
		replicaConfig.Sink = &config.SinkConfig{
			Protocol: aws.String("canal-json"),
		}

		ctx = context.WithValue(ctx, "testing.T", t)
		ddlSink, err := NewPulsarDDLSink(ctx, changefeedID, sinkURI, replicaConfig,
			manager.NewMockPulsarTopicManager, pulsarConfig.NewMockCreatorFactory, ddlproducer.NewMockPulsarProducerDDL)

		require.NoError(t, err)
		require.NotNil(t, ddlSink)

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
		err = ddlSink.WriteDDLEvent(ctx, ddl)
		require.NoError(t, err)

		err = ddlSink.WriteDDLEvent(ctx, ddl)
		require.NoError(t, err)

		require.Len(t, ddlSink.producer.(*ddlproducer.PulsarMockProducers).GetAllEvents(),
			2, "Write DDL 2 Events")
	}
}
