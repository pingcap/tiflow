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
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/config"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/util/testleak"
)

type mqSinkSuite struct{}

var _ = check.Suite(&mqSinkSuite{})

func (s mqSinkSuite) TestKafkaSink(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx, cancel := context.WithCancel(context.Background())

	topic := "kafka-test"
	leader := sarama.NewMockBroker(c, 1)
	defer leader.Close()
	metadataResponse := new(sarama.MetadataResponse)
	metadataResponse.AddBroker(leader.Addr(), leader.BrokerID())
	metadataResponse.AddTopicPartition(topic, 0, leader.BrokerID(), nil, nil, nil, sarama.ErrNoError)
	leader.Returns(metadataResponse)
	leader.Returns(metadataResponse)

	prodSuccess := new(sarama.ProduceResponse)
	prodSuccess.AddTopicPartition(topic, 0, sarama.ErrNoError)

	uriTemplate := "kafka://%s/kafka-test?kafka-version=0.9.0.0&max-batch-size=1" +
		"&max-message-bytes=4194304&max-batch-size=1048576&partition-num=1" +
		"&kafka-client-id=unit-test&auto-create-topic=false&compression=gzip"
	uri := fmt.Sprintf(uriTemplate, leader.Addr())
	sinkURI, err := url.Parse(uri)
	c.Assert(err, check.IsNil)
	replicaConfig := config.GetDefaultReplicaConfig()
	fr, err := filter.NewFilter(replicaConfig)
	c.Assert(err, check.IsNil)
	opts := map[string]string{}
	errCh := make(chan error, 1)
	sink, err := newKafkaSaramaSink(ctx, sinkURI, fr, replicaConfig, opts, errCh)
	c.Assert(err, check.IsNil)

	// mock kafka broker processes 1 row changed event
	leader.Returns(prodSuccess)
	row := &model.RowChangedEvent{
		Table: &model.TableName{
			Schema: "test",
			Table:  "t1",
		},
		StartTs:  100,
		CommitTs: 120,
	}
	err = sink.EmitRowChangedEvents(ctx, row)
	c.Assert(err, check.IsNil)
	checkpointTs, err := sink.FlushRowChangedEvents(ctx, uint64(120))
	c.Assert(err, check.IsNil)
	c.Assert(checkpointTs, check.Equals, uint64(120))
	// flush older resolved ts
	checkpointTs, err = sink.FlushRowChangedEvents(ctx, uint64(110))
	c.Assert(err, check.IsNil)
	c.Assert(checkpointTs, check.Equals, uint64(120))

	// mock kafka broker processes 1 checkpoint ts event
	leader.Returns(prodSuccess)
	err = sink.EmitCheckpointTs(ctx, uint64(120))
	c.Assert(err, check.IsNil)

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
	err = sink.EmitCheckpointTs(ctx, uint64(140))
	if err != nil {
		c.Assert(errors.Cause(err), check.Equals, context.Canceled)
	}

	err = sink.Close()
	if err != nil {
		c.Assert(errors.Cause(err), check.Equals, context.Canceled)
	}
}

func (s mqSinkSuite) TestKafkaSinkFilter(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	topic := "kafka-test"
	leader := sarama.NewMockBroker(c, 1)
	defer leader.Close()
	metadataResponse := new(sarama.MetadataResponse)
	metadataResponse.AddBroker(leader.Addr(), leader.BrokerID())
	metadataResponse.AddTopicPartition(topic, 0, leader.BrokerID(), nil, nil, nil, sarama.ErrNoError)
	leader.Returns(metadataResponse)
	leader.Returns(metadataResponse)

	prodSuccess := new(sarama.ProduceResponse)
	prodSuccess.AddTopicPartition(topic, 0, sarama.ErrNoError)

	uriTemplate := "kafka://%s/kafka-test?kafka-version=0.9.0.0&auto-create-topic=false"
	uri := fmt.Sprintf(uriTemplate, leader.Addr())
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

	err = sink.Close()
	if err != nil {
		c.Assert(errors.Cause(err), check.Equals, context.Canceled)
	}
}
