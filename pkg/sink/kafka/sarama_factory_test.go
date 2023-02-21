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

package kafka

import (
	"testing"

	"github.com/Shopify/sarama"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/stretchr/testify/require"
)

func TestNewSaramaFactory(t *testing.T) {
	t.Parallel()

	o := NewOptions()
	o.BrokerEndpoints = []string{"127.0.0.1:9092"}
	o.ClientID = "sarama-test"

	f, err := NewSaramaFactory(o, model.DefaultChangeFeedID("sarama-test"))
	require.NoError(t, err)
	require.NotNil(t, f)

	factory, ok := f.(*saramaFactory)
	require.True(t, ok)
	require.NotNil(t, factory)
	require.NotNil(t, factory.config)
	require.NotNil(t, factory.option)
}

func TestSyncProducer(t *testing.T) {
	t.Parallel()

	leader := sarama.NewMockBroker(t, 1)

	metadataResponse := new(sarama.MetadataResponse)
	metadataResponse.AddBroker(leader.Addr(), leader.BrokerID())
	// Response for `sarama.NewClient`
	leader.Returns(metadataResponse)

	defer leader.Close()

	o := NewOptions()
	o.Version = "0.9.0.0"
	o.BrokerEndpoints = []string{leader.Addr()}
	o.ClientID = "sarama-test"

	f, err := NewSaramaFactory(o, model.DefaultChangeFeedID("sarama-test"))
	require.NoError(t, err)

	factory, ok := f.(*saramaFactory)
	require.True(t, ok)

	sync, err := factory.SyncProducer()
	require.NoError(t, err)
	require.NotNil(t, sync)
	defer sync.Close()

	require.Equal(t, o.MaxMessageBytes, factory.config.Producer.MaxMessageBytes)
}

func TestAsyncProducer(t *testing.T) {
	t.Parallel()

	leader := sarama.NewMockBroker(t, 1)

	metadataResponse := new(sarama.MetadataResponse)
	metadataResponse.AddBroker(leader.Addr(), leader.BrokerID())
	// Response for `sarama.NewClient`
	leader.Returns(metadataResponse)

	defer leader.Close()

	o := NewOptions()
	o.Version = "0.9.0.0"
	o.BrokerEndpoints = []string{leader.Addr()}
	o.ClientID = "sarama-test"

	f, err := NewSaramaFactory(o, model.DefaultChangeFeedID("sarama-test"))
	require.NoError(t, err)

	async, err := f.AsyncProducer(make(chan struct{}, 1), make(chan error, 1))
	require.NoError(t, err)
	require.NotNil(t, async)
	defer async.Close()

	require.Equal(t, o.MaxMessageBytes, f.(*saramaFactory).config.Producer.MaxMessageBytes)
}
