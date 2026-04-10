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
	"context"
	stdErrors "errors"
	"testing"

	"github.com/IBM/sarama"
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
	require.NotNil(t, factory.option)
	require.NotNil(t, factory.registry)
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
	o.IsAssignedVersion = true
	o.BrokerEndpoints = []string{leader.Addr()}
	o.ClientID = "sarama-test"
	// specify request version for mock broker
	o.RequestVersion = 3

	f, err := NewSaramaFactory(o, model.DefaultChangeFeedID("sarama-test"))
	require.NoError(t, err)

	factory, ok := f.(*saramaFactory)
	require.True(t, ok)

	sync, err := factory.SyncProducer(context.Background())
	require.NoError(t, err)
	require.NotNil(t, sync)
	sync.Close()
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
	o.IsAssignedVersion = true
	o.BrokerEndpoints = []string{leader.Addr()}
	o.ClientID = "sarama-test"
	// specify request version for mock broker
	o.RequestVersion = 3

	f, err := NewSaramaFactory(o, model.DefaultChangeFeedID("sarama-test"))
	require.NoError(t, err)

	ctx := context.Background()
	async, err := f.AsyncProducer(ctx, make(chan error, 1))
	require.NoError(t, err)
	require.NotNil(t, async)
	async.Close()
}

type testSaramaClient struct {
	sarama.Client
	closeCalls int
	closed     bool
	closeErr   error
	callOrder  *[]string
	callLabel  string
}

func (c *testSaramaClient) Close() error {
	c.closeCalls++
	c.closed = true
	if c.callOrder != nil {
		*c.callOrder = append(*c.callOrder, c.callLabel)
	}
	return c.closeErr
}

func (c *testSaramaClient) Closed() bool {
	return c.closed
}

type testSaramaClusterAdmin struct {
	sarama.ClusterAdmin
	closeCalls int
	closeErr   error
	callOrder  *[]string
	callLabel  string
}

func (a *testSaramaClusterAdmin) Close() error {
	a.closeCalls++
	if a.callOrder != nil {
		*a.callOrder = append(*a.callOrder, a.callLabel)
	}
	return a.closeErr
}

// TestSaramaFactoryAdminClientClosesClientOnAdminInitFailure verifies the
// factory closes the raw sarama client when admin construction fails before any
// wrapper takes ownership.
func TestSaramaFactoryAdminClientClosesClientOnAdminInitFailure(t *testing.T) {
	o := NewOptions()
	o.Version = "2.0.0"
	o.IsAssignedVersion = true
	o.BrokerEndpoints = []string{"127.0.0.1:9092"}
	o.ClientID = "sarama-test"

	factory, err := NewSaramaFactory(o, model.DefaultChangeFeedID("sarama-test"))
	require.NoError(t, err)
	saramaFactory, ok := factory.(*saramaFactory)
	require.True(t, ok)

	client := &testSaramaClient{}
	oldClientCreator := newSaramaClientImpl
	oldAdminCreator := newSaramaClusterAdminFromClientImpl
	newSaramaClientImpl = func([]string, *sarama.Config) (sarama.Client, error) {
		return client, nil
	}
	newSaramaClusterAdminFromClientImpl = func(sarama.Client) (sarama.ClusterAdmin, error) {
		return nil, stdErrors.New("injected admin init failure")
	}
	defer func() {
		newSaramaClientImpl = oldClientCreator
		newSaramaClusterAdminFromClientImpl = oldAdminCreator
	}()

	_, err = saramaFactory.AdminClient(context.Background())
	require.Error(t, err)
	require.Equal(t, 1, client.closeCalls)
	require.True(t, client.closed)
}

// TestSaramaFactorySyncProducerClosesClientOnInitFailure verifies the factory
// releases the shared sarama client when sync producer construction fails.
func TestSaramaFactorySyncProducerClosesClientOnInitFailure(t *testing.T) {
	o := NewOptions()
	o.Version = "2.0.0"
	o.IsAssignedVersion = true
	o.BrokerEndpoints = []string{"127.0.0.1:9092"}
	o.ClientID = "sarama-test"

	factory, err := NewSaramaFactory(o, model.DefaultChangeFeedID("sarama-test"))
	require.NoError(t, err)
	saramaFactory, ok := factory.(*saramaFactory)
	require.True(t, ok)

	client := &testSaramaClient{}
	oldClientCreator := newSaramaClientImpl
	oldSyncCreator := newSaramaSyncProducerFromClientImpl
	newSaramaClientImpl = func([]string, *sarama.Config) (sarama.Client, error) {
		return client, nil
	}
	newSaramaSyncProducerFromClientImpl = func(sarama.Client) (sarama.SyncProducer, error) {
		return nil, stdErrors.New("injected sync producer init failure")
	}
	defer func() {
		newSaramaClientImpl = oldClientCreator
		newSaramaSyncProducerFromClientImpl = oldSyncCreator
	}()

	_, err = saramaFactory.SyncProducer(context.Background())
	require.Error(t, err)
	require.Equal(t, 1, client.closeCalls)
	require.True(t, client.closed)
}

// TestSaramaFactoryAsyncProducerClosesClientOnInitFailure verifies the factory
// releases the shared sarama client when async producer construction fails.
func TestSaramaFactoryAsyncProducerClosesClientOnInitFailure(t *testing.T) {
	o := NewOptions()
	o.Version = "2.0.0"
	o.IsAssignedVersion = true
	o.BrokerEndpoints = []string{"127.0.0.1:9092"}
	o.ClientID = "sarama-test"

	factory, err := NewSaramaFactory(o, model.DefaultChangeFeedID("sarama-test"))
	require.NoError(t, err)
	saramaFactory, ok := factory.(*saramaFactory)
	require.True(t, ok)

	client := &testSaramaClient{}
	oldClientCreator := newSaramaClientImpl
	oldAsyncCreator := newSaramaAsyncProducerFromClientImpl
	newSaramaClientImpl = func([]string, *sarama.Config) (sarama.Client, error) {
		return client, nil
	}
	newSaramaAsyncProducerFromClientImpl = func(sarama.Client) (sarama.AsyncProducer, error) {
		return nil, stdErrors.New("injected async producer init failure")
	}
	defer func() {
		newSaramaClientImpl = oldClientCreator
		newSaramaAsyncProducerFromClientImpl = oldAsyncCreator
	}()

	_, err = saramaFactory.AsyncProducer(context.Background(), make(chan error, 1))
	require.Error(t, err)
	require.Equal(t, 1, client.closeCalls)
	require.True(t, client.closed)
}
