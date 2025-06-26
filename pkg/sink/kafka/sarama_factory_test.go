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
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/IBM/sarama/mocks"
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

// mockClientForHeartbeat is a mock sarama.Client used to verify
// that the Brokers() method is called as part of the heartbeat logic.
type mockClientForHeartbeat struct {
	sarama.Client // Embed the interface to avoid implementing all methods.
	brokersCalled chan struct{}
}

// Brokers is the mocked method. It sends a signal to a channel when called.
func (c *mockClientForHeartbeat) Brokers() []*sarama.Broker {
	// The function under test iterates over the returned slice.
	// Returning nil is fine, as we only want to check if this method was called.
	c.brokersCalled <- struct{}{}
	return nil
}

// Close closes the signal channel.
func (c *mockClientForHeartbeat) Close() error {
	close(c.brokersCalled)
	return nil
}

func TestSaramaSyncProducerHeartbeatThrottling(t *testing.T) {
	t.Parallel()

	mockClient := &mockClientForHeartbeat{
		// Use a buffered channel to prevent blocking in case of unexpected calls.
		brokersCalled: make(chan struct{}, 10),
	}

	producer := &saramaSyncProducer{
		id:                    model.DefaultChangeFeedID("test-sync-producer"),
		producer:              nil, // Not needed for this test.
		client:                mockClient,
		keepConnAliveInterval: 100 * time.Millisecond,
		// Set the last heartbeat time to a long time ago to ensure the first call is not throttled.
		lastHeartbeatTime: time.Now().Add(-200 * time.Millisecond),
	}

	// First call, should trigger a call to Brokers().
	producer.HeartbeatBrokers()
	select {
	case <-mockClient.brokersCalled:
		// Expected behavior.
	case <-time.After(100 * time.Millisecond):
		t.Fatal("HeartbeatBrokers should have called client.Brokers(), but it did not")
	}

	// Second call immediately after, should be throttled.
	producer.HeartbeatBrokers()
	select {
	case <-mockClient.brokersCalled:
		t.Fatal("client.Brokers() was called, but it should have been throttled")
	case <-time.After(50 * time.Millisecond):
		// Expected behavior, no call was made.
	}

	// Wait for the interval to pass.
	time.Sleep(producer.keepConnAliveInterval)

	// Third call, should trigger a call to Brokers() again.
	producer.HeartbeatBrokers()
	select {
	case <-mockClient.brokersCalled:
		// Expected behavior.
	case <-time.After(100 * time.Millisecond):
		t.Fatal("HeartbeatBrokers should have called client.Brokers() again, but it did not")
	}
}

func TestSaramaAsyncProducerHeartbeat(t *testing.T) {
	t.Parallel()

	mockClient := &mockClientForHeartbeat{
		brokersCalled: make(chan struct{}, 10),
	}
	// Using a test config is better practice for initializing mocks.
	config := mocks.NewTestConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	mockAsyncProducer := mocks.NewAsyncProducer(t, config)

	producer := &saramaAsyncProducer{
		client:                mockClient,
		producer:              mockAsyncProducer, // Use the mock producer.
		changefeedID:          model.DefaultChangeFeedID("test-async-producer"),
		keepConnAliveInterval: 50 * time.Millisecond, // Use a short interval for testing.
		failpointCh:           make(chan error, 1),
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = producer.AsyncRunCallback(ctx)
	}()

	// Check for the first heartbeat.
	select {
	case <-mockClient.brokersCalled:
		// Good, first heartbeat received.
	case <-time.After(producer.keepConnAliveInterval * 4): // Use a more generous timeout
		t.Fatal("Timed out waiting for the first heartbeat")
	}

	// Check for the second heartbeat.
	select {
	case <-mockClient.brokersCalled:
		// Good, second heartbeat received.
	case <-time.After(producer.keepConnAliveInterval * 4): // Use a more generous timeout
		t.Fatal("Timed out waiting for the second heartbeat")
	}

	// Stop the AsyncRunCallback goroutine.
	cancel()
	wg.Wait()

	// Manually close the mock producer's input channel to fix a deadlock issue.
	// This deterministically signals the mock's internal goroutine to shut down.
	// This is safe because AsyncRunCallback has already exited and will no longer
	// try to close or write to the producer.
	close(mockAsyncProducer.Input())

	// Drain the producer's channels to ensure its internal goroutine has shut down
	// before the test finishes. This prevents both leaks and deadlocks.
	var drainWg sync.WaitGroup
	drainWg.Add(2)
	go func() {
		defer drainWg.Done()
		for range mockAsyncProducer.Successes() {
			// Drain successes
		}
	}()
	go func() {
		defer drainWg.Done()
		for range mockAsyncProducer.Errors() {
			// Drain errors
		}
	}()
	drainWg.Wait()
}
