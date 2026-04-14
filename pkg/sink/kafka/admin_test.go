// Copyright 2026 PingCAP, Inc.
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
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/stretchr/testify/require"
)

type testSyncProducer struct {
	sarama.SyncProducer
	closeCalls int
	closeErr   error
	callOrder  *[]string
	callLabel  string

	// doneCh is closed after Close finishes, providing a happens-before
	// relationship so tests can safely wait for the async close goroutine.
	doneOnce  sync.Once
	doneCh    chan struct{}
	closeOnce sync.Once
}

func (p *testSyncProducer) done() chan struct{} {
	p.doneOnce.Do(func() {
		p.doneCh = make(chan struct{})
	})
	return p.doneCh
}

func (p *testSyncProducer) closeDone() bool {
	select {
	case <-p.done():
		return true
	default:
		return false
	}
}

func (p *testSyncProducer) Close() error {
	p.closeCalls++
	if p.callOrder != nil {
		*p.callOrder = append(*p.callOrder, p.callLabel)
	}
	p.closeOnce.Do(func() {
		close(p.done())
	})
	return p.closeErr
}

// TestSaramaAdminClientCloseClosesAdminThenClient covers the normal admin close
// path and verifies the wrapper releases both the admin handle and the owned
// client in a deterministic order.
func TestSaramaAdminClientCloseClosesAdminThenClient(t *testing.T) {
	callOrder := make([]string, 0, 2)
	client := &testSaramaClient{callOrder: &callOrder, callLabel: "client"}
	admin := &testSaramaClusterAdmin{callOrder: &callOrder, callLabel: "admin"}

	adminClient := &saramaAdminClient{
		changefeed: model.DefaultChangeFeedID("admin-close-test"),
		client:     client,
		admin:      admin,
	}

	adminClient.Close()
	require.Equal(t, 1, admin.closeCalls)
	require.Equal(t, 1, client.closeCalls)
	require.Equal(t, []string{"admin", "client"}, callOrder)
}

// TestSaramaAdminClientCloseStillClosesClientWhenAdminCloseFails covers the
// error path where admin.Close reports an error but the wrapper must still close
// the owned sarama client.
func TestSaramaAdminClientCloseStillClosesClientWhenAdminCloseFails(t *testing.T) {
	client := &testSaramaClient{}
	admin := &testSaramaClusterAdmin{closeErr: sarama.ErrOutOfBrokers}

	adminClient := &saramaAdminClient{
		changefeed: model.DefaultChangeFeedID("admin-close-error-test"),
		client:     client,
		admin:      admin,
	}

	adminClient.Close()
	require.Equal(t, 1, admin.closeCalls)
	require.Equal(t, 1, client.closeCalls)
	require.True(t, client.closed)
}

// TestSaramaSyncProducerCloseClosesClientAndProducer covers the release-branch
// async close path and verifies the cleanup goroutine still closes both owned
// resources in the branch-specific order.
func TestSaramaSyncProducerCloseClosesClientAndProducer(t *testing.T) {
	callOrder := make([]string, 0, 2)
	client := &testSaramaClient{callOrder: &callOrder, callLabel: "client"}
	producer := &testSyncProducer{callOrder: &callOrder, callLabel: "producer"}

	syncProducer := &saramaSyncProducer{
		id:       model.DefaultChangeFeedID("sync-close-test"),
		client:   client,
		producer: producer,
	}

	syncProducer.Close()
	require.Eventually(t, func() bool {
		return producer.closeDone() && client.closeDone()
	}, time.Second, 10*time.Millisecond)
	require.Equal(t, 1, producer.closeCalls)
	require.Equal(t, 1, client.closeCalls)
	require.Equal(t, []string{"client", "producer"}, callOrder)
}

// TestSaramaSyncProducerCloseStillClosesClientWhenProducerCloseFails covers the
// partial-close path and verifies the release-branch cleanup goroutine still
// releases the owned client even if producer.Close returns an error.
func TestSaramaSyncProducerCloseStillClosesClientWhenProducerCloseFails(t *testing.T) {
	client := &testSaramaClient{}
	producer := &testSyncProducer{closeErr: sarama.ErrOutOfBrokers}

	syncProducer := &saramaSyncProducer{
		id:       model.DefaultChangeFeedID("sync-close-error-test"),
		client:   client,
		producer: producer,
	}

	syncProducer.Close()
	require.Eventually(t, func() bool {
		return producer.closeDone() && client.closeDone()
	}, time.Second, 10*time.Millisecond)
	require.Equal(t, 1, producer.closeCalls)
	require.Equal(t, 1, client.closeCalls)
	require.True(t, client.closed)
}
