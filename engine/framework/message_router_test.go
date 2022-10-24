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

package framework

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tiflow/engine/pkg/p2p"
	"github.com/pingcap/tiflow/pkg/workerpool"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

type testMessage struct {
	id int
}

type messageSuite struct {
	sendCount int
	expected  []testMessage
}

func testMessageRouter(t *testing.T, suite *messageSuite) {
	received := &struct {
		sync.Mutex
		msgs []testMessage
	}{
		msgs: make([]testMessage, 0),
	}
	msgCounter := atomic.NewInt32(0)
	routeFn := func(topic p2p.Topic, msg p2p.MessageValue) error {
		msgCounter.Add(1)
		received.Lock()
		defer received.Unlock()
		tmsg, ok := msg.(testMessage)
		require.True(t, ok)
		received.msgs = append(received.msgs, tmsg)
		return nil
	}

	// send suite.sendCount messages to mesasge router
	pool := workerpool.NewDefaultAsyncPool(1)
	router := NewMessageRouter("test-worker", pool, defaultMessageRouterBufferSize, routeFn)
	for i := 0; i < suite.sendCount; i++ {
		router.AppendMessage(p2p.Topic("test-topic"), testMessage{id: i})
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = pool.Run(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if err := router.Tick(ctx); err != nil {
				return
			}
		}
	}()

	require.Eventually(t, func() bool {
		if int(msgCounter.Load()) != len(suite.expected) {
			return false
		}
		received.Lock()
		defer received.Unlock()
		require.Equal(t, suite.expected, received.msgs)
		return true
	}, time.Second, time.Millisecond*10)

	cancel()
	wg.Wait()
}

func TestMessageRouter(t *testing.T) {
	t.Parallel()

	suite := &messageSuite{
		sendCount: defaultMessageRouterBufferSize,
		expected: []testMessage{
			{0}, {1}, {2}, {3},
		},
	}
	testMessageRouter(t, suite)
}

func TestMessageRouterOverflow(t *testing.T) {
	t.Parallel()

	// old messages that are not processed in time will be dropped
	suite := &messageSuite{
		sendCount: 4 * defaultMessageRouterBufferSize,
		expected: []testMessage{
			{12}, {13}, {14}, {15},
		},
	}
	testMessageRouter(t, suite)
}
