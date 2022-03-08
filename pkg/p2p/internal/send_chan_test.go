// Copyright 2021 PingCAP, Inc.
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

package internal

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

const (
	defaultSendChanCap = 8
	numProducers       = 16
	numMsgPerProducer  = 10000
)

func TestSendChanBasics(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	seq := atomic.NewInt64(0)
	c := NewSendChan(defaultSendChanCap)

	var wg sync.WaitGroup

	// Runs the producers
	for i := 0; i < numProducers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lastSeq := int64(0)
			for j := 0; j < numMsgPerProducer; {
				ok, seq := c.SendAsync("test-topic", []byte("test-value"), func() int64 {
					return seq.Inc()
				})
				if !ok {
					continue
				}
				j++
				require.Greater(t, seq, lastSeq)
				lastSeq = seq
			}
		}()
	}

	// Runs the consumer
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(time.Millisecond * 10)

		recvCount := 0
		lastSeq := int64(0)
		for {
			msg, ok, err := c.Receive(ctx, ticker.C)
			require.NoError(t, err)
			if !ok {
				continue
			}
			recvCount++
			require.Equal(t, lastSeq+1, msg.Sequence)
			lastSeq = msg.Sequence
			if recvCount == numProducers*numMsgPerProducer {
				return
			}
		}
	}()

	wg.Wait()
	cancel()
}
