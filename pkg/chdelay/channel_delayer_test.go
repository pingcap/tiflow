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

package chdelay

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestChannelDelayerShortDelay(t *testing.T) {
	t.Parallel()

	testChannelDelayer(t, time.Millisecond*100, 10000)
}

func TestChannelDelayerLongDelay(t *testing.T) {
	t.Parallel()

	testChannelDelayer(t, time.Second*2, 100)
}

func testChannelDelayer(t *testing.T, delayBy time.Duration, count int) {
	inCh := make(chan time.Time, 16)
	delayer := NewChannelDelayer(delayBy, inCh, 1024, 16)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(inCh)

		for i := 0; i < count; i++ {
			inCh <- time.Now()
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		counter := 0
		for ts := range delayer.Out() {
			require.Greater(t, time.Since(ts), delayBy)
			counter++
		}

		require.Equal(t, count, counter)
	}()

	wg.Wait()
	delayer.Close()
}
