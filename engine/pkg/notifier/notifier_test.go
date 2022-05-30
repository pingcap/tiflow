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

package notifier

import (
	"context"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNotifierBasics(t *testing.T) {
	n := NewNotifier[int]()
	defer n.Close()

	const (
		numReceivers = 10
		numEvents    = 10000
		finEv        = math.MaxInt
	)
	var wg sync.WaitGroup

	for i := 0; i < numReceivers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			r := n.NewReceiver()
			defer r.Close()

			var ev, lastEv int
			for {
				select {
				case ev = <-r.C:
				}

				if ev == finEv {
					return
				}

				if lastEv != 0 {
					require.Equal(t, lastEv+1, ev)
				}
				lastEv = ev
			}
		}()
	}

	for i := 1; i <= numEvents; i++ {
		n.Notify(i)
	}

	n.Notify(finEv)
	err := n.Flush(context.Background())
	require.NoError(t, err)

	wg.Wait()
}

func TestNotifierClose(t *testing.T) {
	n := NewNotifier[int]()
	defer n.Close()

	const (
		numReceivers = 1000
	)
	var wg sync.WaitGroup

	for i := 0; i < numReceivers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			r := n.NewReceiver()
			defer r.Close()

			_, ok := <-r.C
			require.False(t, ok)
		}()
	}

	time.Sleep(1 * time.Second)
	n.Close()

	wg.Wait()
}
