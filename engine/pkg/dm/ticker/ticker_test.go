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

package ticker

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestDefaultTicker(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dummyTicker := NewDummyTicker()
	var wg sync.WaitGroup

	tickError := errors.New("tick error")
	dummyTicker.SetResult([]error{tickError, tickError, nil})

	wg.Add(1)
	// run task manager
	go func() {
		defer wg.Done()
		t := time.NewTicker(50 * time.Millisecond)
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				dummyTicker.DoTick(ctx)
			}
		}
	}()

	// first tick when start
	require.Eventually(t, dummyTicker.ResultAllMeet, 5*time.Second, 100*time.Millisecond)

	// mock manual trigger a tick
	dummyTicker.SetResult([]error{tickError, tickError, nil})
	dummyTicker.SetNextCheckTime(time.Now())
	require.Eventually(t, dummyTicker.ResultAllMeet, 5*time.Second, 100*time.Millisecond)

	// mock trigger with delay
	dummyTicker.SetResult([]error{nil})
	dummyTicker.SetNextCheckTime(time.Now().Add(time.Second))
	require.Eventually(t, dummyTicker.ResultAllMeet, 5*time.Second, 100*time.Millisecond)

	cancel()
	wg.Wait()
}

type DummyTicker struct {
	*DefaultTicker

	sync.Mutex
	results []error
}

func NewDummyTicker() *DummyTicker {
	dummyTicker := &DummyTicker{
		DefaultTicker: NewDefaultTicker(time.Hour, 100*time.Millisecond),
	}
	dummyTicker.DefaultTicker.Ticker = dummyTicker
	return dummyTicker
}

func (t *DummyTicker) SetResult(results []error) {
	t.Lock()
	defer t.Unlock()
	t.results = append(t.results, results...)
}

func (t *DummyTicker) TickImpl(ctx context.Context) error {
	t.Lock()
	defer t.Unlock()
	if len(t.results) == 0 {
		panic("no result in dummy scheduler")
	}
	result := t.results[0]
	t.results = t.results[1:]
	return result
}

func (t *DummyTicker) ResultAllMeet() bool {
	t.Lock()
	defer t.Unlock()
	return len(t.results) == 0
}
