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

package sink

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/stretchr/testify/require"
)

func TestFlushRowChangedEvents(t *testing.T) {
	ctx, cancel1 := context.WithCancel(context.Background())
	defer cancel1()

	s := newBlackHoleSink(ctx)
	defer s.Close(ctx)

	totalEmit := uint64(0)
	wg := sync.WaitGroup{}
	ctx, cancel2 := context.WithTimeout(ctx, 3*time.Second)
	defer cancel2()

	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for {
			select {
			case <-ctx.Done():
				return
			default:
				i++
				rows := []*model.RowChangedEvent{
					{
						StartTs:  uint64(i),
						CommitTs: uint64(i + 1),
					},
				}
				s.EmitRowChangedEvents(ctx, rows...)
				atomic.AddUint64(&totalEmit, uint64(1))
			}
		}
	}()

	for i := 0; i <= 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					s.FlushRowChangedEvents(ctx, 0, model.ResolvedTs{})
				}
			}
		}()
	}

	wg.Wait()
	require.Equal(t, totalEmit, s.accumulated)
}
