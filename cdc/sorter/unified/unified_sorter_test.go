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

package unified

import (
	"context"
	"testing"

	"github.com/pingcap/ticdc/cdc/model"
	"github.com/stretchr/testify/require"
)

func TestUnifiedSorterTryAddEntry(t *testing.T) {
	t.Parallel()

	events := []*model.PolymorphicEvent{
		model.NewPolymorphicEvent(&model.RawKVEntry{OpType: model.OpTypePut, StartTs: 1, CRTs: 0, RegionID: 0}),
		model.NewResolvedPolymorphicEvent(0, 1),
	}
	for _, event := range events {
		s := &Sorter{inputCh: make(chan *model.PolymorphicEvent, 2), closeCh: make(chan struct{}, 2)}
		require.True(t, s.TryAddEntry(context.TODO(), event))
		require.True(t, s.TryAddEntry(context.TODO(), event))
		require.False(t, s.TryAddEntry(context.TODO(), event))
		<-s.inputCh
		require.True(t, s.TryAddEntry(context.TODO(), event))
		<-s.inputCh
		ctx, cancel := context.WithCancel(context.TODO())
		cancel()
		require.False(t, s.TryAddEntry(ctx, event))
		require.True(t, s.TryAddEntry(context.TODO(), event))
		<-s.inputCh
		s.closeCh <- struct{}{}
		require.False(t, s.TryAddEntry(ctx, event))
	}
}
