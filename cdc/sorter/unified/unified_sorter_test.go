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

	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
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
		added, err := s.TryAddEntry(context.TODO(), event)
		require.True(t, added)
		require.Nil(t, err)
		added, err = s.TryAddEntry(context.TODO(), event)
		require.True(t, added)
		require.Nil(t, err)
		added, err = s.TryAddEntry(context.TODO(), event)
		require.False(t, added)
		require.Nil(t, err)
		<-s.inputCh
		added, err = s.TryAddEntry(context.TODO(), event)
		require.True(t, added)
		require.Nil(t, err)
		<-s.inputCh
		ctx, cancel := context.WithCancel(context.TODO())
		cancel()
		added, err = s.TryAddEntry(ctx, event)
		require.False(t, added)
		require.False(t, cerror.ErrSorterClosed.Equal(err))
		<-s.inputCh
		s.closeCh <- struct{}{}
		added, err = s.TryAddEntry(context.TODO(), event)
		require.False(t, added)
		require.True(t, cerror.ErrSorterClosed.Equal(err))
	}
}
