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

package puller

import (
	"context"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/kv"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/engine"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/cdc/puller"
	"go.uber.org/zap"
)

// MultiplexingWrapper wraps `puller.MultiplexingPuller`.
type MultiplexingWrapper struct {
	changefeed model.ChangeFeedID
	*puller.MultiplexingPuller
}

// NewMultiplexingPullerWrapper creates a `MultiplexingWrapper`.
func NewMultiplexingPullerWrapper(
	changefeed model.ChangeFeedID,
	client *kv.SharedClient,
	eventSortEngine engine.SortEngine,
	frontiers int,
) *MultiplexingWrapper {
	consume := func(ctx context.Context, raw *model.RawKVEntry, spans []tablepb.Span, shouldSplitKVEntry model.ShouldSplitKVEntry) error {
		if len(spans) > 1 {
			log.Panic("DML puller subscribes multiple spans",
				zap.String("namespace", changefeed.Namespace),
				zap.String("changefeed", changefeed.ID))
		}
		if raw != nil {
			if shouldSplitKVEntry(raw) {
				deleteKVEntry, insertKVEntry, err := model.SplitUpdateKVEntry(raw)
				if err != nil {
					return err
				}
				deleteEvent := model.NewPolymorphicEvent(deleteKVEntry)
				insertEvent := model.NewPolymorphicEvent(insertKVEntry)
				eventSortEngine.Add(spans[0], deleteEvent, insertEvent)
			} else {
				pEvent := model.NewPolymorphicEvent(raw)
				eventSortEngine.Add(spans[0], pEvent)
			}
		}
		return nil
	}

	slots, hasher := eventSortEngine.SlotsAndHasher()
	mp := puller.NewMultiplexingPuller(changefeed, client, consume, slots, hasher, frontiers)
	return &MultiplexingWrapper{
		changefeed:         changefeed,
		MultiplexingPuller: mp,
	}
}
