// Copyright 2020 PingCAP, Inc.
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

package sorter

import (
	"context"

	"github.com/pingcap/tiflow/cdc/model"
)

// Stats of a sorter.
type Stats struct {
	CheckpointTsIngress model.Ts
	ResolvedTsIngress   model.Ts
	CheckpointTsEgress  model.Ts
	ResolvedTsEgress    model.Ts
}

// EventSorter accepts unsorted PolymorphicEvents, sort them in background and returns
// sorted PolymorphicEvents in Output channel
type EventSorter interface {
	Run(ctx context.Context) error
	// TODO add constraints to entries, e.g., order and duplication guarantees.
	AddEntry(ctx context.Context, entry *model.PolymorphicEvent)
	// Output sorted events, ordered by commit ts.
	//
	// Callers must not cache the returned channel, as sorter may not output
	// any resolved events if callers skip calling `Output`.
	//
	//  func caller(ctx context.Context, sorter EventSorter) {
	//  	for {
	//  		output := sorter.Output()
	//  		select {
	//  		case <-ctx.Done():
	//  			return
	//  		case ev := <-output:
	//  			// Do something with ev.
	//  		}
	//  	}
	//  }
	Output() <-chan *model.PolymorphicEvent

	// EmitStartTs let sorter know the start timestamp for consuming data
	EmitStartTs(ctx context.Context, ts uint64)

	Stats() Stats
}
