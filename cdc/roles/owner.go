// Copyright 2019 PingCAP, Inc.
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

package roles

import (
	"context"
)

type ProcessTableInfo struct {
	ID      uint64 `json:"id"`
	StartTS uint64 `json:"start-ts"`
}

// Owner is used to process etcd information for a capture with owner role
type Owner interface {

	// ResolverFunc registers the resolver into Owner
	ResolverFunc(ctx context.Context, resolver func(ctx context.Context, changeFeedID string, captureID string) (uint64, error)) error

	// CheckpointerFunc registers the checkpointer into Owner
	CheckpointerFunc(ctx context.Context, checkpointer func(ctx context.Context, changeFeedID string, captureID string) (uint64, error)) error

	// UpdateResolvedTSFunc registers a updater into Owner, which can update resolvedTS to ETCD
	UpdateResolvedTSFunc(ctx context.Context, updater func(ctx context.Context, changeFeedID string, resolvedTS uint64) error)

	// UpdateCheckpointTSFunc registers a updater into Owner, which can update checkpointTS to ETCD
	UpdateCheckpointTSFunc(ctx context.Context, updater func(ctx context.Context, changeFeedID string, checkpointTS uint64) error)

	// GetResolvedTS gets ResolvedTS of a ChangeFeed
	GetResolvedTS(ctx context.Context, changeFeedID string) (uint64, error)

	// GetCheckpointTS gets CheckpointTS of a ChangeFeed
	GetCheckpointTS(ctx context.Context, changeFeedID string) (uint64, error)

	// Run a goroutine to handle Owner logic
	Run(ctx context.Context) error
}
