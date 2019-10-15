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
	// RegisterResolver registers resolver into Owner, which can get resolveTs form a SubChangeFeed
	RegisterResolver(ctx context.Context, changeFeedID string, captureID string, resolver func(ctx context.Context) uint64) error

	// RegisterResolver registers checkpointer into Owner, which can get checkpointTS form a SubChangeFeed
	RegisterCheckpointer(ctx context.Context, changeFeedID string, captureID string, checkpointer func(ctx context.Context) uint64) error

	// RemoveResolver removes a resolver from Owner
	RemoveResolver(ctx context.Context, changeFeedID string, captureID string) error

	// RemoveCheckpointer removes a checkpointer from Owner
	RemoveCheckpointer(ctx context.Context, changeFeedID string, captureID string) error

	// UpdateResolvedTS registers a updater into Owner, which can update resolvedTS to ETCD
	UpdateResolvedTS(ctx context.Context, updater func(ctx context.Context, changeFeedID string, resolvedTS uint64) error)

	// UpdateResolvedTS registers a updater into Owner, which can update checkpointTS to ETCD
	UpdateCheckpointTS(ctx context.Context, updater func(ctx context.Context, changeFeedID string, checkpointTS uint64) error)

	// CalcResolvedTS gets ResolvedTS of a ChangeFeed
	GetResolvedTS(ctx context.Context, changeFeedID string) (uint64, error)

	// CalcCheckpointTS gets CheckpointTS of a ChangeFeed
	GetCheckpointTS(ctx context.Context, changeFeedID string) (uint64, error)

	// Run a goroutine to handle Owner logic
	Run(ctx context.Context) error
}
