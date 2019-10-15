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
	// TableSchedule updates table infos stored in SubChangeFeed in etcd
	TableSchedule(ctx context.Context, changeFeedID string, schedule map[string][]*ProcessTableInfo) error

	// UpdateResolvedTS updates the ResolvedTS to resolvedTS of a ChangeFeed with ID = changeFeedID
	UpdateResolvedTS(ctx context.Context, changeFeedID string, resolveTS uint64) error

	// UpdateCheckpointTS updates the CheckpointTS to checkpointTS of a ChangeFeed with ID = changeFeedID
	UpdateCheckpointTS(ctx context.Context, changeFeedID string, checkpointTS uint64) error

	// CalcResolvedTS calculates ResolvedTS of a ChangeFeed
	CalcResolvedTS(ctx context.Context, changeFeedID string) (uint64, error)

	// CalcCheckpointTS calculates CheckpointTS of a ChangeFeed
	CalcCheckpointTS(ctx context.Context, changeFeedID string) (uint64, error)
}
