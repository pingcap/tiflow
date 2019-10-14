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

// Processor is used to process etcd information for a capture with processor role
type Processor interface {
	// UpdateCheckpoint updates CheckPointTS stored in SubChangeFeed in etcd
	UpdateCheckpointTS(ctx context.Context, ts uint64) error

	// UpdateResolveTS updates ResolvedTS stored in SubChangeFeed in etcd
	UpdateResolveTS(ctx context.Context, ts uint64) error

	// CalcCheckpointTS gets checkpoint ts based on the checkpoint ts of each puller
	CalcCheckpointTS(ctx context.Context) (uint64, error)

	// CalcResovleTS gets resolve ts based on each puller in the processor
	CalcResolveTS(ctx context.Context) (uint64, error)
}
