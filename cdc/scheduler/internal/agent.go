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

package internal

import (
	"context"

	"github.com/pingcap/tiflow/cdc/model"
)

// Agent is an interface for an object inside Processor that is responsible
// for receiving commands from the Owner.
// Ideally the processor should drive the Agent by Tick.
//
// Note that Agent is not thread-safe
type Agent interface {
	// Tick is called periodically by the processor to drive the Agent's internal logic.
	Tick(ctx context.Context) error

	// GetLastSentCheckpointTs returns the last checkpoint-ts already sent to the Owner.
	GetLastSentCheckpointTs() (checkpointTs model.Ts)

	// Close closes the messenger and does the necessary cleanup.
	Close() error
}
