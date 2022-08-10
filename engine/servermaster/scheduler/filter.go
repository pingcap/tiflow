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

package scheduler

import (
	"context"

	"github.com/pingcap/tiflow/engine/model"
	schedModel "github.com/pingcap/tiflow/engine/servermaster/scheduler/model"
)

type filter interface {
	// GetEligibleExecutors determines which executors are eligible
	// from a list of candidates.
	//
	// An implementation should guarantee that an informative error
	// is returned if no suitable executor is found.
	GetEligibleExecutors(
		ctx context.Context,
		request *schedModel.SchedulerRequest,
		// candidates is the range of executors to choose from,
		// may be the output of another filter.
		candidates []model.ExecutorID,
	) ([]model.ExecutorID, error)
}
