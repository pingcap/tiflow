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
	"github.com/pingcap/tiflow/engine/model"
	schedModel "github.com/pingcap/tiflow/engine/servermaster/scheduler/model"
)

// executorInfoProvider is the abstraction for an object that provides
// necessary information on all online executors.
type executorInfoProvider interface {
	// GetExecutorInfos returns a map containing ExecutorInfo for
	// all executors.
	GetExecutorInfos() map[model.ExecutorID]schedModel.ExecutorInfo
}

type mockExecutorInfoProvider struct {
	infos map[model.ExecutorID]schedModel.ExecutorInfo
}

func (p *mockExecutorInfoProvider) GetExecutorInfos() map[model.ExecutorID]schedModel.ExecutorInfo {
	return p.infos
}
