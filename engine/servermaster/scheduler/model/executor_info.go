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

package model

import (
	"github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/pkg/label"
)

// ExecutorInfo contains information about each executor
// maintained internally to the scheduler.
type ExecutorInfo struct {
	// ID is the executorID.
	ID model.ExecutorID
	// Labels stores the labels configured for the given executor.
	Labels label.Set
}
