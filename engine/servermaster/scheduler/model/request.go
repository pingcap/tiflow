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
	resourcemeta "github.com/pingcap/tiflow/engine/pkg/externalresource/resourcemeta/model"
)

// SchedulerRequest represents a request for an executor to run a given task.
type SchedulerRequest struct {
	TenantID string // reserved for future use.

	Cost              ResourceUnit
	ExternalResources []resourcemeta.ResourceID
}

// SchedulerResponse represents a response to a task scheduling request.
type SchedulerResponse struct {
	ExecutorID model.ExecutorID
}
