package model

import (
	"github.com/hanfei1991/microcosm/model"
	resourcemeta "github.com/hanfei1991/microcosm/pkg/externalresource/resourcemeta/model"
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
