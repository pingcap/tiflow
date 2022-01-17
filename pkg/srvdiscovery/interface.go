package srvdiscovery

import (
	"context"

	"github.com/hanfei1991/microcosm/model"
)

// defines some type alias used in service discovery module
type (
	Revision = int64
	UUID     = string
)

// ServiceResource alias to ExecutorInfo
type ServiceResource = model.ExecutorInfo

// WatchResp defines the change set from a Watch API of Discovery interface
type WatchResp struct {
	addSet map[UUID]ServiceResource
	delSet map[UUID]ServiceResource
	err    error
}

// Discovery defines interface of a simple service discovery
type Discovery interface {
	// Snapshot returns a full set of service resources and the revision of snapshot
	// updateCache indicates whether to update the snapshot to Discovery cache.
	Snapshot(ctx context.Context, updateCache bool) (map[UUID]ServiceResource, Revision, error)

	// Watch watches the change of service resources, the watched events will be
	// returned through a channel.
	Watch(ctx context.Context) <-chan WatchResp
}
