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
	AddSet map[UUID]ServiceResource
	DelSet map[UUID]ServiceResource
	Err    error
}

// Discovery defines interface of a simple service discovery
type Discovery interface {
	// Snapshot returns a full set of service resources and the revision of snapshot
	Snapshot(ctx context.Context) (map[UUID]ServiceResource, error)

	// Watch watches the change of service resources, the watched events will be
	// returned through a channel.
	Watch(ctx context.Context) <-chan WatchResp

	// SnapshotClone returns the clone of cached snapshot in Discovery
	SnapshotClone() (map[UUID]ServiceResource, Revision)

	// CopySnapshot copies snapshot from given Discovery, it is useful when we
	// rebuild the Discovery, and the old snapshot is still kept in both old
	// Discovery and p2p messaging system. With this help we don't need to query
	// snapshot from metastore. What's more, the snapshot kept in p2p messageing
	// system may have compacted in metastore.
	CopySnapshot(snapshot map[UUID]ServiceResource, revision Revision)

	// Close closes watcher
	Close()
}
