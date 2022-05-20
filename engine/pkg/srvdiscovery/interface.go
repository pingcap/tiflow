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

package srvdiscovery

import (
	"context"

	"github.com/pingcap/tiflow/engine/model"
)

// UUID is node id actually
type UUID = string

// ServiceResource alias to NodeInfo
type ServiceResource = model.NodeInfo

// WatchResp defines the change set from a Watch API of Discovery interface
type WatchResp struct {
	AddSet map[UUID]ServiceResource
	DelSet map[UUID]ServiceResource
	Err    error
}

// Discovery defines interface of a simple service discovery
type Discovery interface {
	// Snapshot returns a full set of service resources and the revision of snapshot
	Snapshot(ctx context.Context) (Snapshot, error)

	// Watch watches the change of service resources, the watched events will be
	// returned through a channel.
	Watch(ctx context.Context) <-chan WatchResp

	// CopySnapshot copies snapshot from given Discovery, it is useful when we
	// rebuild the Discovery, and the old snapshot is still kept in both old
	// Discovery and p2p messaging system. With this help we don't need to query
	// snapshot from metastore. What's more, the snapshot kept in p2p messageing
	// system may have compacted in metastore.
	CopySnapshot(snapshot Snapshot)

	// Close closes watcher
	Close()
}
