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

package message

import (
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc/sorter/encoding"
	"github.com/pingcap/ticdc/pkg/db"
	"golang.org/x/sync/semaphore"
)

// Task is a db actor task. It carries write and read request.
type Task struct {
	UID     uint32
	TableID uint64

	// encoded key -> serde.marshal(event)
	// If a value is empty, it deletes the key/value entry in db.
	Events map[Key][]byte
	// Must be buffered channel to avoid blocking.
	SnapCh chan LimitedSnapshot `json:"-"` // Make Task JSON printable.
	// Set NeedSnap whenever caller wants to read something from a snapshot.
	NeedSnap bool

	// For clean-up table task.
	Cleanup            bool
	CleanupRatelimited bool
}

// Key is the key that is written to db.
type Key string

// String returns a pretty printed string.
func (k Key) String() string {
	uid, tableID, startTs, CRTs := encoding.DecodeKey([]byte(k))
	return fmt.Sprintf(
		"uid: %d, tableID: %d, startTs: %d, CRTs: %d",
		uid, tableID, startTs, CRTs)
}

// LimitedSnapshot is a wrapper of db.Snapshot that has a sema to limit
// the total number of alive snapshots.
type LimitedSnapshot struct {
	db.Snapshot
	Sema *semaphore.Weighted
}

// Release resources of the snapshot.
func (s *LimitedSnapshot) Release() error {
	s.Sema.Release(1)
	return errors.Trace(s.Snapshot.Release())
}

// NewCleanupTask returns a clean up task to clean up table data.
func NewCleanupTask(uid uint32, tableID uint64) Task {
	return Task{
		TableID: tableID,
		UID:     uid,
		Cleanup: true,
	}
}
