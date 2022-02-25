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
	"github.com/pingcap/tiflow/cdc/sorter/encoding"
	"github.com/pingcap/tiflow/pkg/db"
	"golang.org/x/sync/semaphore"
)

// Task is a db actor task. It carries write and read request.
type Task struct {
	UID     uint32
	TableID uint64

	// A batch of events (bytes encoded) need to be wrote.
	WriteReq map[Key][]byte
	// Requests an iterator when it is not nil.
	IterReq *IterRequest
	// Deletes all of the key-values in the range.
	DeleteReq *DeleteRequest
}

// DeleteRequest a request to delete range.
type DeleteRequest struct {
	Range [2][]byte
	// Approximately key value pairs in the range.
	Count int
}

// IterRequest contains parameters that necessary to build an iterator.
type IterRequest struct {
	UID uint32

	// The resolved ts at the time of issuing the request.
	ResolvedTs uint64
	// Range of a requested iterator.
	Range [2][]byte
	// Must be buffered channel to avoid blocking.
	IterCh chan *LimitedIterator `json:"-"` // Make Task JSON printable.
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

// LimitedIterator is a wrapper of db.Iterator that has a sema to limit
// the total number of alive iterator.
type LimitedIterator struct {
	db.Iterator
	Sema       *semaphore.Weighted
	ResolvedTs uint64
}

// Release resources of the snapshot.
func (s *LimitedIterator) Release() error {
	s.Sema.Release(1)
	return errors.Trace(s.Iterator.Release())
}
