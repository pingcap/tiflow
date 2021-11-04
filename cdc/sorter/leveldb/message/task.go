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

	"github.com/pingcap/ticdc/cdc/sorter/encoding"
	Iterator "github.com/syndtr/goleveldb/leveldb/iterator"
	lutil "github.com/syndtr/goleveldb/leveldb/util"
	"golang.org/x/sync/semaphore"
)

// Task is a leveldb actor task. It carries write and read request.
type Task struct {
	UID     uint32
	TableID uint64

	// encoded key -> serde.marshal(event)
	// If a value is empty, it deletes the key/value entry in leveldb.
	Events map[Key][]byte
	// Must be buffered channel to avoid blocking.
	IterCh   chan LimitedIterator `json:"-"` // Make Task JSON printable.
	Irange   *lutil.Range
	NeedIter bool
}

// Key is the key that is written to leveldb.
type Key string

// String returns a pretty printed string.
func (k Key) String() string {
	uid, tableID, startTs, CRTs := encoding.DecodeKey([]byte(k))
	return fmt.Sprintf(
		"uid: %d, tableID: %d, startTs: %d, CRTs: %d",
		uid, tableID, startTs, CRTs)
}

// LimitedIterator is a wrapper of leveldb iterator that has a sema to limit
// the total number of open iterators.
type LimitedIterator struct {
	Iterator.Iterator
	Sema *semaphore.Weighted
}
