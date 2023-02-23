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

package causality

type (
	conflictKey = uint64
)

type txnEvent interface {
	// OnConflictResolved is called when the event leaves ConflictDetector.
	OnConflictResolved()

	// Keys are in range [0, math.MaxUint64) and must be deduped.
	//
	// NOTE: if the conflict detector is accessed by multiple threads concurrently,
	// ConflictKeys must also be sorted based on `key % numSlots`.
	ConflictKeys(numSlots uint64) []conflictKey
}

type worker[Txn txnEvent] interface {
	Add(txn Txn, unlock func())
}
