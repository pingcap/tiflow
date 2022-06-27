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
	conflictKey = int64
)

type txnEvent interface {
	ConflictKeys() []conflictKey
	Finish(errIn error)
}

// OutTxnEvent wraps a transaction and a callback.
// The worker should call Callback when Txn is finished
// executing.
// This is temporary solution before the actual data structure
// of and transaction is decided on.
// TODO remove this.
type OutTxnEvent[T txnEvent] struct {
	Txn      T
	Callback func(errIn error)
}

type worker[Txn txnEvent] interface {
	Add(txn *OutTxnEvent[Txn])
}
