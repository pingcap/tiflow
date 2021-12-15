// Copyright 2020 PingCAP, Inc.
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

package workerpool

// Hasher is actually a "placement driver" that balances the workload.
// Non-trivial Hashers will be useful if and when we implement dynamic resizing of WorkerPool.
type Hasher interface {
	Hash(object Hashable) int64
}

// Hashable is an object that can be hashed.
type Hashable interface {
	HashCode() int64
}

type defaultHasher struct{}

// Hash returns the hash value.
func (m *defaultHasher) Hash(object Hashable) int64 {
	return object.HashCode()
}
