// Copyright 2024 PingCAP, Inc.
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

package util

import "sync/atomic"

// CompareAndIncrease updates the target if the new value is larger than or equal to the old value.
// It returns false if the new value is smaller than the old value.
func CompareAndIncrease(target *atomic.Uint64, new uint64) bool {
	for {
		old := target.Load()
		if new < old {
			return false
		}
		if new == old || target.CompareAndSwap(old, new) {
			return true
		}
	}
}

// CompareAndMonotonicIncrease updates the target if the new value is larger than the old value.
// It returns false if the new value is smaller than or equal to the old value.
func CompareAndMonotonicIncrease(target *atomic.Uint64, new uint64) bool {
	for {
		old := target.Load()
		if new <= old {
			return false
		}
		if target.CompareAndSwap(old, new) {
			return true
		}
	}
}

// MustCompareAndMonotonicIncrease updates the target if the new value is larger than the old value. It do nothing
// if the new value is smaller than or equal to the old value.
func MustCompareAndMonotonicIncrease(target *atomic.Uint64, new uint64) {
	_ = CompareAndMonotonicIncrease(target, new)
}
