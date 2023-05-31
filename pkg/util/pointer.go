// Copyright 2023 PingCAP, Inc.
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

// GetOrZero returns the value pointed to by p, or a zero value of
// its type if p is nil.
func GetOrZero[T any](p *T) T {
	var val T
	if p == nil {
		return val
	}
	return *p
}

// AddressOf return the address of the given input variable.
func AddressOf[T any](v T) *T { return &v }
