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

package util

// Difference returns the elements in `a` that aren't in `b`.
func Difference[T comparable](a, b []T) []T {
	dict := make(map[T]struct{}, len(b))
	for _, e := range b {
		dict[e] = struct{}{}
	}
	var diff []T
	for _, x := range a {
		if _, found := dict[x]; !found {
			diff = append(diff, x)
		}
	}
	return diff
}
