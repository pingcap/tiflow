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

package util

// Flag is a uint64 flag to show a 64 bit mask
type Flag uint64

// Set flag : Set(&flag, FLAG_B, FLAG_C)
func (f *Flag) Set(opts ...Flag) {
	for _, o := range opts {
		*f |= o
	}
}

// Unset flag : Unset(&flag, FLAG_B, FLAG_C)
func (f *Flag) Unset(opts ...Flag) {
	for _, o := range opts {
		*f ^= o
	}
}

// Isset : judge a list of flags is setted. Isset(&flag, FLAG_A), Isset(&flag, FLAG_A, FLAG_B), Isset(&flag, FLAG_A | FLAG_B)
func (f Flag) Isset(opts ...Flag) (isset bool) {

	for _, o := range opts {
		if f&o == 0 {
			return false
		}
	}

	return true
}

// One of opts is setted in flag : OneOf(flag, FLAG_A, FLAG_B)
func (f Flag) One(opts ...Flag) (isset bool) {

	for _, o := range opts {
		if f&o > 0 {
			return true
		}
	}

	return
}

// Clear all bit
func (f *Flag) Clear() {
	*f = 0
}
