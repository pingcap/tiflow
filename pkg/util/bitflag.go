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

// HasAll means has all flags
func (f Flag) HasAll(flags ...Flag) bool {
	for _, flag := range flags {
		if flag&f == 0 {
			return false
		}
	}
	return true
}

// HasOne means has one of the flags
func (f Flag) HasOne(flags ...Flag) bool {
	for _, flag := range flags {
		if flag&f != 0 {
			return true
		}
	}
	return false
}

// Add add flags
func (f *Flag) Add(flags ...Flag) {
	for _, flag := range flags {
		*f |= flag
	}
}

// Remove remove flags
func (f *Flag) Remove(flags ...Flag) {
	for _, flag := range flags {
		*f ^= flag
	}
}

// Clear clear all flags
func (f *Flag) Clear() {
	*f ^= *f
}
