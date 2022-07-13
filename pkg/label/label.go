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

package label

type (
	// Key is the type for a label key.
	Key string

	// Value is the type for a label value.
	Value string
)

const (
	invalidLabelKey   = Key("~")
	invalidLabelValue = Value("~")
)

// NewKey creates a new Key from a string.
// It will return legal = false if the input contains
// illegal characters.
func NewKey(str string) (key Key, legal bool) {
	if !isLabelValid(str) {
		return invalidLabelKey, false
	}
	return Key(str), true
}

// NewValue creates a new Value from a string.
// It will return legal = false if the input contains
// illegal characters.
func NewValue(str string) (Value, bool) {
	if !isLabelValid(str) {
		return invalidLabelValue, false
	}
	return Value(str), true
}

func isLabelValid(str string) bool {
	if len(str) == 0 {
		return false
	}

	for i := 0; i < len(str); i++ {
		if !isLabelCharLegal(str[i]) {
			return false
		}
	}

	return true
}

func isLabelCharLegal(c byte) bool {
	if c >= 'a' && c <= 'z' {
		return true
	}
	if c >= 'A' && c <= 'Z' {
		return true
	}
	if c >= '0' && c <= '9' {
		return true
	}
	if c == '_' || c == '-' || c == '.' {
		return true
	}
	return false
}

// Set is a short name for map[Key]Value.
// Note that it is not thread-safe.
type Set map[Key]Value

// NewSet returns a new Set.
func NewSet() Set {
	return make(map[Key]Value, 8)
}

// Add adds a key-value pair to the Set.
// Duplicates are not allowed.
func (s Set) Add(key Key, value Value) (success bool) {
	if key == invalidLabelKey || value == invalidLabelValue {
		panic("Adding invalid key or value to Set")
	}

	if _, exists := s[key]; exists {
		return false
	}
	s[key] = value
	return true
}

// Get gets the value of a key from the Set.
func (s Set) Get(key Key) (value Value, exists bool) {
	if key == invalidLabelKey {
		panic("Trying to get value for invalid key")
	}

	retVal, exists := s[key]
	if !exists {
		return invalidLabelValue, false
	}
	return retVal, true
}
