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

import (
	"regexp"

	"github.com/pingcap/errors"
)

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
func NewKey(str string) (key Key, err error) {
	if err := checkLabelStrValid(str); err != nil {
		return invalidLabelKey, errors.Annotate(err, "new key")
	}
	return Key(str), nil
}

// NewValue creates a new Value from a string.
// It will return legal = false if the input contains
// illegal characters.
func NewValue(str string) (value Value, err error) {
	if err := checkLabelStrValid(str); err != nil {
		return invalidLabelValue, errors.Annotate(err, "new value")
	}
	return Value(str), nil
}

const (
	labelStrRegexStr string = `^[a-zA-Z0-9]([-\.a-zA-Z0-9]*[a-zA-Z0-9])?$`
	maxLabelLength          = 63
)

var labelStrRegex = regexp.MustCompile(labelStrRegexStr)

func checkLabelStrValid(str string) error {
	if len(str) == 0 {
		return errors.New("empty label string")
	}

	if len(str) > maxLabelLength {
		return errors.Errorf("label string too long: %s", str)
	}

	if !labelStrRegex.MatchString(str) {
		return errors.Errorf("label string has wrong format: %s", str)
	}

	return nil
}

// Set is a short name for map[Key]Value.
// Note that it is not thread-safe.
type Set map[Key]Value

// NewSet returns a new Set.
func NewSet() Set {
	return make(map[Key]Value, 8)
}

// NewSetFromMap tries to create a Set from an input of type
// map[string]string. It is used to verify user input and create
// a Set.
func NewSetFromMap(input map[string]string) (Set, error) {
	ret := NewSet()
	for k, v := range input {
		key, err := NewKey(k)
		if err != nil {
			return nil, err
		}

		value, err := NewValue(v)
		if err != nil {
			return nil, err
		}

		if !ret.Add(key, value) {
			// Reachable only if there are duplicate keys,
			// which are not possible because the input is a map.
			panic("unreachable")
		}
	}
	return ret, nil
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

// ToMap converts a Set to a plain map that can be used to
// produce a protobuf message.
func (s Set) ToMap() map[string]string {
	if len(s) == 0 {
		return nil
	}
	ret := make(map[string]string, len(s))
	for k, v := range s {
		ret[string(k)] = string(v)
	}
	return ret
}
