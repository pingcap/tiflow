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
	"testing"

	"github.com/stretchr/testify/require"
)

const longString = "a1234567890123456789012345678901234567890123456789012345678901234567890z"

var cases = []struct {
	str     string
	allowed bool
}{
	{str: "123", allowed: true},
	{str: "abc", allowed: true},
	{str: "ABC", allowed: true},
	{str: "09", allowed: true},
	{str: "az", allowed: true},
	{str: "AZ", allowed: true},
	{str: "123abc", allowed: true},
	{str: "123abcABC", allowed: true},
	{str: "abcABC", allowed: true},
	{str: "09AZaz", allowed: true},
	{str: "09AZaz-_.", allowed: false},
	{str: "-", allowed: false},
	{str: "_", allowed: false},
	{str: ".", allowed: false},
	{str: ":", allowed: false},
	{str: "~", allowed: false},
	{str: string(invalidLabelKey), allowed: false},
	{str: "你好", allowed: false},
	{str: "", allowed: false},
	{str: longString, allowed: false},
}

func TestNewKey(t *testing.T) {
	t.Parallel()

	for _, tc := range cases {
		tc := tc
		t.Run(tc.str, func(t *testing.T) {
			t.Parallel()
			res, err := NewKey(tc.str)
			if tc.allowed {
				require.NoError(t, err)
				require.NotEqual(t, invalidLabelKey, res)
				return
			}
			require.ErrorContains(t, err, "new key")
		})
	}
}

func TestNewValue(t *testing.T) {
	t.Parallel()

	for _, tc := range cases {
		tc := tc
		t.Run(tc.str, func(t *testing.T) {
			t.Parallel()
			res, err := NewValue(tc.str)
			if tc.allowed {
				require.NoError(t, err)
				require.NotEqual(t, invalidLabelValue, res)
				return
			}
			require.ErrorContains(t, err, "new value")
		})
	}
}

func TestSet(t *testing.T) {
	t.Parallel()

	set := NewSet()

	require.True(t, set.Add("123", "abc"))
	require.False(t, set.Add("123", "cde"))

	key, ok := set.Get("123")
	require.True(t, ok)
	require.Equal(t, Value("abc"), key)

	key, ok = set.Get("456")
	require.False(t, ok)
	require.Equal(t, invalidLabelValue, key)

	require.Panics(t, func() {
		set.Add(invalidLabelKey, "aaa")
	})
	require.Panics(t, func() {
		set.Add("abc", invalidLabelValue)
	})
	require.Panics(t, func() {
		set.Get(invalidLabelKey)
	})
}

func TestNewSetFromMap(t *testing.T) {
	t.Parallel()

	_, err := NewSetFromMap(map[string]string{
		"type":      "executor",
		"09AZaz-_.": "asdf",
	})
	require.ErrorContains(t, err, "label string has wrong format")

	_, err = NewSetFromMap(map[string]string{
		"type": "executor",
		"good": "~",
	})
	require.ErrorContains(t, err, "label string has wrong format")

	set, err := NewSetFromMap(map[string]string{
		"label1": "value1",
		"label2": "value2",
	})
	require.NoError(t, err)
	val, ok := set.Get("label1")
	require.True(t, ok)
	require.Equal(t, Value("value1"), val)

	val, ok = set.Get("label2")
	require.True(t, ok)
	require.Equal(t, Value("value2"), val)
}
