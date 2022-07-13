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
	{str: "09AZaz-_.", allowed: true},
	{str: "-", allowed: true},
	{str: "_", allowed: true},
	{str: ".", allowed: true},
	{str: ":", allowed: false},
	{str: "~", allowed: false},
	{str: string(invalidLabelKey), allowed: false},
	{str: "你好", allowed: false},
	{str: "", allowed: false},
}

func TestNewKey(t *testing.T) {
	t.Parallel()

	for _, tc := range cases {
		t.Run(tc.str, func(t *testing.T) {
			res, ok := NewKey(tc.str)
			require.Equal(t, tc.allowed, ok)
			if !ok {
				require.Equal(t, invalidLabelKey, res)
			}
		})
	}
}

func TestNewValue(t *testing.T) {
	t.Parallel()

	for _, tc := range cases {
		t.Run(tc.str, func(t *testing.T) {
			res, ok := NewValue(tc.str)
			require.Equal(t, tc.allowed, ok)
			if !ok {
				require.Equal(t, invalidLabelValue, res)
			}
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
