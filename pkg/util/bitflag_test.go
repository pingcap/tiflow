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

import (
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	FlagA Flag = 1 << Flag(iota)
	FlagB
	FlagC
	FlagD
)

func TestExample(t *testing.T) {
	var flag Flag

	flag.Add(FlagA)
	flag.Add(FlagB, FlagC)
	flag.Add(FlagC, FlagD)
	flag.Clear()
	flag.Add(FlagA, FlagB, FlagC, FlagD)
	flag.Remove(FlagA)
	flag.Remove(FlagB, FlagC)

	require.False(t, flag.HasAll(FlagA))
	require.False(t, flag.HasAll(FlagB))
	require.False(t, flag.HasAll(FlagC))
	require.True(t, flag.HasAll(FlagD))
}

func TestAdd(t *testing.T) {
	var flag Flag

	flag.Add(FlagA)
	flag.Add(FlagB)
	require.True(t, flag.HasAll(FlagA, FlagB))

	flag.Add(FlagA, FlagB)
	require.True(t, flag.HasAll(FlagA, FlagB))
}

func TestRemove(t *testing.T) {
	var flag Flag

	flag.Add(FlagA, FlagB, FlagC)
	require.True(t, flag.HasAll(FlagA, FlagB, FlagC))

	flag.Remove(FlagB)
	require.False(t, flag.HasAll(FlagB))
	require.True(t, flag.HasAll(FlagA, FlagC))
}

func TestClear(t *testing.T) {
	var flag Flag

	flag.Add(FlagA, FlagB, FlagC)
	require.True(t, flag.HasAll(FlagA, FlagB, FlagC))

	flag.Clear()
	require.False(t, flag.HasOne(FlagA, FlagB, FlagC))
}
