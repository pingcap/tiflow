// Copyright 2019 PingCAP, Inc.
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

package helper

import (
	"errors"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

type fIsNil func()

func fIsNil1() {}

func TestIsNil(t *testing.T) {
	// nil value
	i := 123
	require.True(t, IsNil(nil))
	require.False(t, IsNil(i))

	// chan
	require.True(t, IsNil((chan int)(nil)))
	require.False(t, IsNil(make(chan int)))

	// func
	require.True(t, IsNil((fIsNil)(nil)))
	require.False(t, IsNil(fIsNil1))

	// interface (error is an interface)
	require.True(t, IsNil((error)(nil)))
	require.False(t, IsNil(errors.New("")))

	// map
	require.True(t, IsNil((map[int]int)(nil)))
	require.False(t, IsNil(make(map[int]int)))

	// pointer
	var piNil *int
	piNotNil := &i
	require.True(t, IsNil(piNil))
	require.False(t, IsNil(piNotNil))

	// unsafe pointer
	var upiNil unsafe.Pointer
	upiNotNil := unsafe.Pointer(piNotNil)
	require.True(t, IsNil(upiNil))
	require.False(t, IsNil(upiNotNil))

	// slice
	require.True(t, IsNil(([]int)(nil)))
	require.False(t, IsNil(make([]int, 0)))
}
