// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.
package uuid

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGenerator(t *testing.T) {
	t.Parallel()

	gen := NewGenerator()
	uuid1 := gen.NewString()
	uuid2 := gen.NewString()
	require.NotEqual(t, uuid1, uuid2)
}

func TestMockGenerator(t *testing.T) {
	t.Parallel()

	gen := NewMock()
	require.Panics(t, func() { gen.NewString() })

	uuids := []string{"uuid1", "uuid2", "uuid3"}
	for _, uid := range uuids {
		gen.Push(uid)
	}
	for _, uid := range uuids {
		require.Equal(t, uid, gen.NewString())
	}
}

func TestConstGenerator(t *testing.T) {
	t.Parallel()

	uid := "const-uuid"
	gen := NewConstGenerator(uid)
	for i := 0; i < 3; i++ {
		require.Equal(t, uid, gen.NewString())
	}
}
