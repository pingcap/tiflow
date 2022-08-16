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

package redo

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOwnerManagerOptions(t *testing.T) {
	ch := make(chan error, 1)
	opts := NewOwnerManagerOptions(ch)
	require.True(t, opts.EnableBgRunner)
	require.False(t, opts.EnableGCRunner)
	require.True(t, opts.EmitMeta)
	require.False(t, opts.EmitRowEvents)
	require.True(t, opts.EmitDDLEvents)
	select {
	case opts.ErrCh <- nil:
	default:
		t.Fatal("ch shouldn't be full")
	}
}

func TestProcessorManagerOptions(t *testing.T) {
	ch := make(chan error, 1)
	opts := NewProcessorManagerOptions(ch)
	require.True(t, opts.EnableBgRunner)
	require.True(t, opts.EnableGCRunner)
	require.False(t, opts.EmitMeta)
	require.True(t, opts.EmitRowEvents)
	require.False(t, opts.EmitDDLEvents)
	select {
	case opts.ErrCh <- nil:
	default:
		t.Fatal("ch shouldn't be full")
	}
}
