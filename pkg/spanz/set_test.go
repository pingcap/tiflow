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

package spanz

import (
	"testing"

	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/stretchr/testify/require"
)

func TestSet(t *testing.T) {
	s := NewSet()
	s.Add(tablepb.Span{TableID: 1})
	s.Add(tablepb.Span{TableID: 1})
	s.Add(tablepb.Span{TableID: 2})
	s.Add(tablepb.Span{TableID: 3})

	require.Equal(t, 3, s.Size())
	s.Remove(tablepb.Span{TableID: 3})
	require.Equal(t, 2, s.Size())

	require.True(t, s.Contain(tablepb.Span{TableID: 2}))
	require.False(t, s.Contain(tablepb.Span{TableID: 5}))

	require.Equal(t, []tablepb.Span{{TableID: 1}, {TableID: 2}}, s.Keys())
}
