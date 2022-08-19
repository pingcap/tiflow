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

package util

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDifference(t *testing.T) {
	sa := []string{"a", "b", "c", "d", "e"}
	sb := []string{"a", "b", "c", "d"}
	sexpected := []string{"e"}
	require.Equal(t, sexpected, Difference(sa, sb))

	ia := []int{1, 2, 3, 4}
	ib := []int{1, 2, 3}
	iexpected := []int{4}
	require.Equal(t, iexpected, Difference(ia, ib))
}
