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

	"github.com/stretchr/testify/require"
)

func TestHashableSpan(t *testing.T) {
	t.Parallel()

	// Make sure it can be a map key.
	m := make(map[hashableSpan]int)
	m[hashableSpan{}] = 1
	require.Equal(t, 1, m[hashableSpan{}])

	span := toHashableSpan(TableIDToComparableSpan(1))
	require.EqualValues(t, TableIDToComparableSpan(1), span.toSpan())
}
