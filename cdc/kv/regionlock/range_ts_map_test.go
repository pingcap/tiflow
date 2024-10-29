// Copyright 2024 PingCAP, Inc.
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

package regionlock

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRangeTsMap(t *testing.T) {
	t.Parallel()

	m := newRangeTsMap([]byte("a"), []byte("z"), math.MaxUint64)

	mustGetMin := func(startKey, endKey string, expectedTs uint64) {
		ts := m.getMinTsInRange([]byte(startKey), []byte(endKey))
		require.Equal(t, expectedTs, ts)
	}
	set := func(startKey, endKey string, ts uint64) {
		m.set([]byte(startKey), []byte(endKey), ts)
	}
	unset := func(startKey, endKey string) {
		m.unset([]byte(startKey), []byte(endKey))
	}

	mustGetMin("a", "z", math.MaxUint64)
	unset("b", "e")
	set("b", "e", 100)
	mustGetMin("a", "z", 100)
	mustGetMin("b", "e", 100)
	mustGetMin("a", "c", 100)
	mustGetMin("d", "f", 100)
	mustGetMin("a", "b", math.MaxUint64)
	mustGetMin("e", "f", math.MaxUint64)
	mustGetMin("a", "b\x00", 100)

	unset("d", "g")
	set("d", "g", 80)
	mustGetMin("d", "g", 80)
	mustGetMin("a", "z", 80)
	mustGetMin("d", "e", 80)
	mustGetMin("a", "d", 100)

	unset("c", "f")
	set("c", "f", 120)
	mustGetMin("c", "f", 120)
	mustGetMin("c", "d", 120)
	mustGetMin("d", "e", 120)
	mustGetMin("e", "f", 120)
	mustGetMin("b", "e", 100)
	mustGetMin("a", "z", 80)

	unset("c", "f")
	set("c", "f", 130)
	mustGetMin("c", "f", 130)
	mustGetMin("c", "d", 130)
	mustGetMin("d", "e", 130)
	mustGetMin("e", "f", 130)
	mustGetMin("b", "e", 100)
	mustGetMin("a", "z", 80)
}
