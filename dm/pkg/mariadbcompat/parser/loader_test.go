// Copyright 2025 PingCAP, Inc.
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

package parser

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStripIndexOptions_DoesNotTouchStringOrComment(t *testing.T) {
	input := "CREATE TABLE t (a VARCHAR(64) DEFAULT 'IGNORED', b INT, KEY idx_b (b) IGNORED) -- WITHOUT OVERLAPS\n"
	out := stripIndexOptions(input)
	require.Contains(t, out, "'IGNORED'")
	require.Contains(t, out, "-- WITHOUT OVERLAPS")
	require.NotContains(t, out, "KEY idx_b (b) IGNORED")
}

func TestStripSpatialIndexKeywords_DoesNotTouchStringOrComment(t *testing.T) {
	input := "CREATE TABLE t (note VARCHAR(64) DEFAULT 'SPATIAL INDEX', SPATIAL INDEX idx_b (note)) -- SPATIAL INDEX\n"
	out := stripSpatialIndexKeywords(input)
	require.Contains(t, out, "'SPATIAL INDEX'")
	require.Contains(t, out, "-- SPATIAL INDEX")
	require.Contains(t, out, "INDEX idx_b")
	require.NotContains(t, out, "SPATIAL INDEX idx_b")
}
