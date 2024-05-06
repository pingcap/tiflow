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

package metrics

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLagBucket(t *testing.T) {
	buckets := LagBucket()
	require.Equal(t, 64, len(buckets))
	require.Equal(t, 0.5, buckets[0])
	require.Equal(t, 1.0, buckets[1])
	require.Equal(t, 21.0, buckets[30])
	require.Equal(t, 900.0, buckets[50])
	require.Equal(t, 4000.0, buckets[60])
	require.Equal(t, float64(32000), buckets[63])
}
