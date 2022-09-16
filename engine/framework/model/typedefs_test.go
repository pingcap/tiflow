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

package model

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWorkerType(t *testing.T) {
	t.Parallel()
	for i, s := range typesStringify {
		wt, ok := toWorkerType[s]
		require.True(t, ok)
		bs, err := json.Marshal(wt)
		require.NoError(t, err)
		var wt2 WorkerType
		require.NoError(t, json.Unmarshal(bs, &wt2))
		require.Equal(t, wt, wt2)
		require.Equal(t, wt, WorkerType(i))
	}

	wt := WorkerType(-1)
	require.Equal(t, "Unknown WorkerType -1", wt.String())
	wt = WorkerType(1000)
	require.Equal(t, "Unknown WorkerType 1000", wt.String())
	bs, err := json.Marshal(wt)
	require.NoError(t, err)
	var wt2 WorkerType
	require.EqualError(t, json.Unmarshal(bs, &wt2), "Unknown WorkerType Unknown WorkerType 1000")
	require.Equal(t, WorkerType(0), wt2)
}
