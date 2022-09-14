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

package dm

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOperateType(t *testing.T) {
	t.Parallel()
	for i, s := range typesStringify {
		op, ok := toOperateType[s]
		require.True(t, ok)
		bs, err := json.Marshal(op)
		require.NoError(t, err)
		var op2 OperateType
		require.NoError(t, json.Unmarshal(bs, &op2))
		require.Equal(t, op, op2)
		require.Equal(t, op, OperateType(i))
	}

	op := OperateType(-1)
	require.Equal(t, "Unknown OperateType -1", op.String())
	op = OperateType(1000)
	require.Equal(t, "Unknown OperateType 1000", op.String())
	bs, err := json.Marshal(op)
	require.NoError(t, err)
	var op2 OperateType
	require.EqualError(t, json.Unmarshal(bs, &op2), "Unknown OperateType Unknown OperateType 1000")
	require.Equal(t, OperateType(0), op2)
}
