//  Copyright 2021 PingCAP, Inc.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  See the License for the specific language governing permissions and
//  limitations under the License.

package common

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMarshalAndUnmarshal(t *testing.T) {
	tests := []LogMeta{
		{
			CheckPointTs: uint64(10),
			ResolvedTsList: map[int64]uint64{
				int64(1): uint64(13),
				int64(2): uint64(14),
				int64(3): uint64(15),
			},
		},
		{CheckPointTs: uint64(10)},
	}

	for _, meta1 := range tests {
		var meta2 LogMeta
		var data []byte
		var err error
		data, err = meta1.MarshalMsg(nil)
		require.Nil(t, err)

		zero, err := meta2.UnmarshalMsg(data)
		require.Nil(t, err)
		require.Equal(t, 0, len(zero))

		require.Equal(t, meta1.CheckPointTs, meta2.CheckPointTs)
		require.Equal(t, meta1.ResolvedTs(), meta2.ResolvedTs())
		require.Equal(t, len(meta1.ResolvedTsList), len(meta2.ResolvedTsList))
		for k, v1 := range meta1.ResolvedTsList {
			v2 := meta2.ResolvedTsList[k]
			require.Equal(t, v1, v2)
		}

		_, err = meta2.UnmarshalMsg(data[0 : len(data)-1])
		require.NotNil(t, err)
	}
}
