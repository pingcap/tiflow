// Copyright 2020 PingCAP, Inc.
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

package entry

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeTableID(t *testing.T) {
	/*
		"7480000000000000685f728000000000000001"
		└─## decode hex key
		  └─"t\200\000\000\000\000\000\000h_r\200\000\000\000\000\000\000\001"
		    ├─## table prefix
		    │ └─table: 104
		    └─## table row key
		      ├─table: 104
		      └─row: 1
	*/
	key := "7480000000000000685f728000000000000001"
	keyBytes, err := hex.DecodeString(key)
	require.NoError(t, err)
	tableID, err := DecodeTableID(keyBytes)
	require.NoError(t, err)
	require.Equal(t, tableID, int64(104))
}
