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
	"testing"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/stretchr/testify/require"
)

func TestDecodeRecordKey(t *testing.T) {
	t.Parallel()
	recordPrefix := tablecodec.GenTableRecordPrefix(12345)
	key := tablecodec.EncodeRecordKey(recordPrefix, kv.IntHandle(67890))
	key, tableID, err := decodeTableID(key)
	require.Nil(t, err)
	require.Equal(t, tableID, int64(12345))
	key, recordID, err := decodeRecordID(key)
	require.Nil(t, err)
	require.Equal(t, recordID, int64(67890))
	require.Equal(t, len(key), 0)
}

func TestDecodeListData(t *testing.T) {
	t.Parallel()
	key := []byte("hello")
	var index int64 = 3

	meta, err := decodeMetaKey(buildMetaKey(key, index))
	require.Nil(t, err)
	require.Equal(t, meta.getType(), ListData)
	list := meta.(metaListData)
	require.Equal(t, list.key, string(key))
	require.Equal(t, list.index, index)
}

func buildMetaKey(key []byte, index int64) []byte {
	ek := make([]byte, 0, len(metaPrefix)+len(key)+36)
	ek = append(ek, metaPrefix...)
	ek = codec.EncodeBytes(ek, key)
	ek = codec.EncodeUint(ek, uint64(ListData))
	return codec.EncodeInt(ek, index)
}
