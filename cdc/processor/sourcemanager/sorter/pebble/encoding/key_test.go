// Copyright 2021 PingCAP, Inc.
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

package encoding

import (
	"bytes"
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/stretchr/testify/require"
)

func TestEncodeKey(t *testing.T) {
	t.Parallel()
	mustLess := func(uniqueIDa, uniqueIDb uint32, tableIDa, tableIDb uint64, a, b *model.PolymorphicEvent) {
		keya, keyb := EncodeKey(uniqueIDa, tableIDa, a), EncodeKey(uniqueIDb, tableIDb, b)
		require.Equal(t, bytes.Compare(keya, keyb), -1)
		require.Equal(t, len(keya), cap(keya))
		require.Equal(t, len(keyb), cap(keyb))
	}

	// UID
	mustLess(
		0, 1,
		0, 0,
		model.NewPolymorphicEvent(&model.RawKVEntry{
			OpType:  model.OpTypeDelete,
			Key:     []byte{1},
			StartTs: 1,
			CRTs:    2,
		}),
		model.NewPolymorphicEvent(&model.RawKVEntry{
			OpType:  model.OpTypeDelete,
			Key:     []byte{1},
			StartTs: 1,
			CRTs:    2,
		}),
	)

	// TableID
	mustLess(
		0, 0,
		0, 1,
		model.NewPolymorphicEvent(&model.RawKVEntry{
			OpType:  model.OpTypeDelete,
			Key:     []byte{1},
			StartTs: 1,
			CRTs:    2,
		}),
		model.NewPolymorphicEvent(&model.RawKVEntry{
			OpType:  model.OpTypeDelete,
			Key:     []byte{1},
			StartTs: 1,
			CRTs:    2,
		}),
	)
	// OpType: OpTypeDelete < OpTypePut
	mustLess(
		0, 0,
		1, 1,
		model.NewPolymorphicEvent(&model.RawKVEntry{
			OpType:  model.OpTypeDelete,
			Key:     []byte{1},
			StartTs: 1,
			CRTs:    2,
		}),
		model.NewPolymorphicEvent(&model.RawKVEntry{
			OpType:  model.OpTypePut,
			Key:     []byte{1},
			StartTs: 1,
			CRTs:    2,
		}),
	)
	// CRTs
	mustLess(
		0, 0,
		1, 1,
		model.NewPolymorphicEvent(&model.RawKVEntry{
			OpType:  model.OpTypePut,
			Key:     []byte{1},
			StartTs: 1,
			CRTs:    2,
		}),
		model.NewPolymorphicEvent(&model.RawKVEntry{
			OpType:  model.OpTypePut,
			Key:     []byte{1},
			StartTs: 1,
			CRTs:    3,
		}),
	)
	// Key
	mustLess(
		0, 0,
		1, 1,
		model.NewPolymorphicEvent(&model.RawKVEntry{
			OpType:  model.OpTypePut,
			Key:     []byte{1},
			StartTs: 1,
			CRTs:    3,
		}),
		model.NewPolymorphicEvent(&model.RawKVEntry{
			OpType:  model.OpTypePut,
			Key:     []byte{2},
			StartTs: 1,
			CRTs:    3,
		}),
	)
	// StartTs
	mustLess(
		0, 0,
		1, 1,
		model.NewPolymorphicEvent(&model.RawKVEntry{
			OpType:  model.OpTypePut,
			Key:     []byte{2},
			StartTs: 1,
			CRTs:    3,
		}),
		model.NewPolymorphicEvent(&model.RawKVEntry{
			OpType:  model.OpTypePut,
			Key:     []byte{2},
			StartTs: 2,
			CRTs:    3,
		}),
	)

	// update < insert
	mustLess(
		0, 0,
		1, 1,
		model.NewPolymorphicEvent(&model.RawKVEntry{
			OpType:   model.OpTypePut,
			Key:      []byte{2},
			StartTs:  1,
			CRTs:     1,
			Value:    []byte{2},
			OldValue: []byte{2},
		}),
		model.NewPolymorphicEvent(&model.RawKVEntry{
			OpType:  model.OpTypePut,
			Key:     []byte{2},
			StartTs: 1,
			CRTs:    1,
			Value:   []byte{2},
		}),
	)
}

func TestEncodeTsKey(t *testing.T) {
	t.Parallel()
	mustLess := func(uniqueIDa, uniqueIDb uint32, tableIDa, tableIDb uint64, a *model.PolymorphicEvent, b uint64) {
		keya, keyb := EncodeKey(uniqueIDa, tableIDa, a), EncodeTsKey(uniqueIDb, tableIDb, b)
		require.Equal(t, bytes.Compare(keya, keyb), -1)
		require.Equal(t, len(keya), cap(keya))
		require.Equal(t, len(keyb), cap(keyb))
	}

	// UID
	mustLess(
		0, 1,
		0, 0,
		model.NewPolymorphicEvent(&model.RawKVEntry{
			OpType:  model.OpTypeDelete,
			Key:     []byte{1},
			StartTs: 1,
			CRTs:    2,
		}),
		0,
	)

	// TableID
	mustLess(
		0, 0,
		0, 1,
		model.NewPolymorphicEvent(&model.RawKVEntry{
			OpType:  model.OpTypeDelete,
			Key:     []byte{1},
			StartTs: 1,
			CRTs:    2,
		}),
		0,
	)
	mustLess(
		0, 0,
		1, 1,
		model.NewPolymorphicEvent(&model.RawKVEntry{
			OpType:  model.OpTypeDelete,
			Key:     []byte{1},
			StartTs: 1,
			CRTs:    2,
		}),
		3,
	)
}

func TestDecodeKey(t *testing.T) {
	t.Parallel()
	key := EncodeKey(1, 2, model.NewPolymorphicEvent(&model.RawKVEntry{
		OpType:  model.OpTypePut,
		Key:     []byte{3},
		StartTs: 4,
		CRTs:    5,
	}))
	uid, tableID, startTs, CRTs := DecodeKey(key)
	require.EqualValues(t, 1, uid)
	require.EqualValues(t, 2, tableID)
	require.EqualValues(t, 4, startTs)
	require.EqualValues(t, 5, CRTs)

	key = EncodeTsKey(1, 2, 3)
	uid, tableID, startTs, CRTs = DecodeKey(key)
	require.EqualValues(t, 1, uid)
	require.EqualValues(t, 2, tableID)
	require.EqualValues(t, 0, startTs)
	require.EqualValues(t, 3, CRTs)
}
