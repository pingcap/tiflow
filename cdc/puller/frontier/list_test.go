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

package frontier

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func insertIntoList(l *skipList, keys ...[]byte) {
	for _, k := range keys {
		l.Insert(k, nil)
	}
}

func TestInsertAndRemove(t *testing.T) {
	t.Parallel()
	list := newSpanList()
	var keys [][]byte
	for i := 0; i < 100000; i++ {
		key := make([]byte, rand.Intn(128)+1)
		rand.Read(key)
		keys = append(keys, key)
		list.Insert(key, nil)
	}

	// check all the keys are exist in list
	for _, k := range keys {
		a := list.Seek(k, make(seekResult, maxHeight)).Node().Key()
		cmp := bytes.Compare(a, k)
		require.Equal(t, 0, cmp)
	}
	checkList(t, list)

	for i := 0; i < 10000; i++ {
		indexToRemove := rand.Intn(10000)
		seekRes := list.Seek(keys[indexToRemove], make(seekResult, maxHeight))
		if seekRes.Node().Next() == nil {
			break
		}
		removedKey := seekRes.Node().Next().Key()
		list.Remove(seekRes, seekRes.Node().Next())

		// check the node is already removed
		a := list.Seek(removedKey, make(seekResult, maxHeight)).Node().Key()
		cmp := bytes.Compare(a, removedKey)
		require.LessOrEqual(t, cmp, 0)
	}
	checkList(t, list)
}

func checkList(t *testing.T, list *skipList) {
	// check the order of the keys in list
	var lastKey []byte
	var nodeNum int
	for node := list.First(); node != nil; node = node.Next() {
		if len(lastKey) != 0 {
			cmp := bytes.Compare(lastKey, node.Key())
			require.LessOrEqual(t, cmp, 0)
		}
		lastKey = node.Key()
		nodeNum++
	}

	// check all the pointers are valid
	prevs := make([]*skipListNode, list.height)
	for i := range prevs {
		prevs[i] = list.head.nexts[i]
	}

	for node := list.First(); node != nil; node = node.Next() {
		for i := 0; i < len(node.nexts); i++ {
			require.Equal(t, node, prevs[i])
			prevs[i] = node.nexts[i]
		}
	}
}

func TestSeek(t *testing.T) {
	t.Parallel()
	key1 := []byte("15")
	keyA := []byte("a5")
	keyB := []byte("b5")
	keyC := []byte("c5")
	keyD := []byte("d5")
	keyE := []byte("e5")
	keyF := []byte("f5")
	keyG := []byte("g5")
	keyH := []byte("h5")
	keyZ := []byte("z5")

	list := newSpanList()

	require.Nil(t, list.Seek(keyA, make(seekResult, maxHeight)).Node())

	// insert keyA to keyH
	insertIntoList(list, keyC, keyF, keyE, keyH, keyG, keyD, keyA, keyB)

	// Point to the first node, if seek key is smaller than the first key in list.
	require.Nil(t, list.Seek(key1, make(seekResult, maxHeight)).Node().Key())

	// Point to the last node with key smaller than seek key.
	require.Equal(t, keyH, list.Seek(keyH, make(seekResult, maxHeight)).Node().key)

	// Point to itself.
	require.Equal(t, keyG, list.Seek(keyG, make(seekResult, maxHeight)).Node().key)

	// Ensure there is no problem to seek a larger key.
	require.Equal(t, keyH, list.Seek(keyZ, make(seekResult, maxHeight)).Node().key)

	require.Equal(t, keyA, list.Seek([]byte("b0"), make(seekResult, maxHeight)).Node().key)
	require.Equal(t, keyB, list.Seek([]byte("c0"), make(seekResult, maxHeight)).Node().key)
	require.Equal(t, keyC, list.Seek([]byte("d0"), make(seekResult, maxHeight)).Node().key)
	require.Equal(t, keyD, list.Seek([]byte("e0"), make(seekResult, maxHeight)).Node().key)
	require.Equal(t, keyE, list.Seek([]byte("f0"), make(seekResult, maxHeight)).Node().key)
	require.Equal(t, keyF, list.Seek([]byte("g0"), make(seekResult, maxHeight)).Node().key)
	require.Equal(t, keyG, list.Seek([]byte("h0"), make(seekResult, maxHeight)).Node().key)
	require.Equal(t, keyH, list.Seek([]byte("i0"), make(seekResult, maxHeight)).Node().key)
	require.Equal(t, "[a5] [b5] [c5] [d5] [e5] [f5] [g5] [h5] ", list.String())
	checkList(t, list)

	// remove c5
	seekRes := list.Seek([]byte("c0"), make(seekResult, maxHeight))
	list.Remove(seekRes, seekRes.Node().Next())
	require.Equal(t, keyB, list.Seek([]byte("c0"), make(seekResult, maxHeight)).Node().key)
	require.Equal(t, keyB, list.Seek([]byte("d0"), make(seekResult, maxHeight)).Node().key)
	require.Equal(t, keyD, list.Seek([]byte("e0"), make(seekResult, maxHeight)).Node().key)
	require.Equal(t, "[a5] [b5] [d5] [e5] [f5] [g5] [h5] ", list.String())
	checkList(t, list)

	// remove d5
	list.Remove(seekRes, seekRes.Node().Next())
	require.Equal(t, keyB, list.Seek([]byte("d0"), make(seekResult, maxHeight)).Node().key)
	require.Equal(t, keyB, list.Seek([]byte("e0"), make(seekResult, maxHeight)).Node().key)
	require.Equal(t, keyE, list.Seek([]byte("f0"), make(seekResult, maxHeight)).Node().key)
	require.Equal(t, "[a5] [b5] [e5] [f5] [g5] [h5] ", list.String())
	checkList(t, list)

	// remove the first node
	seekRes = list.Seek([]byte("10"), make(seekResult, maxHeight))
	list.Remove(seekRes, seekRes.Node().Next())
	require.Equal(t, "[b5] [e5] [f5] [g5] [h5] ", list.String())
	checkList(t, list)
}
