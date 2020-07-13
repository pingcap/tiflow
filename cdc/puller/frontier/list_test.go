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

	"github.com/pingcap/check"
)

type spanListSuite struct{}

var _ = check.Suite(&spanListSuite{})

func (s *spanListSuite) insertIntoList(l *skipList, keys ...[]byte) {
	for _, k := range keys {
		l.Insert(k, nil)
	}
}

func (s *spanListSuite) TestInsertAndRemove(c *check.C) {
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
		a := list.Seek(k).Node().Key()
		cmp := bytes.Compare(a, k)
		c.Assert(cmp, check.Equals, 0)
	}
	checkList(c, list)

	for i := 0; i < 10000; i++ {
		indexToRemove := rand.Intn(10000)
		seekRes := list.Seek(keys[indexToRemove])
		if seekRes.Node().Next() == nil {
			break
		}
		removedKey := seekRes.Node().Next().Key()
		list.Remove(seekRes, seekRes.Node().Next())

		// check the node is already removed
		a := list.Seek(removedKey).Node().Key()
		cmp := bytes.Compare(a, removedKey)
		c.Assert(cmp, check.LessEqual, 0)
	}
	checkList(c, list)
}

func checkList(c *check.C, list *skipList) {
	// check the order of the keys in list
	var lastKey []byte
	var nodeNum int
	for node := list.First(); node != nil; node = node.Next() {
		if len(lastKey) != 0 {
			cmp := bytes.Compare(lastKey, node.Key())
			c.Assert(cmp, check.LessEqual, 0)
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
			c.Assert(prevs[i], check.Equals, node)
			prevs[i] = node.nexts[i]
		}
	}
}

func (s *spanListSuite) TestSeek(c *check.C) {
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

	c.Assert(list.Seek(keyA).Node(), check.IsNil)

	// insert keyA to keyH
	s.insertIntoList(list, keyC, keyF, keyE, keyH, keyG, keyD, keyA, keyB)

	// Point to the first node, if seek key is smaller than the first key in list.
	c.Assert(list.Seek(key1).Node().Key(), check.IsNil)

	// Point to the last node with key smaller than seek key.
	c.Assert(list.Seek(keyH).Node().key, check.BytesEquals, keyH)

	// Point to itself.
	c.Assert(list.Seek(keyG).Node().key, check.BytesEquals, keyG)

	// Ensure there is no problem to seek a larger key.
	c.Assert(list.Seek(keyZ).Node().key, check.BytesEquals, keyH)

	c.Assert(list.Seek([]byte("b0")).Node().key, check.BytesEquals, keyA)
	c.Assert(list.Seek([]byte("c0")).Node().key, check.BytesEquals, keyB)
	c.Assert(list.Seek([]byte("d0")).Node().key, check.BytesEquals, keyC)
	c.Assert(list.Seek([]byte("e0")).Node().key, check.BytesEquals, keyD)
	c.Assert(list.Seek([]byte("f0")).Node().key, check.BytesEquals, keyE)
	c.Assert(list.Seek([]byte("g0")).Node().key, check.BytesEquals, keyF)
	c.Assert(list.Seek([]byte("h0")).Node().key, check.BytesEquals, keyG)
	c.Assert(list.Seek([]byte("i0")).Node().key, check.BytesEquals, keyH)
	c.Assert(list.String(), check.Equals, "[a5] [b5] [c5] [d5] [e5] [f5] [g5] [h5] ")
	checkList(c, list)

	// remove c5
	seekRes := list.Seek([]byte("c0"))
	list.Remove(seekRes, seekRes.Node().Next())
	c.Assert(list.Seek([]byte("c0")).Node().key, check.BytesEquals, keyB)
	c.Assert(list.Seek([]byte("d0")).Node().key, check.BytesEquals, keyB)
	c.Assert(list.Seek([]byte("e0")).Node().key, check.BytesEquals, keyD)
	c.Assert(list.String(), check.Equals, "[a5] [b5] [d5] [e5] [f5] [g5] [h5] ")
	checkList(c, list)

	// remove d5
	list.Remove(seekRes, seekRes.Node().Next())
	c.Assert(list.Seek([]byte("d0")).Node().key, check.BytesEquals, keyB)
	c.Assert(list.Seek([]byte("e0")).Node().key, check.BytesEquals, keyB)
	c.Assert(list.Seek([]byte("f0")).Node().key, check.BytesEquals, keyE)
	c.Assert(list.String(), check.Equals, "[a5] [b5] [e5] [f5] [g5] [h5] ")
	checkList(c, list)

	// remove the first node
	seekRes = list.Seek([]byte("10"))
	list.Remove(seekRes, seekRes.Node().Next())
	c.Assert(list.String(), check.Equals, "[b5] [e5] [f5] [g5] [h5] ")
	checkList(c, list)
}
