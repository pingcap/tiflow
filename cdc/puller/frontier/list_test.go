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
	"math/rand"

	"github.com/pingcap/check"
)

type spanListSuite struct{}

var _ = check.Suite(&spanListSuite{})

func (s *spanListSuite) insertIntoList(l *spanList, keys ...[]byte) {
	for _, k := range keys {
		l.insert(&node{
			start: k,
		})
	}
}

func (s *spanListSuite) TestInsert(c *check.C) {
	var keys [][]byte
	for i := 0; i < 10000; i++ {
		key := make([]byte, rand.Intn(19)+1)
		rand.Read(key)
		keys = append(keys, key)
	}

	var list spanList
	list.init()
	s.insertIntoList(&list, keys...)

	for _, k := range keys {
		c.Assert(list.seek(k).start, check.BytesEquals, k)
	}
}

func (s *spanListSuite) TestSeek(c *check.C) {
	key1 := []byte("1")
	keyA := []byte("a")
	keyB := []byte("b")
	keyC := []byte("c")
	keyD := []byte("d")
	keyZ := []byte("z")

	var list spanList
	list.init()

	c.Assert(list.seek(keyA), check.IsNil)

	s.insertIntoList(&list, keyB, keyD, keyA)

	// Point to the first node, if seek key is smaller than the first key in list.
	c.Assert(list.seek(key1).start, check.BytesEquals, keyA)

	// Point to the last node with key smaller than seek key.
	c.Assert(list.seek(keyC).start, check.BytesEquals, keyB)

	// Point to itself.
	c.Assert(list.seek(keyD).start, check.BytesEquals, keyD)

	// Ensure there is no problem to seek a larger key.
	c.Assert(list.seek(keyZ).start, check.BytesEquals, keyD)
}
