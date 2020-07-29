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

package hash

import (
	"testing"

	. "github.com/pingcap/check"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testPositionInertia{})

type testPositionInertia struct{}

func (s *testPositionInertia) TestPositionInertia(c *C) {
	hash := NewPositionInertia()
	hash.Write([]byte("hello"), []byte("hash"))
	hash.Write([]byte("hello"), []byte("pingcap"))
	hash.Write([]byte("hello"), []byte("ticdc"))
	hash.Write([]byte("hello"), []byte("tools"))
	c.Assert(hash.Sum8(), Equals, byte(0x1c))

	hash.Reset()
	hash.Write([]byte("hello"), []byte("pingcap"))
	hash.Write([]byte("hello"), []byte("hash"))
	hash.Write([]byte("hello"), []byte("tools"))
	hash.Write([]byte("hello"), []byte("ticdc"))
	c.Assert(hash.Sum8(), Equals, byte(0x1c))

	hash.Reset()
	hash.Write([]byte("hello"), []byte("ticdc"))
	hash.Write([]byte("hello"), []byte("hash"))
	hash.Write([]byte("hello"), []byte("tools"))
	hash.Write([]byte("hello"), []byte("pingcap"))
	c.Assert(hash.Sum8(), Equals, byte(0x1c))

	hash.Reset()
	hash.Write([]byte("ticdc"), []byte("hello"))
	hash.Write([]byte("hello"), []byte("hash"))
	hash.Write([]byte("hello"), []byte("tools"))
	hash.Write([]byte("hello"), []byte("pingcap"))
	c.Assert(hash.Sum8(), Equals, byte(0x8c))

	hash.Reset()
	hash.Write([]byte("ticdc"), []byte("hello"))
	hash.Write([]byte("hello"), []byte("hash"))
	hash.Write([]byte("tools"), []byte("hello"))
	hash.Write([]byte("hello"), []byte("pingcap"))
	c.Assert(hash.Sum8(), Equals, byte(0x93))
}
