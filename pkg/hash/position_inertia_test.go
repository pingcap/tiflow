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

func (s *testPositionInertia) TestLoopLeftMove(c *C) {
	c.Assert(loopLeftMove(0b11001100, 0), Equals, byte(0b11001100))
	c.Assert(loopLeftMove(0b11001100, 1), Equals, byte(0b10011001))
	c.Assert(loopLeftMove(0b11001100, 2), Equals, byte(0b00110011))
	c.Assert(loopLeftMove(0b11001100, -1), Equals, byte(0b01100110))
	c.Assert(loopLeftMove(0b11001100, 8), Equals, byte(0b11001100))
	c.Assert(loopLeftMove(0b11001100, 13), Equals, byte(0b10011001))
	c.Assert(loopLeftMove(0b11001100, -13), Equals, byte(0b01100110))
}

func (s *testPositionInertia) TestPositionInertia(c *C) {
	var hash PositionInertia
	hash.Write([]byte("hello"), []byte("hash"))
	hash.Write([]byte("hello"), []byte("pingcap"))
	hash.Write([]byte("hello"), []byte("ticdc"))
	hash.Write([]byte("hello"), []byte("tools"))
	c.Assert(hash, Equals, PositionInertia(0xef))

	hash = 0
	hash.Write([]byte("hello"), []byte("pingcap"))
	hash.Write([]byte("hello"), []byte("hash"))
	hash.Write([]byte("hello"), []byte("tools"))
	hash.Write([]byte("hello"), []byte("ticdc"))
	c.Assert(hash, Equals, PositionInertia(0xef))

	hash = 0
	hash.Write([]byte("hello"), []byte("ticdc"))
	hash.Write([]byte("hello"), []byte("hash"))
	hash.Write([]byte("hello"), []byte("tools"))
	hash.Write([]byte("hello"), []byte("pingcap"))
	c.Assert(hash, Equals, PositionInertia(0xef))

	hash = 0
	hash.Write([]byte("ticdc"), []byte("hello"))
	hash.Write([]byte("hello"), []byte("hash"))
	hash.Write([]byte("hello"), []byte("tools"))
	hash.Write([]byte("hello"), []byte("pingcap"))
	c.Assert(hash, Equals, PositionInertia(0x40))

	hash = 0
	hash.Write([]byte("ticdc"), []byte("hello"))
	hash.Write([]byte("hello"), []byte("hash"))
	hash.Write([]byte("tools"), []byte("hello"))
	hash.Write([]byte("hello"), []byte("pingcap"))
	c.Assert(hash, Equals, PositionInertia(0x3d))
}
