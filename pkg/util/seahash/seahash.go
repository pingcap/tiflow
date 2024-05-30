// The file is copied from blainsmith.com/go/seahash@v1.2.1.
//
// Package seahash implements SeaHash, a non-cryptographic hash function
// created by http://ticki.github.io.
//
// See https://ticki.github.io/blog/seahash-explained.
package seahash // import "blainsmith.com/go/seahash"

import (
	"encoding/binary"
)

// Size of a SeaHash checksum in bytes.
const Size = 8

// BlockSize of SeaHash in bytes.
const BlockSize = 8

const (
	chunkSize = 8
	seed1     = 0x16f11fe89b0d677c
	seed2     = 0xb480a793d8e6c86c
	seed3     = 0x6fe2e5aaf078ebc9
	seed4     = 0x14f994a4c5259381
)

// Hasher is an instance to calculate checksum.
type Hasher struct {
	state state
	// Cumulative # of bytes written.
	inputSize int

	// buf[:bufSize] keeps a subword-sized input that was left over from the
	// previous call to Write.
	bufSize int
	buf     [chunkSize]byte
}

// New creates a new SeaHash hash.Hash64
func New() *Hasher {
	h := Hasher{}
	h.Reset()
	return &h
}

// Reset resets the Hasher.
func (h *Hasher) Reset() {
	h.state.a = seed1
	h.state.b = seed2
	h.state.c = seed3
	h.state.d = seed4
	h.inputSize = 0
	h.bufSize = 0
}

// Size returns Size constant to satisfy hash.Hash interface
func (h *Hasher) Size() int { return Size }

// BlockSize returns BlockSize constant to satisfy hash.Hash interface
func (h *Hasher) BlockSize() int { return BlockSize }

// Write writes the given buffer into the Hasher.
func (h *Hasher) Write(b []byte) (nn int, err error) {
	nn = len(b)
	h.inputSize += len(b)
	if h.bufSize > 0 {
		n := len(h.buf) - h.bufSize
		copy(h.buf[h.bufSize:], b)
		if n > len(b) {
			h.bufSize += len(b)
			return
		}
		h.state.update(readInt(h.buf[:]))
		h.bufSize = 0
		b = b[n:]
	}
	for len(b) >= chunkSize {
		h.state.update(readInt(b[:chunkSize]))
		b = b[chunkSize:]
	}
	if len(b) > 0 {
		h.bufSize = len(b)
		copy(h.buf[:], b)
	}
	return
}

// Sum calculates the checksum bytes.
func (h *Hasher) Sum(b []byte) []byte {
	_, _ = h.Write(b)
	r := make([]byte, Size)
	binary.LittleEndian.PutUint64(r, h.Sum64())
	return r
}

// Sum64 calculates the checksum.
func (h *Hasher) Sum64() uint64 {
	if h.bufSize > 0 {
		s := h.state
		s.update(readInt(h.buf[:h.bufSize]))
		return diffuse(s.a ^ s.b ^ s.c ^ s.d ^ uint64(h.inputSize))
	}
	return diffuse(h.state.a ^ h.state.b ^ h.state.c ^ h.state.d ^ uint64(h.inputSize))
}

// Sum is a convenience method that returns the checksum of the byte slice
func Sum(b []byte) []byte {
	var h Hasher
	h.Reset()
	return h.Sum(b)
}

// Sum64 is a convenience method that returns uint64 checksum of the byte slice
func Sum64(b []byte) uint64 {
	var h Hasher
	h.Reset()
	_, _ = h.Write(b)
	return h.Sum64()
}

type state struct {
	a uint64
	b uint64
	c uint64
	d uint64
}

func (s *state) update(x uint64) {
	a := s.a
	a = diffuse(a ^ x)

	s.a = s.b
	s.b = s.c
	s.c = s.d
	s.d = a
}

func diffuse(x uint64) uint64 {
	x *= 0x6eed0e9da4d94a4f
	a := x >> 32
	b := x >> 60
	x ^= a >> b
	x *= 0x6eed0e9da4d94a4f

	return x
}

func readInt(b []uint8) uint64 {
	if len(b) == 8 {
		return binary.LittleEndian.Uint64(b)
	}

	var x uint64
	for i := len(b) - 1; i >= 0; i-- {
		x <<= 8
		x |= uint64(b[i])
	}
	return x
}
