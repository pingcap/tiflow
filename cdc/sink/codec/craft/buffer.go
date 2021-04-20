// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.orglicensesLICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package craft

/// Utility functions for buffer allocation
func newBufferSize(oldSize int) int {
	var newSize int
	if oldSize > 128 {
		newSize = oldSize + 128
	} else {
		if oldSize > 0 {
			newSize = oldSize * 2
		} else {
			newSize = 8
		}
	}
	return newSize
}

// int slice allocator
type intSliceAllocator struct {
	buffer []int
	offset int
}

func (b *intSliceAllocator) realloc(old []int, newSize int) []int {
	n := b.alloc(newSize)
	copy(n, old)
	return n
}

func (b *intSliceAllocator) alloc(size int) []int {
	if len(b.buffer)-b.offset < size {
		if size > len(b.buffer)/4 {
			// large allocation
			return make([]int, size)
		}
		b.buffer = make([]int, len(b.buffer))
		b.offset = 0
	}
	result := b.buffer[b.offset : b.offset+size]
	b.offset += size
	return result
}

func (b *intSliceAllocator) one(x int) []int {
	r := b.alloc(1)
	r[0] = x
	return r
}

func newIntSliceAllocator(batchSize int) *intSliceAllocator {
	return &intSliceAllocator{buffer: make([]int, batchSize)}
}

// int64 slice allocator
type int64SliceAllocator struct {
	buffer []int64
	offset int
}

func (b *int64SliceAllocator) realloc(old []int64, newSize int) []int64 {
	n := b.alloc(newSize)
	copy(n, old)
	return n
}

func (b *int64SliceAllocator) alloc(size int) []int64 {
	if len(b.buffer)-b.offset < size {
		if size > len(b.buffer)/4 {
			// large allocation
			return make([]int64, size)
		}
		b.buffer = make([]int64, len(b.buffer))
		b.offset = 0
	}
	result := b.buffer[b.offset : b.offset+size]
	b.offset += size
	return result
}

func (b *int64SliceAllocator) one(x int64) []int64 {
	r := b.alloc(1)
	r[0] = x
	return r
}

func newInt64SliceAllocator(batchSize int) *int64SliceAllocator {
	return &int64SliceAllocator{buffer: make([]int64, batchSize)}
}

// uint64 slice allocator
type uint64SliceAllocator struct {
	buffer []uint64
	offset int
}

func (b *uint64SliceAllocator) realloc(old []uint64, newSize int) []uint64 {
	n := b.alloc(newSize)
	copy(n, old)
	return n
}

func (b *uint64SliceAllocator) alloc(size int) []uint64 {
	if len(b.buffer)-b.offset < size {
		if size > len(b.buffer)/4 {
			// large allocation
			return make([]uint64, size)
		}
		b.buffer = make([]uint64, len(b.buffer))
		b.offset = 0
	}
	result := b.buffer[b.offset : b.offset+size]
	b.offset += size
	return result
}

func (b *uint64SliceAllocator) one(x uint64) []uint64 {
	r := b.alloc(1)
	r[0] = x
	return r
}

func newUint64SliceAllocator(batchSize int) *uint64SliceAllocator {
	return &uint64SliceAllocator{buffer: make([]uint64, batchSize)}
}

// string slice allocator
type stringSliceAllocator struct {
	buffer []string
	offset int
}

func (b *stringSliceAllocator) realloc(old []string, newSize int) []string {
	n := b.alloc(newSize)
	copy(n, old)
	return n
}

func (b *stringSliceAllocator) alloc(size int) []string {
	if len(b.buffer)-b.offset < size {
		if size > len(b.buffer)/4 {
			// large allocation
			return make([]string, size)
		}
		b.buffer = make([]string, len(b.buffer))
		b.offset = 0
	}
	result := b.buffer[b.offset : b.offset+size]
	b.offset += size
	return result
}

func (b *stringSliceAllocator) one(x string) []string {
	r := b.alloc(1)
	r[0] = x
	return r
}

func newStringSliceAllocator(batchSize int) *stringSliceAllocator {
	return &stringSliceAllocator{buffer: make([]string, batchSize)}
}

// nullable string slice allocator
type nullableStringSliceAllocator struct {
	buffer []*string
	offset int
}

func (b *nullableStringSliceAllocator) realloc(old []*string, newSize int) []*string {
	n := b.alloc(newSize)
	copy(n, old)
	return n
}

func (b *nullableStringSliceAllocator) alloc(size int) []*string {
	if len(b.buffer)-b.offset < size {
		if size > len(b.buffer)/4 {
			// large allocation
			return make([]*string, size)
		}
		b.buffer = make([]*string, len(b.buffer))
		b.offset = 0
	}
	result := b.buffer[b.offset : b.offset+size]
	b.offset += size
	return result
}

func (b *nullableStringSliceAllocator) one(x *string) []*string {
	r := b.alloc(1)
	r[0] = x
	return r
}

func newNullableStringSliceAllocator(batchSize int) *nullableStringSliceAllocator {
	return &nullableStringSliceAllocator{buffer: make([]*string, batchSize)}
}

// byte slice allocator
type byteSliceAllocator struct {
	buffer []byte
	offset int
}

func (b *byteSliceAllocator) realloc(old []byte, newSize int) []byte {
	n := b.alloc(newSize)
	copy(n, old)
	return n
}

func (b *byteSliceAllocator) alloc(size int) []byte {
	if len(b.buffer)-b.offset < size {
		if size > len(b.buffer)/4 {
			// large allocation
			return make([]byte, size)
		}
		b.buffer = make([]byte, len(b.buffer))
		b.offset = 0
	}
	result := b.buffer[b.offset : b.offset+size]
	b.offset += size
	return result
}

func (b *byteSliceAllocator) one(x byte) []byte {
	r := b.alloc(1)
	r[0] = x
	return r
}

func newByteSliceAllocator(batchSize int) *byteSliceAllocator {
	return &byteSliceAllocator{buffer: make([]byte, batchSize)}
}

// bytes slice allocator
type bytesSliceAllocator struct {
	buffer [][]byte
	offset int
}

func (b *bytesSliceAllocator) realloc(old [][]byte, newSize int) [][]byte {
	n := b.alloc(newSize)
	copy(n, old)
	return n
}

func (b *bytesSliceAllocator) alloc(size int) [][]byte {
	if len(b.buffer)-b.offset < size {
		if size > len(b.buffer)/4 {
			// large allocation
			return make([][]byte, size)
		}
		b.buffer = make([][]byte, len(b.buffer))
		b.offset = 0
	}
	result := b.buffer[b.offset : b.offset+size]
	b.offset += size
	return result
}

func (b *bytesSliceAllocator) one(x []byte) [][]byte {
	r := b.alloc(1)
	r[0] = x
	return r
}

func newBytesSliceAllocator(batchSize int) *bytesSliceAllocator {
	return &bytesSliceAllocator{buffer: make([][]byte, batchSize)}
}

// columnGroup slice allocator
type columnGroupSliceAllocator struct {
	buffer []*columnGroup
	offset int
}

func (b *columnGroupSliceAllocator) realloc(old []*columnGroup, newSize int) []*columnGroup {
	n := b.alloc(newSize)
	copy(n, old)
	return n
}

func (b *columnGroupSliceAllocator) alloc(size int) []*columnGroup {
	if len(b.buffer)-b.offset < size {
		if size > len(b.buffer)/4 {
			// large allocation
			return make([]*columnGroup, size)
		}
		b.buffer = make([]*columnGroup, len(b.buffer))
		b.offset = 0
	}
	result := b.buffer[b.offset : b.offset+size]
	b.offset += size
	return result
}

func (b *columnGroupSliceAllocator) one(x *columnGroup) []*columnGroup {
	r := b.alloc(1)
	r[0] = x
	return r
}

func newColumnGroupSliceAllocator(batchSize int) *columnGroupSliceAllocator {
	return &columnGroupSliceAllocator{buffer: make([]*columnGroup, batchSize)}
}

// rowChangedEvent slice allocator
type rowChangedEventSliceAllocator struct {
	buffer []rowChangedEvent
	offset int
}

func (b *rowChangedEventSliceAllocator) realloc(old []rowChangedEvent, newSize int) []rowChangedEvent {
	n := b.alloc(newSize)
	copy(n, old)
	return n
}

func (b *rowChangedEventSliceAllocator) alloc(size int) []rowChangedEvent {
	if len(b.buffer)-b.offset < size {
		if size > len(b.buffer)/4 {
			// large allocation
			return make([]rowChangedEvent, size)
		}
		b.buffer = make([]rowChangedEvent, len(b.buffer))
		b.offset = 0
	}
	result := b.buffer[b.offset : b.offset+size]
	b.offset += size
	return result
}

func (b *rowChangedEventSliceAllocator) one(x rowChangedEvent) []rowChangedEvent {
	r := b.alloc(1)
	r[0] = x
	return r
}

func newRowChangedEventSliceAllocator(batchSize int) *rowChangedEventSliceAllocator {
	return &rowChangedEventSliceAllocator{buffer: make([]rowChangedEvent, batchSize)}
}

// allocator for different slice types
type SliceAllocator struct {
	intAllocator             *intSliceAllocator
	int64Allocator           *int64SliceAllocator
	uint64Allocator          *uint64SliceAllocator
	stringAllocator          *stringSliceAllocator
	nullableStringAllocator  *nullableStringSliceAllocator
	byteAllocator            *byteSliceAllocator
	bytesAllocator           *bytesSliceAllocator
	columnGroupAllocator     *columnGroupSliceAllocator
	rowChangedEventAllocator *rowChangedEventSliceAllocator
}

func NewSliceAllocator(batchSize int) *SliceAllocator {
	return &SliceAllocator{
		intAllocator:             newIntSliceAllocator(batchSize),
		int64Allocator:           newInt64SliceAllocator(batchSize),
		uint64Allocator:          newUint64SliceAllocator(batchSize),
		stringAllocator:          newStringSliceAllocator(batchSize),
		nullableStringAllocator:  newNullableStringSliceAllocator(batchSize),
		byteAllocator:            newByteSliceAllocator(batchSize),
		bytesAllocator:           newBytesSliceAllocator(batchSize),
		columnGroupAllocator:     newColumnGroupSliceAllocator(batchSize),
		rowChangedEventAllocator: newRowChangedEventSliceAllocator(batchSize),
	}
}

func (b *SliceAllocator) intSlice(size int) []int {
	return b.intAllocator.alloc(size)
}

func (b *SliceAllocator) oneIntSlice(x int) []int {
	return b.intAllocator.one(x)
}

func (b *SliceAllocator) resizeIntSlice(old []int, newSize int) []int {
	return b.intAllocator.realloc(old, newSize)
}

func (b *SliceAllocator) int64Slice(size int) []int64 {
	return b.int64Allocator.alloc(size)
}

func (b *SliceAllocator) oneInt64Slice(x int64) []int64 {
	return b.int64Allocator.one(x)
}

func (b *SliceAllocator) resizeInt64Slice(old []int64, newSize int) []int64 {
	return b.int64Allocator.realloc(old, newSize)
}

func (b *SliceAllocator) uint64Slice(size int) []uint64 {
	return b.uint64Allocator.alloc(size)
}

func (b *SliceAllocator) oneUint64Slice(x uint64) []uint64 {
	return b.uint64Allocator.one(x)
}

func (b *SliceAllocator) resizeUint64Slice(old []uint64, newSize int) []uint64 {
	return b.uint64Allocator.realloc(old, newSize)
}

func (b *SliceAllocator) stringSlice(size int) []string {
	return b.stringAllocator.alloc(size)
}

func (b *SliceAllocator) oneStringSlice(x string) []string {
	return b.stringAllocator.one(x)
}

func (b *SliceAllocator) resizeStringSlice(old []string, newSize int) []string {
	return b.stringAllocator.realloc(old, newSize)
}

func (b *SliceAllocator) nullableStringSlice(size int) []*string {
	return b.nullableStringAllocator.alloc(size)
}

func (b *SliceAllocator) oneNullableStringSlice(x *string) []*string {
	return b.nullableStringAllocator.one(x)
}

func (b *SliceAllocator) resizeNullableStringSlice(old []*string, newSize int) []*string {
	return b.nullableStringAllocator.realloc(old, newSize)
}

func (b *SliceAllocator) byteSlice(size int) []byte {
	return b.byteAllocator.alloc(size)
}

func (b *SliceAllocator) oneByteSlice(x byte) []byte {
	return b.byteAllocator.one(x)
}

func (b *SliceAllocator) resizeByteSlice(old []byte, newSize int) []byte {
	return b.byteAllocator.realloc(old, newSize)
}

func (b *SliceAllocator) bytesSlice(size int) [][]byte {
	return b.bytesAllocator.alloc(size)
}

func (b *SliceAllocator) oneBytesSlice(x []byte) [][]byte {
	return b.bytesAllocator.one(x)
}

func (b *SliceAllocator) resizeBytesSlice(old [][]byte, newSize int) [][]byte {
	return b.bytesAllocator.realloc(old, newSize)
}

func (b *SliceAllocator) columnGroupSlice(size int) []*columnGroup {
	return b.columnGroupAllocator.alloc(size)
}

func (b *SliceAllocator) oneColumnGroupSlice(x *columnGroup) []*columnGroup {
	return b.columnGroupAllocator.one(x)
}

func (b *SliceAllocator) resizeColumnGroupSlice(old []*columnGroup, newSize int) []*columnGroup {
	return b.columnGroupAllocator.realloc(old, newSize)
}

func (b *SliceAllocator) rowChangedEventSlice(size int) []rowChangedEvent {
	return b.rowChangedEventAllocator.alloc(size)
}

func (b *SliceAllocator) oneRowChangedEventSlice(x rowChangedEvent) []rowChangedEvent {
	return b.rowChangedEventAllocator.one(x)
}

func (b *SliceAllocator) resizeRowChangedEventSlice(old []rowChangedEvent, newSize int) []rowChangedEvent {
	return b.rowChangedEventAllocator.realloc(old, newSize)
}
