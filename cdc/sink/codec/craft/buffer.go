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

// Utility functions for buffer allocation
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

// generic slice allocator
type sliceAllocator[T any] struct {
	buffer []T
	offset int
}

//nolint:unused
func (b *sliceAllocator[T]) realloc(old []T, newSize int) []T {
	n := b.alloc(newSize)
	copy(n, old)
	return n
}

func (b *sliceAllocator[T]) alloc(size int) []T {
	if len(b.buffer)-b.offset < size {
		if size > len(b.buffer)/4 {
			// large allocation
			return make([]T, size)
		}
		b.buffer = make([]T, len(b.buffer))
		b.offset = 0
	}
	result := b.buffer[b.offset : b.offset+size]
	b.offset += size
	return result
}

//nolint:unused
func (b *sliceAllocator[T]) one(x T) []T {
	r := b.alloc(1)
	r[0] = x
	return r
}

func newGenericSliceAllocator[T any](batchSize int) *sliceAllocator[T] {
	return &sliceAllocator[T]{buffer: make([]T, batchSize)}
}

// SliceAllocator for different slice types
type SliceAllocator struct {
	intAllocator             *sliceAllocator[int]
	int64Allocator           *sliceAllocator[int64]
	uint64Allocator          *sliceAllocator[uint64]
	stringAllocator          *sliceAllocator[string]
	nullableStringAllocator  *sliceAllocator[*string]
	byteAllocator            *sliceAllocator[byte]
	bytesAllocator           *sliceAllocator[[]byte]
	columnGroupAllocator     *sliceAllocator[*columnGroup]
	rowChangedEventAllocator *sliceAllocator[rowChangedEvent]
}

// NewSliceAllocator creates a new slice allocator with given batch allocation size.
func NewSliceAllocator(batchSize int) *SliceAllocator {
	return &SliceAllocator{
		intAllocator:             newGenericSliceAllocator[int](batchSize),
		int64Allocator:           newGenericSliceAllocator[int64](batchSize),
		uint64Allocator:          newGenericSliceAllocator[uint64](batchSize),
		stringAllocator:          newGenericSliceAllocator[string](batchSize),
		nullableStringAllocator:  newGenericSliceAllocator[*string](batchSize),
		byteAllocator:            newGenericSliceAllocator[byte](batchSize),
		bytesAllocator:           newGenericSliceAllocator[[]byte](batchSize),
		columnGroupAllocator:     newGenericSliceAllocator[*columnGroup](batchSize),
		rowChangedEventAllocator: newGenericSliceAllocator[rowChangedEvent](batchSize),
	}
}

func (b *SliceAllocator) intSlice(size int) []int {
	return b.intAllocator.alloc(size)
}

//nolint:unused
func (b *SliceAllocator) oneIntSlice(x int) []int {
	return b.intAllocator.one(x)
}

//nolint:unused
func (b *SliceAllocator) resizeIntSlice(old []int, newSize int) []int {
	return b.intAllocator.realloc(old, newSize)
}

func (b *SliceAllocator) int64Slice(size int) []int64 {
	return b.int64Allocator.alloc(size)
}

//nolint:unused
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

//nolint:unused
func (b *SliceAllocator) oneStringSlice(x string) []string {
	return b.stringAllocator.one(x)
}

//nolint:unused
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

//nolint:unused
func (b *SliceAllocator) oneByteSlice(x byte) []byte {
	return b.byteAllocator.one(x)
}

//nolint:unused
func (b *SliceAllocator) resizeByteSlice(old []byte, newSize int) []byte {
	return b.byteAllocator.realloc(old, newSize)
}

func (b *SliceAllocator) bytesSlice(size int) [][]byte {
	return b.bytesAllocator.alloc(size)
}

//nolint:unused
func (b *SliceAllocator) oneBytesSlice(x []byte) [][]byte {
	return b.bytesAllocator.one(x)
}

//nolint:unused
func (b *SliceAllocator) resizeBytesSlice(old [][]byte, newSize int) [][]byte {
	return b.bytesAllocator.realloc(old, newSize)
}

func (b *SliceAllocator) columnGroupSlice(size int) []*columnGroup {
	return b.columnGroupAllocator.alloc(size)
}

//nolint:unused
func (b *SliceAllocator) oneColumnGroupSlice(x *columnGroup) []*columnGroup {
	return b.columnGroupAllocator.one(x)
}

//nolint:unused
func (b *SliceAllocator) resizeColumnGroupSlice(old []*columnGroup, newSize int) []*columnGroup {
	return b.columnGroupAllocator.realloc(old, newSize)
}

//nolint:unused
func (b *SliceAllocator) rowChangedEventSlice(size int) []rowChangedEvent {
	return b.rowChangedEventAllocator.alloc(size)
}

//nolint:unused
func (b *SliceAllocator) oneRowChangedEventSlice(x rowChangedEvent) []rowChangedEvent {
	return b.rowChangedEventAllocator.one(x)
}

func (b *SliceAllocator) resizeRowChangedEventSlice(old []rowChangedEvent, newSize int) []rowChangedEvent {
	return b.rowChangedEventAllocator.realloc(old, newSize)
}
